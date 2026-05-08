use rand::{rngs::StdRng, Rng, SeedableRng};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use torrentfs::metadata::MetadataManager;
use torrentfs::TorrentRuntime;
use torrentfs_fuse::TorrentFsFilesystem;
use torrentfs_libtorrent::Session;

fn test_torrent_dir() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir.join("../test_data")
}

fn first_torrent_file() -> Option<PathBuf> {
    let dir = test_torrent_dir();
    fs::read_dir(&dir)
        .ok()?
        .filter_map(|e| {
            let e = e.ok()?;
            let name = e.file_name().to_string_lossy().into_owned();
            if name.ends_with(".torrent") {
                Some(e.path())
            } else {
                None
            }
        })
        .next()
}

#[test]
fn test_concurrent_random_read_no_deadlock() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt
        .block_on(TorrentRuntime::new(&state_path))
        .expect("TorrentRuntime::new() should succeed");
    let metadata_manager = Arc::new(MetadataManager::new(runtime.db.clone()).unwrap());
    let session = Arc::new(Session::new().unwrap());

    let fs = TorrentFsFilesystem::new_with_async(state_path.clone(), metadata_manager, session);

    let options = vec![
        fuser::MountOption::FSName("torrentfs".to_string()),
        fuser::MountOption::AutoUnmount,
    ];
    let guard = fuser::spawn_mount2(fs, &mount_path, &options).unwrap();

    thread::sleep(Duration::from_millis(500));

    let src = match first_torrent_file() {
        Some(p) => p,
        None => {
            eprintln!("No .torrent files found, skipping concurrent test");
            drop(guard);
            return;
        }
    };

    let dest = mount_path.join("metadata").join(src.file_name().unwrap());
    fs::copy(&src, &dest).expect("Failed to copy .torrent file");

    thread::sleep(Duration::from_millis(500));

    let data_entries: Vec<_> = fs::read_dir(mount_path.join("data"))
        .unwrap()
        .filter_map(|e| e.ok())
        .collect();

    if data_entries.is_empty() {
        eprintln!("No torrent directories in data/, skipping concurrent test");
        drop(guard);
        return;
    }

    let torrent_dir = data_entries[0].path();
    let file_list: Vec<_> = fs::read_dir(&torrent_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .collect();

    if file_list.is_empty() {
        eprintln!("No files in torrent directory, skipping concurrent test");
        drop(guard);
        return;
    }

    let num_threads = 10;
    let test_duration = Duration::from_secs(300);
    let start_time = Instant::now();

    let total_operations = Arc::new(AtomicUsize::new(0));
    let total_errors = Arc::new(AtomicUsize::new(0));
    let total_bytes_read = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();

    for thread_id in 0..num_threads {
        let files = file_list.clone();
        let torrent_dir = torrent_dir.clone();
        let ops = Arc::clone(&total_operations);
        let errs = Arc::clone(&total_errors);
        let bytes = Arc::clone(&total_bytes_read);

        let handle = thread::spawn(move || {
            let mut local_ops = 0usize;
            let mut local_errs = 0usize;
            let mut local_bytes = 0usize;

            while start_time.elapsed() < test_duration {
                let file_idx = rand::random::<usize>() % files.len();
                let file_path = &files[file_idx];

                match fs::metadata(file_path) {
                    Ok(metadata) => {
                        if metadata.is_file() {
                            match fs::read(file_path) {
                                Ok(data) => {
                                    local_ops += 1;
                                    local_bytes += data.len();
                                }
                                Err(e) => {
                                    if e.kind() != std::io::ErrorKind::BrokenPipe {
                                        local_errs += 1;
                                        eprintln!("Thread {} read error: {}", thread_id, e);
                                    }
                                }
                            }
                        } else {
                            match fs::read_dir(file_path) {
                                Ok(_) => local_ops += 1,
                                Err(e) => {
                                    local_errs += 1;
                                    eprintln!("Thread {} readdir error: {}", thread_id, e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        local_errs += 1;
                        eprintln!("Thread {} metadata error: {}", thread_id, e);
                    }
                }

                thread::sleep(Duration::from_millis(10));
            }

            ops.fetch_add(local_ops, Ordering::Relaxed);
            errs.fetch_add(local_errs, Ordering::Relaxed);
            bytes.fetch_add(local_bytes, Ordering::Relaxed);

            (local_ops, local_errs, local_bytes)
        });

        handles.push(handle);
    }

    let stats_start = Instant::now();
    let mut last_report = Instant::now();
    let report_interval = Duration::from_secs(30);

    while stats_start.elapsed() < test_duration {
        if last_report.elapsed() >= report_interval {
            let ops = total_operations.load(Ordering::Relaxed);
            let errs = total_errors.load(Ordering::Relaxed);
            let bytes = total_bytes_read.load(Ordering::Relaxed);
            let elapsed = stats_start.elapsed();

            println!(
                "[{:.1}s] Operations: {}, Errors: {}, Bytes: {:.2} MB",
                elapsed.as_secs_f64(),
                ops,
                errs,
                bytes as f64 / 1_048_576.0
            );
            last_report = Instant::now();
        }
        thread::sleep(Duration::from_millis(100));
    }

    for handle in handles {
        let (_ops, _errs, _bytes) = handle.join().expect("Thread panicked");
    }

    drop(guard);

    let final_ops = total_operations.load(Ordering::Relaxed);
    let final_errs = total_errors.load(Ordering::Relaxed);
    let final_bytes = total_bytes_read.load(Ordering::Relaxed);
    let actual_duration = start_time.elapsed();

    println!("\n=== Concurrent Read Test Summary ===");
    println!("Test duration: {:?}", actual_duration);
    println!("Total operations: {}", final_ops);
    println!("Total errors: {}", final_errs);
    println!(
        "Total bytes read: {:.2} MB",
        final_bytes as f64 / 1_048_576.0
    );
    println!(
        "Operations per second: {:.2}",
        final_ops as f64 / actual_duration.as_secs_f64()
    );
    println!(
        "Throughput: {:.2} MB/s",
        (final_bytes as f64 / 1_048_576.0) / actual_duration.as_secs_f64()
    );

    assert_eq!(
        final_errs, 0,
        "No errors should occur during concurrent access"
    );
}

#[test]
fn test_concurrent_metadata_operations() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt
        .block_on(TorrentRuntime::new(&state_path))
        .expect("TorrentRuntime::new() should succeed");
    let metadata_manager = Arc::new(MetadataManager::new(runtime.db.clone()).unwrap());
    let session = Arc::new(Session::new().unwrap());

    let fs = TorrentFsFilesystem::new_with_async(state_path.clone(), metadata_manager, session);

    let options = vec![
        fuser::MountOption::FSName("torrentfs".to_string()),
        fuser::MountOption::AutoUnmount,
    ];
    let guard = fuser::spawn_mount2(fs, &mount_path, &options).unwrap();

    thread::sleep(Duration::from_millis(500));

    let num_threads = 10;
    let test_duration = Duration::from_secs(300);
    let start_time = Instant::now();

    let total_operations = Arc::new(AtomicUsize::new(0));
    let total_errors = Arc::new(AtomicUsize::new(0));
    let total_metadata_ops = Arc::new(AtomicUsize::new(0));
    let total_readdir_ops = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();

    for thread_id in 0..num_threads {
        let mount_path = mount_path.clone();
        let ops = Arc::clone(&total_operations);
        let errs = Arc::clone(&total_errors);
        let meta_ops = Arc::clone(&total_metadata_ops);
        let readdir_ops = Arc::clone(&total_readdir_ops);

        let handle = thread::spawn(move || {
            let mut local_ops = 0usize;
            let mut local_errs = 0usize;
            let mut local_meta_ops = 0usize;
            let mut local_readdir_ops = 0usize;

            while start_time.elapsed() < test_duration {
                let metadata_dir = mount_path.join("metadata");
                let data_dir = mount_path.join("data");

                match fs::read_dir(&metadata_dir) {
                    Ok(_) => {
                        local_ops += 1;
                        local_readdir_ops += 1;
                    }
                    Err(e) => {
                        local_errs += 1;
                        eprintln!("Thread {} metadata readdir error: {}", thread_id, e);
                    }
                }

                match fs::read_dir(&data_dir) {
                    Ok(_) => {
                        local_ops += 1;
                        local_readdir_ops += 1;
                    }
                    Err(e) => {
                        local_errs += 1;
                        eprintln!("Thread {} data readdir error: {}", thread_id, e);
                    }
                }

                match fs::metadata(&metadata_dir) {
                    Ok(_) => {
                        local_ops += 1;
                        local_meta_ops += 1;
                    }
                    Err(e) => {
                        local_errs += 1;
                        eprintln!("Thread {} metadata stat error: {}", thread_id, e);
                    }
                }

                thread::sleep(Duration::from_millis(50));
            }

            ops.fetch_add(local_ops, Ordering::Relaxed);
            errs.fetch_add(local_errs, Ordering::Relaxed);
            meta_ops.fetch_add(local_meta_ops, Ordering::Relaxed);
            readdir_ops.fetch_add(local_readdir_ops, Ordering::Relaxed);

            (local_ops, local_errs, local_meta_ops, local_readdir_ops)
        });

        handles.push(handle);
    }

    let stats_start = Instant::now();
    let mut last_report = Instant::now();
    let report_interval = Duration::from_secs(30);

    while stats_start.elapsed() < test_duration {
        if last_report.elapsed() >= report_interval {
            let ops = total_operations.load(Ordering::Relaxed);
            let errs = total_errors.load(Ordering::Relaxed);
            let elapsed = stats_start.elapsed();

            println!(
                "[{:.1}s] Operations: {}, Errors: {}",
                elapsed.as_secs_f64(),
                ops,
                errs
            );
            last_report = Instant::now();
        }
        thread::sleep(Duration::from_millis(100));
    }

    for handle in handles {
        let (_ops, _errs, _meta, _readdir) = handle.join().expect("Thread panicked");
    }

    drop(guard);

    let final_ops = total_operations.load(Ordering::Relaxed);
    let final_errs = total_errors.load(Ordering::Relaxed);
    let final_meta = total_metadata_ops.load(Ordering::Relaxed);
    let final_readdir = total_readdir_ops.load(Ordering::Relaxed);
    let actual_duration = start_time.elapsed();

    println!("\n=== Concurrent Metadata Test Summary ===");
    println!("Test duration: {:?}", actual_duration);
    println!("Total operations: {}", final_ops);
    println!("  Metadata ops: {}", final_meta);
    println!("  Readdir ops: {}", final_readdir);
    println!("Total errors: {}", final_errs);
    println!(
        "Operations per second: {:.2}",
        final_ops as f64 / actual_duration.as_secs_f64()
    );

    assert_eq!(
        final_errs, 0,
        "No errors should occur during concurrent metadata access"
    );
}

#[test]
fn test_random_offset_reading() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt
        .block_on(TorrentRuntime::new(&state_path))
        .expect("TorrentRuntime::new() should succeed");
    let metadata_manager = Arc::new(MetadataManager::new(runtime.db.clone()).unwrap());
    let session = Arc::new(Session::new().unwrap());

    let fs = TorrentFsFilesystem::new_with_async(state_path.clone(), metadata_manager, session);

    let options = vec![
        fuser::MountOption::FSName("torrentfs".to_string()),
        fuser::MountOption::AutoUnmount,
    ];
    let guard = fuser::spawn_mount2(fs, &mount_path, &options).unwrap();

    thread::sleep(Duration::from_millis(500));

    let src = match first_torrent_file() {
        Some(p) => p,
        None => {
            eprintln!("No .torrent files found, skipping random offset test");
            drop(guard);
            return;
        }
    };

    let dest = mount_path.join("metadata").join(src.file_name().unwrap());
    fs::copy(&src, &dest).expect("Failed to copy .torrent file");

    thread::sleep(Duration::from_millis(500));

    let data_entries: Vec<_> = fs::read_dir(mount_path.join("data"))
        .unwrap()
        .filter_map(|e| e.ok())
        .collect();

    if data_entries.is_empty() {
        eprintln!("No torrent directories in data/, skipping random offset test");
        drop(guard);
        return;
    }

    let torrent_dir = data_entries[0].path();
    let file_list: Vec<_> = fs::read_dir(&torrent_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let path = e.path();
            if path.is_file() {
                fs::metadata(&path).ok().map(|m| (path, m.len()))
            } else {
                None
            }
        })
        .collect();

    if file_list.is_empty() {
        eprintln!("No files in torrent directory, skipping random offset test");
        drop(guard);
        return;
    }

    let seed: u64 = std::env::var("TORRENTFS_TEST_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            eprintln!(
                "Using random seed: {} (set TORRENTFS_TEST_SEED env var to override)",
                timestamp
            );
            timestamp
        });

    let num_threads = 10;
    let test_duration = Duration::from_secs(300);
    let min_read_size = 1024usize;
    let max_read_size = 1024 * 1024usize;

    let start_time = Instant::now();

    let total_operations = Arc::new(AtomicUsize::new(0));
    let total_errors = Arc::new(AtomicUsize::new(0));
    let total_bytes_read = Arc::new(AtomicUsize::new(0));
    let total_verification_failures = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();

    for thread_id in 0..num_threads {
        let files = file_list.clone();
        let ops = Arc::clone(&total_operations);
        let errs = Arc::clone(&total_errors);
        let bytes = Arc::clone(&total_bytes_read);
        let verify_fails = Arc::clone(&total_verification_failures);
        let thread_seed = seed.wrapping_add(thread_id as u64);

        let handle = thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(thread_seed);
            let mut local_ops = 0usize;
            let mut local_errs = 0usize;
            let mut local_bytes = 0usize;
            let mut local_verify_fails = 0usize;

            while start_time.elapsed() < test_duration {
                let file_idx = rng.gen::<usize>() % files.len();
                let (file_path, file_size) = &files[file_idx];

                if *file_size == 0 {
                    continue;
                }

                let read_size =
                    rng.gen_range(min_read_size..=max_read_size.min(*file_size as usize));
                let max_offset = file_size.saturating_sub(read_size as u64);
                let offset = if max_offset > 0 {
                    rng.gen_range(0..=max_offset)
                } else {
                    0
                };

                match OpenOptions::new().read(true).open(file_path) {
                    Ok(mut file) => {
                        let mut buffer = vec![0u8; read_size];

                        match file.seek(SeekFrom::Start(offset)) {
                            Ok(_) => match file.read_exact(&mut buffer) {
                                Ok(_) => {
                                    local_ops += 1;
                                    local_bytes += read_size;

                                    match fs::read(file_path) {
                                        Ok(full_content) => {
                                            let expected = &full_content[offset as usize
                                                ..(offset as usize + read_size)
                                                    .min(full_content.len())];
                                            if buffer.len() != expected.len() || buffer != expected
                                            {
                                                local_verify_fails += 1;
                                                eprintln!("Thread {} verification failed for {:?} at offset {}, size {}", 
                                                        thread_id, file_path, offset, read_size);
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!("Thread {} failed to read full file for verification: {}", thread_id, e);
                                        }
                                    }
                                }
                                Err(e) => {
                                    if e.kind() != std::io::ErrorKind::BrokenPipe {
                                        local_errs += 1;
                                        eprintln!(
                                            "Thread {} read_exact error at offset {}: {}",
                                            thread_id, offset, e
                                        );
                                    }
                                }
                            },
                            Err(e) => {
                                local_errs += 1;
                                eprintln!("Thread {} seek error: {}", thread_id, e);
                            }
                        }
                    }
                    Err(e) => {
                        if e.kind() != std::io::ErrorKind::BrokenPipe {
                            local_errs += 1;
                            eprintln!("Thread {} open error: {}", thread_id, e);
                        }
                    }
                }

                let sleep_ms = rng.gen_range(5..20);
                thread::sleep(Duration::from_millis(sleep_ms));
            }

            ops.fetch_add(local_ops, Ordering::Relaxed);
            errs.fetch_add(local_errs, Ordering::Relaxed);
            bytes.fetch_add(local_bytes, Ordering::Relaxed);
            verify_fails.fetch_add(local_verify_fails, Ordering::Relaxed);

            (local_ops, local_errs, local_bytes, local_verify_fails)
        });

        handles.push(handle);
    }

    let stats_start = Instant::now();
    let mut last_report = Instant::now();
    let report_interval = Duration::from_secs(30);

    while stats_start.elapsed() < test_duration {
        if last_report.elapsed() >= report_interval {
            let ops = total_operations.load(Ordering::Relaxed);
            let errs = total_errors.load(Ordering::Relaxed);
            let bytes = total_bytes_read.load(Ordering::Relaxed);
            let verify_fails = total_verification_failures.load(Ordering::Relaxed);
            let elapsed = stats_start.elapsed();
            let success_rate = if ops > 0 {
                ((ops - verify_fails) as f64 / ops as f64) * 100.0
            } else {
                0.0
            };

            println!("[{:.1}s] Ops: {}, Errors: {}, Verify fails: {}, Success rate: {:.2}%, Bytes: {:.2} MB", 
                elapsed.as_secs_f64(),
                ops,
                errs,
                verify_fails,
                success_rate,
                bytes as f64 / 1_048_576.0
            );
            last_report = Instant::now();
        }
        thread::sleep(Duration::from_millis(100));
    }

    for handle in handles {
        let (_ops, _errs, _bytes, _verify) = handle.join().expect("Thread panicked");
    }

    drop(guard);

    let final_ops = total_operations.load(Ordering::Relaxed);
    let final_errs = total_errors.load(Ordering::Relaxed);
    let final_bytes = total_bytes_read.load(Ordering::Relaxed);
    let final_verify_fails = total_verification_failures.load(Ordering::Relaxed);
    let actual_duration = start_time.elapsed();
    let success_rate = if final_ops > 0 {
        ((final_ops - final_verify_fails) as f64 / final_ops as f64) * 100.0
    } else {
        0.0
    };

    println!("\n=== Random Offset Read Test Summary ===");
    println!("Test seed: {}", seed);
    println!("Test duration: {:?}", actual_duration);
    println!("Total operations: {}", final_ops);
    println!("Total errors: {}", final_errs);
    println!("Verification failures: {}", final_verify_fails);
    println!("Success rate: {:.2}%", success_rate);
    println!(
        "Total bytes read: {:.2} MB",
        final_bytes as f64 / 1_048_576.0
    );
    println!(
        "Operations per second: {:.2}",
        final_ops as f64 / actual_duration.as_secs_f64()
    );
    println!(
        "Throughput: {:.2} MB/s",
        (final_bytes as f64 / 1_048_576.0) / actual_duration.as_secs_f64()
    );

    assert_eq!(
        final_errs, 0,
        "No errors should occur during random offset reading"
    );
    assert_eq!(
        final_verify_fails, 0,
        "All random reads should match expected content"
    );
    assert!(success_rate > 99.0, "Success rate should be > 99%");
}
