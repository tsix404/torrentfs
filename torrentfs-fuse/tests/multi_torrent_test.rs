use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use torrentfs_fuse::TorrentFsFilesystem;
use torrentfs::TorrentRuntime;
use torrentfs_libtorrent::Session;
use torrentfs::metadata::MetadataManager;

fn test_torrent_dir() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir.join("../test_data")
}

fn all_torrent_files() -> Vec<PathBuf> {
    let dir = test_torrent_dir();
    fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| {
            let e = e.ok()?;
            let name = e.file_name().to_string_lossy().into_owned();
            if name.ends_with(".torrent") {
                Some(e.path())
            } else {
                None
            }
        })
        .collect()
}

#[derive(Debug, Clone)]
struct TorrentStats {
    name: String,
    operations: usize,
    errors: usize,
    bytes_read: usize,
}

#[test]
fn test_concurrent_read_multiple_torrents() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt.block_on(TorrentRuntime::new(&state_path)).expect("TorrentRuntime::new() should succeed");
    let metadata_manager = Arc::new(
        MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = Arc::new(Session::new().unwrap());

    let fs = TorrentFsFilesystem::new_with_async(
        state_path.clone(),
        metadata_manager,
        session,
    );

    let options = vec![
        fuser::MountOption::FSName("torrentfs".to_string()),
        fuser::MountOption::AutoUnmount,
    ];
    let guard = fuser::spawn_mount2(fs, &mount_path, &options).unwrap();

    thread::sleep(Duration::from_millis(500));

    let torrent_files = all_torrent_files();
    if torrent_files.len() < 3 {
        eprintln!("Need at least 3 .torrent files for multi-torrent test, found {}, skipping", torrent_files.len());
        drop(guard);
        return;
    }

    for (idx, src) in torrent_files.iter().enumerate() {
        let dest = mount_path.join("metadata").join(format!("{}_{}.torrent", idx, src.file_name().unwrap().to_string_lossy()));
        fs::copy(&src, &dest).expect("Failed to copy .torrent file");
    }

    thread::sleep(Duration::from_millis(500));

    let data_entries: Vec<_> = fs::read_dir(mount_path.join("data"))
        .unwrap()
        .filter_map(|e| e.ok())
        .collect();
    
    if data_entries.len() < 3 {
        eprintln!("Expected at least 3 torrent directories in data/, found {}, skipping multi-torrent test", data_entries.len());
        drop(guard);
        return;
    }

    let mut torrent_info_list: Vec<(PathBuf, Vec<PathBuf>)> = Vec::new();
    
    for entry in &data_entries {
        let torrent_dir = entry.path();
        let file_list: Vec<_> = fs::read_dir(&torrent_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .collect();
        
        if !file_list.is_empty() {
            torrent_info_list.push((torrent_dir, file_list));
        }
    }

    if torrent_info_list.len() < 3 {
        eprintln!("Need at least 3 torrents with files, found {}, skipping", torrent_info_list.len());
        drop(guard);
        return;
    }

    let num_threads = 10;
    let test_duration = Duration::from_secs(300);
    let start_time = Instant::now();
    
    let total_operations = Arc::new(AtomicUsize::new(0));
    let total_errors = Arc::new(AtomicUsize::new(0));
    let total_bytes_read = Arc::new(AtomicUsize::new(0));
    
    let torrent_stats: Arc<std::sync::Mutex<Vec<TorrentStats>>> = Arc::new(std::sync::Mutex::new(
        torrent_info_list.iter().map(|(dir, _)| TorrentStats {
            name: dir.file_name().unwrap().to_string_lossy().into_owned(),
            operations: 0,
            errors: 0,
            bytes_read: 0,
        }).collect()
    ));
    
    let mut handles = Vec::new();

    for thread_id in 0..num_threads {
        let torrent_info = torrent_info_list.clone();
        let ops = Arc::clone(&total_operations);
        let errs = Arc::clone(&total_errors);
        let bytes = Arc::clone(&total_bytes_read);
        let stats = Arc::clone(&torrent_stats);
        
        let handle = thread::spawn(move || {
            let mut local_ops = 0usize;
            let mut local_errs = 0usize;
            let mut local_bytes = 0usize;
            let mut local_torrent_ops: Vec<usize> = vec![0; torrent_info.len()];
            
            while start_time.elapsed() < test_duration {
                let torrent_idx = rand::random::<usize>() % torrent_info.len();
                let (torrent_dir, files) = &torrent_info[torrent_idx];
                
                let file_idx = rand::random::<usize>() % files.len();
                let file_path = &files[file_idx];
                
                match fs::metadata(file_path) {
                    Ok(metadata) => {
                        if metadata.is_file() {
                            match fs::read(file_path) {
                                Ok(data) => {
                                    local_ops += 1;
                                    local_bytes += data.len();
                                    local_torrent_ops[torrent_idx] += 1;
                                }
                                Err(e) => {
                                    if e.kind() != std::io::ErrorKind::BrokenPipe {
                                        local_errs += 1;
                                        eprintln!("Thread {} read error in torrent {:?}: {}", thread_id, torrent_dir, e);
                                    }
                                }
                            }
                        } else {
                            match fs::read_dir(file_path) {
                                Ok(_) => {
                                    local_ops += 1;
                                    local_torrent_ops[torrent_idx] += 1;
                                }
                                Err(e) => {
                                    local_errs += 1;
                                    eprintln!("Thread {} readdir error in torrent {:?}: {}", thread_id, torrent_dir, e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        local_errs += 1;
                        eprintln!("Thread {} metadata error in torrent {:?}: {}", thread_id, torrent_dir, e);
                    }
                }
                
                thread::sleep(Duration::from_millis(10));
            }
            
            ops.fetch_add(local_ops, Ordering::Relaxed);
            errs.fetch_add(local_errs, Ordering::Relaxed);
            bytes.fetch_add(local_bytes, Ordering::Relaxed);
            
            {
                let mut stats_guard = stats.lock().unwrap();
                for (idx, count) in local_torrent_ops.iter().enumerate() {
                    stats_guard[idx].operations += *count;
                }
            }
            
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
            
            println!("[{:.1}s] Operations: {}, Errors: {}, Bytes: {:.2} MB", 
                elapsed.as_secs_f64(),
                ops,
                errs,
                bytes as f64 / 1_048_576.0
            );
            
            {
                let stats_guard = torrent_stats.lock().unwrap();
                print!("  Per-torrent stats: ");
                for stat in stats_guard.iter() {
                    print!("{}={}ops ", stat.name, stat.operations);
                }
                println!();
            }
            
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

    println!("\n=== Multi-Torrent Concurrent Read Test Summary ===");
    println!("Test duration: {:?}", actual_duration);
    println!("Total operations: {}", final_ops);
    println!("Total errors: {}", final_errs);
    println!("Total bytes read: {:.2} MB", final_bytes as f64 / 1_048_576.0);
    println!("Operations per second: {:.2}", final_ops as f64 / actual_duration.as_secs_f64());
    println!("Throughput: {:.2} MB/s", (final_bytes as f64 / 1_048_576.0) / actual_duration.as_secs_f64());
    
    println!("\n=== Per-Torrent Statistics ===");
    {
        let stats_guard = torrent_stats.lock().unwrap();
        for stat in stats_guard.iter() {
            println!("  {}: {} operations", stat.name, stat.operations);
        }
        
        let ops_values: Vec<usize> = stats_guard.iter().map(|s| s.operations).collect();
        let min_ops = ops_values.iter().min().unwrap_or(&0);
        let max_ops = ops_values.iter().max().unwrap_or(&0);
        println!("\n  Load distribution: min={} ops, max={} ops", min_ops, max_ops);
        
        assert!(*min_ops > 0, "All torrents should be accessed during test");
    }
    
    assert_eq!(final_errs, 0, "No errors should occur during concurrent multi-torrent access");
}

#[test]
fn test_torrent_switching_stress() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt.block_on(TorrentRuntime::new(&state_path)).expect("TorrentRuntime::new() should succeed");
    let metadata_manager = Arc::new(
        MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = Arc::new(Session::new().unwrap());

    let fs = TorrentFsFilesystem::new_with_async(
        state_path.clone(),
        metadata_manager,
        session,
    );

    let options = vec![
        fuser::MountOption::FSName("torrentfs".to_string()),
        fuser::MountOption::AutoUnmount,
    ];
    let guard = fuser::spawn_mount2(fs, &mount_path, &options).unwrap();

    thread::sleep(Duration::from_millis(500));

    let torrent_files = all_torrent_files();
    if torrent_files.len() < 3 {
        eprintln!("Need at least 3 .torrent files for switching test, found {}, skipping", torrent_files.len());
        drop(guard);
        return;
    }

    for (idx, src) in torrent_files.iter().enumerate() {
        let dest = mount_path.join("metadata").join(format!("switch_{}.torrent", idx));
        fs::copy(&src, &dest).expect("Failed to copy .torrent file");
    }

    thread::sleep(Duration::from_millis(500));

    let torrent_dirs: Vec<_> = fs::read_dir(mount_path.join("data"))
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .collect();

    if torrent_dirs.len() < 3 {
        eprintln!("Expected at least 3 torrent directories, found {}, skipping", torrent_dirs.len());
        drop(guard);
        return;
    }

    let num_threads = 5;
    let iterations = 100;
    let total_switches = Arc::new(AtomicUsize::new(0));
    let total_errors = Arc::new(AtomicUsize::new(0));
    
    let mut handles = Vec::new();

    for thread_id in 0..num_threads {
        let dirs = torrent_dirs.clone();
        let switches = Arc::clone(&total_switches);
        let errors = Arc::clone(&total_errors);
        
        let handle = thread::spawn(move || {
            let mut local_switches = 0usize;
            let mut local_errors = 0usize;
            
            for _ in 0..iterations {
                for torrent_dir in &dirs {
                    let file_list: Vec<_> = fs::read_dir(torrent_dir)
                        .unwrap()
                        .filter_map(|e| e.ok())
                        .map(|e| e.path())
                        .collect();
                    
                    if file_list.is_empty() {
                        continue;
                    }
                    
                    for file_path in &file_list {
                        match fs::metadata(file_path) {
                            Ok(_) => {
                                local_switches += 1;
                            }
                            Err(e) => {
                                local_errors += 1;
                                eprintln!("Thread {} error accessing {:?}: {}", thread_id, file_path, e);
                            }
                        }
                    }
                    
                    thread::sleep(Duration::from_millis(5));
                }
            }
            
            switches.fetch_add(local_switches, Ordering::Relaxed);
            errors.fetch_add(local_errors, Ordering::Relaxed);
        });
        
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    drop(guard);

    let final_switches = total_switches.load(Ordering::Relaxed);
    let final_errors = total_errors.load(Ordering::Relaxed);

    println!("\n=== Torrent Switching Stress Test Summary ===");
    println!("Total torrent switches: {}", final_switches);
    println!("Total errors: {}", final_errors);
    println!("Number of threads: {}", num_threads);
    println!("Iterations per thread: {}", iterations);
    
    assert_eq!(final_errors, 0, "No errors should occur during torrent switching");
    assert!(final_switches > 0, "Should have performed file switches");
}

#[test]
fn test_no_resource_conflict_between_torrents() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt.block_on(TorrentRuntime::new(&state_path)).expect("TorrentRuntime::new() should succeed");
    let metadata_manager = Arc::new(
        MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = Arc::new(Session::new().unwrap());

    let fs = TorrentFsFilesystem::new_with_async(
        state_path.clone(),
        metadata_manager,
        session,
    );

    let options = vec![
        fuser::MountOption::FSName("torrentfs".to_string()),
        fuser::MountOption::AutoUnmount,
    ];
    let guard = fuser::spawn_mount2(fs, &mount_path, &options).unwrap();

    thread::sleep(Duration::from_millis(500));

    let torrent_files = all_torrent_files();
    if torrent_files.len() < 3 {
        eprintln!("Need at least 3 .torrent files for conflict test, found {}, skipping", torrent_files.len());
        drop(guard);
        return;
    }

    for (idx, src) in torrent_files.iter().enumerate() {
        let dest = mount_path.join("metadata").join(format!("conflict_{}.torrent", idx));
        fs::copy(&src, &dest).expect("Failed to copy .torrent file");
    }

    thread::sleep(Duration::from_millis(500));

    let torrent_dirs: Vec<_> = fs::read_dir(mount_path.join("data"))
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .collect();

    let num_threads = 3;
    let test_duration = Duration::from_secs(60);
    let start_time = Instant::now();
    
    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for thread_id in 0..num_threads {
        let dirs = torrent_dirs.clone();
        let errs = Arc::clone(&errors);
        let test_id = thread_id;
        
        let handle = thread::spawn(move || {
            while start_time.elapsed() < test_duration {
                for torrent_dir in &dirs {
                    match fs::read_dir(torrent_dir) {
                        Ok(entries) => {
                            for entry in entries.filter_map(|e| e.ok()) {
                                let path = entry.path();
                                if path.is_file() {
                                    let mut file = match std::fs::File::open(&path) {
                                        Ok(f) => f,
                                        Err(_) => {
                                            errs.fetch_add(1, Ordering::Relaxed);
                                            continue;
                                        }
                                    };
                                    
                                    use std::io::Read;
                                    let mut buf = [0u8; 1024];
                                    let _ = file.read(&mut buf);
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Thread {} error reading {:?}: {}", test_id, torrent_dir, e);
                            errs.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                thread::sleep(Duration::from_millis(10));
            }
        });
        
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    drop(guard);

    let final_errors = errors.load(Ordering::Relaxed);
    
    println!("\n=== Resource Conflict Test Summary ===");
    println!("Test duration: {:?}", test_duration);
    println!("Number of threads: {}", num_threads);
    println!("Number of torrents: {}", torrent_dirs.len());
    println!("Total errors: {}", final_errors);
    
    assert_eq!(final_errors, 0, "No resource conflicts should occur between torrents");
}
