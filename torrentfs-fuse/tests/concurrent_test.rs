use std::fs;
use std::path::PathBuf;
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

fn first_torrent_file() -> Option<PathBuf> {
    let dir = test_torrent_dir();
    fs::read_dir(&dir).ok()?.filter_map(|e| {
        let e = e.ok()?;
        let name = e.file_name().to_string_lossy().into_owned();
        if name.ends_with(".torrent") {
            Some(e.path())
        } else {
            None
        }
    }).next()
}

#[test]
fn test_concurrent_random_read_no_deadlock() {
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
    let test_duration = Duration::from_secs(30);
    let start_time = Instant::now();
    let mut handles = Vec::new();

    for thread_id in 0..num_threads {
        let files = file_list.clone();
        let torrent_dir = torrent_dir.clone();
        
        let handle = thread::spawn(move || {
            let mut operations = 0usize;
            let mut errors = 0usize;
            
            while start_time.elapsed() < test_duration {
                let file_idx = rand::random::<usize>() % files.len();
                let file_path = &files[file_idx];
                
                match fs::metadata(file_path) {
                    Ok(metadata) => {
                        if metadata.is_file() {
                            match fs::read(file_path) {
                                Ok(_) => operations += 1,
                                Err(e) => {
                                    if e.kind() != std::io::ErrorKind::BrokenPipe {
                                        errors += 1;
                                        eprintln!("Thread {} read error: {}", thread_id, e);
                                    }
                                }
                            }
                        } else {
                            match fs::read_dir(file_path) {
                                Ok(_) => operations += 1,
                                Err(e) => {
                                    errors += 1;
                                    eprintln!("Thread {} readdir error: {}", thread_id, e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        errors += 1;
                        eprintln!("Thread {} metadata error: {}", thread_id, e);
                    }
                }
                
                thread::sleep(Duration::from_millis(10));
            }
            
            (operations, errors)
        });
        
        handles.push(handle);
    }

    let mut total_operations = 0usize;
    let mut total_errors = 0usize;
    
    for handle in handles {
        let (ops, errs) = handle.join().expect("Thread panicked");
        total_operations += ops;
        total_errors += errs;
    }

    drop(guard);

    println!("Concurrent test completed:");
    println!("  Total operations: {}", total_operations);
    println!("  Total errors: {}", total_errors);
    println!("  Test duration: {:?}", test_duration);
    
    assert_eq!(total_errors, 0, "No errors should occur during concurrent access");
}

#[test]
fn test_concurrent_metadata_operations() {
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

    let num_threads = 10;
    let test_duration = Duration::from_secs(60);
    let start_time = Instant::now();
    let mut handles = Vec::new();

    for thread_id in 0..num_threads {
        let mount_path = mount_path.clone();
        
        let handle = thread::spawn(move || {
            let mut operations = 0usize;
            let mut errors = 0usize;
            
            while start_time.elapsed() < test_duration {
                let metadata_dir = mount_path.join("metadata");
                let data_dir = mount_path.join("data");
                
                match fs::read_dir(&metadata_dir) {
                    Ok(_) => operations += 1,
                    Err(e) => {
                        errors += 1;
                        eprintln!("Thread {} metadata readdir error: {}", thread_id, e);
                    }
                }
                
                match fs::read_dir(&data_dir) {
                    Ok(_) => operations += 1,
                    Err(e) => {
                        errors += 1;
                        eprintln!("Thread {} data readdir error: {}", thread_id, e);
                    }
                }
                
                match fs::metadata(&metadata_dir) {
                    Ok(_) => operations += 1,
                    Err(e) => {
                        errors += 1;
                        eprintln!("Thread {} metadata stat error: {}", thread_id, e);
                    }
                }
                
                thread::sleep(Duration::from_millis(50));
            }
            
            (operations, errors)
        });
        
        handles.push(handle);
    }

    let mut total_operations = 0usize;
    let mut total_errors = 0usize;
    
    for handle in handles {
        let (ops, errs) = handle.join().expect("Thread panicked");
        total_operations += ops;
        total_errors += errs;
    }

    drop(guard);

    println!("Metadata concurrent test completed:");
    println!("  Total operations: {}", total_operations);
    println!("  Total errors: {}", total_errors);
    
    assert_eq!(total_errors, 0, "No errors should occur during concurrent metadata access");
}
