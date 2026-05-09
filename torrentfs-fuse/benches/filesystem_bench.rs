use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput, BenchmarkId};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use torrentfs::metadata::MetadataManager;
use torrentfs::TorrentRuntime;
use torrentfs_fuse::TorrentFsFilesystem;
use torrentfs_libtorrent::Session;

fn test_torrent_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../test_data")
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

fn setup_benchmark_env() -> (TempDir, PathBuf, PathBuf) {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();
    
    (mount_dir, mount_path, state_path)
}

fn bench_read_operations(c: &mut Criterion) {
    let (_mount_dir, mount_path, state_path) = setup_benchmark_env();
    
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
            eprintln!("No .torrent files found, skipping benchmark");
            drop(guard);
            return;
        }
    };

    let dest = mount_path.join("metadata").join(src.file_name().unwrap());
    fs::copy(&src, &dest).expect("Failed to copy .torrent file");

    let max_wait = Duration::from_secs(10);
    let wait_start = std::time::Instant::now();
    let mut data_entries = Vec::new();

    while wait_start.elapsed() < max_wait {
        data_entries = fs::read_dir(mount_path.join("data"))
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        if !data_entries.is_empty() {
            break;
        }
        thread::sleep(Duration::from_millis(200));
    }

    let torrent_dir = data_entries[0].path();
    let file_list: Vec<_> = fs::read_dir(&torrent_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .collect();

    let file_path = file_list.iter()
        .find(|p| p.is_file())
        .expect("Should have at least one file")
        .clone();

    let metadata = fs::metadata(&file_path).unwrap();
    let file_size = metadata.len() as usize;
    
    c.throughput(Throughput::Bytes(file_size as u64));

    let mut group = c.benchmark_group("read_operations");
    group.measurement_time(Duration::from_secs(10));
    
    group.bench_function("file_read", |b| {
        b.iter(|| {
            let data = fs::read(black_box(&file_path)).unwrap();
            black_box(data);
        });
    });

    group.finish();

    drop(guard);
}

fn bench_metadata_operations(c: &mut Criterion) {
    let (_mount_dir, mount_path, state_path) = setup_benchmark_env();
    
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
            eprintln!("No .torrent files found, skipping benchmark");
            drop(guard);
            return;
        }
    };

    let dest = mount_path.join("metadata").join(src.file_name().unwrap());
    fs::copy(&src, &dest).expect("Failed to copy .torrent file");

    let max_wait = Duration::from_secs(10);
    let wait_start = std::time::Instant::now();
    let mut data_entries = Vec::new();

    while wait_start.elapsed() < max_wait {
        data_entries = fs::read_dir(mount_path.join("data"))
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        if !data_entries.is_empty() {
            break;
        }
        thread::sleep(Duration::from_millis(200));
    }

    let torrent_dir = data_entries[0].path();
    let metadata_dir = mount_path.join("metadata");
    let data_dir = mount_path.join("data");

    let mut group = c.benchmark_group("metadata_operations");
    group.measurement_time(Duration::from_secs(10));
    
    group.bench_function("stat_file", |b| {
        b.iter(|| {
            let metadata = fs::metadata(black_box(&torrent_dir)).unwrap();
            black_box(metadata);
        });
    });

    group.bench_function("readdir", |b| {
        b.iter(|| {
            let entries: Vec<_> = fs::read_dir(black_box(&data_dir))
                .unwrap()
                .filter_map(|e| e.ok())
                .collect();
            black_box(entries);
        });
    });

    group.bench_function("stat_metadata_dir", |b| {
        b.iter(|| {
            let metadata = fs::metadata(black_box(&metadata_dir)).unwrap();
            black_box(metadata);
        });
    });

    group.finish();

    drop(guard);
}

fn bench_concurrent_access(c: &mut Criterion) {
    let (_mount_dir, mount_path, state_path) = setup_benchmark_env();
    
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
            eprintln!("No .torrent files found, skipping benchmark");
            drop(guard);
            return;
        }
    };

    let dest = mount_path.join("metadata").join(src.file_name().unwrap());
    fs::copy(&src, &dest).expect("Failed to copy .torrent file");

    let max_wait = Duration::from_secs(10);
    let wait_start = std::time::Instant::now();
    let mut data_entries = Vec::new();

    while wait_start.elapsed() < max_wait {
        data_entries = fs::read_dir(mount_path.join("data"))
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        if !data_entries.is_empty() {
            break;
        }
        thread::sleep(Duration::from_millis(200));
    }

    let torrent_dir = data_entries[0].path();
    let file_list: Vec<_> = fs::read_dir(&torrent_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .collect();

    let mut group = c.benchmark_group("concurrent_access");
    
    for threads in [1, 2, 4, 8].iter() {
        group.bench_with_input(BenchmarkId::new("concurrent_read", threads), threads, |b, &threads| {
            b.iter(|| {
                let file_list = file_list.clone();
                let handles: Vec<_> = (0..threads)
                    .map(|_| {
                        let files = file_list.clone();
                        thread::spawn(move || {
                            for file_path in &files {
                                if file_path.is_file() {
                                    let _ = fs::read(file_path);
                                }
                            }
                        })
                    })
                    .collect();

                for handle in handles {
                    handle.join().unwrap();
                }
            });
        });
    }

    group.finish();

    drop(guard);
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(5))
        .sample_size(50);
    targets = bench_read_operations, bench_metadata_operations, bench_concurrent_access
}

criterion_main!(benches);
