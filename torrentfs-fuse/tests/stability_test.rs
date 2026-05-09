mod resource_monitor;

use resource_monitor::{ResourceMonitor, print_resource_chart, ResourceReport};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
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

fn get_open_fd_count() -> usize {
    #[cfg(target_os = "linux")]
    {
        let pid = std::process::id();
        let fd_dir = format!("/proc/{}/fd", pid);
        if let Ok(entries) = fs::read_dir(&fd_dir) {
            return entries.count();
        }
    }
    #[cfg(target_os = "macos")]
    {
        if let Ok(output) = Command::new("lsof")
            .args(["-p", &std::process::id().to_string()])
            .output()
        {
            let stdout = String::from_utf8_lossy(&output.stdout);
            return stdout.lines().count().saturating_sub(1);
        }
    }
    0
}

#[derive(Debug, Clone)]
pub struct StabilityTestConfig {
    pub test_duration: Duration,
    pub num_threads: usize,
    pub report_interval: Duration,
    pub sample_interval: Duration,
    pub memory_leak_threshold_mb: f64,
    pub fd_leak_threshold: usize,
    pub enable_crash_recovery: bool,
}

impl Default for StabilityTestConfig {
    fn default() -> Self {
        Self {
            test_duration: Duration::from_secs(300),
            num_threads: 10,
            report_interval: Duration::from_secs(30),
            sample_interval: Duration::from_secs(1),
            memory_leak_threshold_mb: 100.0,
            fd_leak_threshold: 50,
            enable_crash_recovery: false,
        }
    }
}

impl StabilityTestConfig {
    pub fn from_env() -> Self {
        let test_duration = std::env::var("TORRENTFS_STABILITY_DURATION_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(300));
        
        let num_threads = std::env::var("TORRENTFS_STABILITY_THREADS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10);
        
        Self {
            test_duration,
            num_threads,
            ..Default::default()
        }
    }
    
    pub fn long_running() -> Self {
        Self {
            test_duration: Duration::from_secs(24 * 60 * 60),
            num_threads: 20,
            report_interval: Duration::from_secs(300),
            sample_interval: Duration::from_secs(5),
            memory_leak_threshold_mb: 200.0,
            fd_leak_threshold: 100,
            enable_crash_recovery: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StabilityTestReport {
    pub config: StabilityTestConfig,
    pub resource_report: ResourceReport,
    pub total_operations: usize,
    pub total_errors: usize,
    pub total_bytes_read: usize,
    pub operations_per_second: f64,
    pub throughput_mbps: f64,
    pub fd_count_start: usize,
    pub fd_count_end: usize,
    pub fd_leak_detected: bool,
    pub success_rate: f64,
    pub test_duration: Duration,
}

impl StabilityTestReport {
    pub fn print_summary(&self) {
        println!("\n{}", "=".repeat(60));
        println!("STABILITY TEST REPORT");
        println!("{}", "=".repeat(60));
        
        println!("\n[Test Configuration]");
        println!("  Duration: {:?}", self.config.test_duration);
        println!("  Threads: {}", self.config.num_threads);
        println!("  Memory leak threshold: {:.1} MB", self.config.memory_leak_threshold_mb);
        println!("  FD leak threshold: {}", self.config.fd_leak_threshold);
        
        println!("\n[Resource Metrics]");
        self.resource_report.print_summary();
        
        println!("\n[Performance Metrics]");
        println!("  Total operations: {}", self.total_operations);
        println!("  Total errors: {}", self.total_errors);
        println!("  Total bytes read: {:.2} MB", self.total_bytes_read as f64 / 1_048_576.0);
        println!("  Operations/sec: {:.2}", self.operations_per_second);
        println!("  Throughput: {:.2} MB/s", self.throughput_mbps);
        println!("  Success rate: {:.2}%", self.success_rate);
        
        println!("\n[File Descriptor Analysis]");
        println!("  FD count at start: {}", self.fd_count_start);
        println!("  FD count at end: {}", self.fd_count_end);
        println!("  FD growth: {}", self.fd_count_end.saturating_sub(self.fd_count_start));
        println!("  FD leak detected: {}", if self.fd_leak_detected { "YES" } else { "NO" });
        
        println!("\n[Test Result]");
        if self.resource_report.potential_memory_leak {
            println!("  STATUS: FAILED - Memory leak detected");
        } else if self.fd_leak_detected {
            println!("  STATUS: FAILED - File descriptor leak detected");
        } else if self.total_errors > 0 {
            println!("  STATUS: FAILED - Errors occurred during test");
        } else {
            println!("  STATUS: PASSED");
        }
        println!("{}", "=".repeat(60));
    }
}

#[test]
fn test_file_descriptor_leak_detection() {
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

    let src = first_torrent_file()
        .expect("No .torrent files found in test_data directory");

    let dest = mount_path.join("metadata").join(src.file_name().unwrap());
    fs::copy(&src, &dest).expect("Failed to copy .torrent file");

    let max_wait = Duration::from_secs(10);
    let wait_start = Instant::now();
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

    if data_entries.is_empty() {
        panic!("No torrent directories appeared in data/");
    }

    let torrent_dir = data_entries[0].path();
    let file_list: Vec<_> = fs::read_dir(&torrent_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .collect();

    let initial_fd_count = get_open_fd_count();
    println!("Initial FD count: {}", initial_fd_count);

    let iterations = 1000;
    let mut max_fd_count = initial_fd_count;

    for i in 0..iterations {
        for file_path in &file_list {
            if file_path.is_file() {
                let _ = fs::read(file_path);
            }
        }
        
        if i % 100 == 0 {
            let current_fd_count = get_open_fd_count();
            max_fd_count = max_fd_count.max(current_fd_count);
            println!("Iteration {}: FD count = {}", i, current_fd_count);
        }
    }

    let final_fd_count = get_open_fd_count();
    println!("Final FD count: {}", final_fd_count);
    
    drop(guard);

    let fd_growth = final_fd_count.saturating_sub(initial_fd_count);
    println!("FD growth: {}", fd_growth);
    
    assert!(
        fd_growth < 50,
        "Potential file descriptor leak: FD count grew by {} (threshold: 50)",
        fd_growth
    );
}

#[test]
fn test_long_running_endurance() {
    let config = StabilityTestConfig::from_env();
    
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

    let src = first_torrent_file()
        .expect("No .torrent files found in test_data directory");

    let dest = mount_path.join("metadata").join(src.file_name().unwrap());
    fs::copy(&src, &dest).expect("Failed to copy .torrent file");

    let max_wait = Duration::from_secs(10);
    let wait_start = Instant::now();
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

    if data_entries.is_empty() {
        panic!("No torrent directories appeared in data/");
    }

    let torrent_dir = data_entries[0].path();
    let file_list: Vec<_> = fs::read_dir(&torrent_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .collect();

    let mut resource_monitor = ResourceMonitor::new();
    let monitor_handle = resource_monitor.start(config.sample_interval);

    let initial_fd_count = get_open_fd_count();

    let start_time = Instant::now();
    let total_operations = Arc::new(AtomicUsize::new(0));
    let total_errors = Arc::new(AtomicUsize::new(0));
    let total_bytes_read = Arc::new(AtomicUsize::new(0));
    let running = Arc::new(AtomicBool::new(true));

    let mut handles = Vec::new();

    for thread_id in 0..config.num_threads {
        let files = file_list.clone();
        let ops = Arc::clone(&total_operations);
        let errs = Arc::clone(&total_errors);
        let bytes = Arc::clone(&total_bytes_read);
        let run = Arc::clone(&running);
        let test_duration = config.test_duration;

        let handle = thread::spawn(move || {
            let mut local_ops = 0usize;
            let mut local_errs = 0usize;
            let mut local_bytes = 0usize;

            while run.load(Ordering::Relaxed) && start_time.elapsed() < test_duration {
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
                                        if local_errs <= 10 {
                                            eprintln!("Thread {} read error: {}", thread_id, e);
                                        }
                                    }
                                }
                            }
                        } else {
                            match fs::read_dir(file_path) {
                                Ok(_) => local_ops += 1,
                                Err(_) => {
                                    local_errs += 1;
                                }
                            }
                        }
                    }
                    Err(_e) => {
                        local_errs += 1;
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

    let mut last_report = Instant::now();
    while start_time.elapsed() < config.test_duration {
        if last_report.elapsed() >= config.report_interval {
            let ops = total_operations.load(Ordering::Relaxed);
            let errs = total_errors.load(Ordering::Relaxed);
            let bytes = total_bytes_read.load(Ordering::Relaxed);
            let elapsed = start_time.elapsed();
            let current_memory = resource_monitor.get_current_memory_mb();
            let current_fd = get_open_fd_count();

            println!(
                "[{:.1}s] Ops: {}, Errs: {}, Bytes: {:.2} MB, Memory: {:.1} MB, FDs: {}",
                elapsed.as_secs_f64(),
                ops,
                errs,
                bytes as f64 / 1_048_576.0,
                current_memory,
                current_fd
            );
            last_report = Instant::now();
        }
        thread::sleep(Duration::from_millis(100));
    }

    running.store(false, Ordering::Relaxed);

    for handle in handles {
        let _ = handle.join();
    }

    resource_monitor.stop();
    monitor_handle.join().expect("Resource monitor thread should join");

    drop(guard);

    let final_fd_count = get_open_fd_count();
    let resource_report = resource_monitor.generate_report();
    let final_ops = total_operations.load(Ordering::Relaxed);
    let final_errs = total_errors.load(Ordering::Relaxed);
    let final_bytes = total_bytes_read.load(Ordering::Relaxed);
    let actual_duration = start_time.elapsed();

    let report = StabilityTestReport {
        config: config.clone(),
        resource_report,
        total_operations: final_ops,
        total_errors: final_errs,
        total_bytes_read: final_bytes,
        operations_per_second: final_ops as f64 / actual_duration.as_secs_f64(),
        throughput_mbps: (final_bytes as f64 / 1_048_576.0) / actual_duration.as_secs_f64(),
        fd_count_start: initial_fd_count,
        fd_count_end: final_fd_count,
        fd_leak_detected: final_fd_count.saturating_sub(initial_fd_count) > config.fd_leak_threshold,
        success_rate: if final_ops > 0 {
            ((final_ops - final_errs) as f64 / final_ops as f64) * 100.0
        } else {
            0.0
        },
        test_duration: actual_duration,
    };

    report.print_summary();
    print_resource_chart(&report.resource_report, 80);

    assert!(
        !report.resource_report.potential_memory_leak,
        "Memory leak detected: memory grew by {:.2} MB",
        report.resource_report.memory_growth_mb
    );
    
    assert!(
        !report.fd_leak_detected,
        "File descriptor leak detected: FD count grew by {}",
        final_fd_count.saturating_sub(initial_fd_count)
    );
}

#[test]
fn test_crash_recovery_simulation() {
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_path_buf();

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

    let src = first_torrent_file()
        .expect("No .torrent files found in test_data directory");

    let dest = mount_path.join("metadata").join(src.file_name().unwrap());
    fs::copy(&src, &dest).expect("Failed to copy .torrent file");

    thread::sleep(Duration::from_millis(500));

    let torrents_before = rt.block_on(runtime.metadata_manager.list_torrents()).unwrap();
    let torrent_count_before = torrents_before.len();
    println!("Torrents before crash simulation: {}", torrent_count_before);

    drop(guard);
    drop(runtime);

    println!("Simulated crash - dropped runtime without graceful shutdown");

    let rt2 = tokio::runtime::Runtime::new().unwrap();
    let runtime2 = rt2
        .block_on(TorrentRuntime::new(&state_path))
        .expect("TorrentRuntime::new() after crash should succeed");

    let torrents_after = rt2.block_on(runtime2.metadata_manager.list_torrents()).unwrap();
    let torrent_count_after = torrents_after.len();
    println!("Torrents after recovery: {}", torrent_count_after);

    assert!(
        torrent_count_after >= torrent_count_before,
        "Torrents should be restored after crash. Before: {}, After: {}",
        torrent_count_before,
        torrent_count_after
    );

    println!("Crash recovery test passed - {} torrents restored", torrent_count_after);
}

#[test]
fn test_performance_benchmark() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();
    let cache_dir = TempDir::new().unwrap();
    let piece_cache = torrentfs::PieceCache::with_cache_dir(cache_dir.path().to_path_buf()).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt
        .block_on(TorrentRuntime::new(&state_path))
        .expect("TorrentRuntime::new() should succeed");
    let metadata_manager = Arc::new(MetadataManager::new(runtime.db.clone()).unwrap());
    let session = Arc::clone(&runtime.session);
    let piece_cache = Arc::new(piece_cache);

    let src = first_torrent_file().expect("No .torrent files found");
    let torrent_data = fs::read(&src).expect("Failed to read torrent file");
    let torrent_info = torrentfs_libtorrent::parse_torrent(&torrent_data).expect("Failed to parse torrent");
    let info_hash = torrent_info.info_hash.clone();

    let total_size: usize = torrent_info.files.iter().map(|f| f.size as usize).sum();
    let mut all_content: Vec<u8> = Vec::with_capacity(total_size);
    for i in 0..total_size {
        all_content.push((i % 256) as u8);
    }

    let piece_size = torrent_info.piece_size as usize;
    for piece_idx in 0..=((total_size - 1) / piece_size) {
        let start = piece_idx * piece_size;
        let end = std::cmp::min(start + piece_size, all_content.len());
        piece_cache
            .write_piece(&info_hash, piece_idx as u32, &all_content[start..end])
            .expect("Failed to write piece to cache");
    }

    session
        .add_torrent_paused(&torrent_data, "/tmp/torrentfs")
        .expect("Failed to add torrent");

    let download_coordinator = Arc::new(torrentfs::DownloadCoordinator::new(
        Arc::clone(&session),
        Arc::clone(&piece_cache),
    ));

    let fs = TorrentFsFilesystem::new_with_download_coordinator(
        state_path.clone(),
        metadata_manager,
        session,
        download_coordinator,
    );

    let options = vec![
        fuser::MountOption::FSName("torrentfs".to_string()),
        fuser::MountOption::AutoUnmount,
    ];
    let guard = fuser::spawn_mount2(fs, &mount_path, &options).unwrap();

    thread::sleep(Duration::from_millis(500));

    let dest = mount_path.join("metadata").join(src.file_name().unwrap());
    fs::copy(&src, &dest).expect("Failed to copy .torrent file");

    let max_wait = Duration::from_secs(10);
    let wait_start = Instant::now();
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
    
    fn collect_files(dir: &std::path::Path) -> Vec<(std::path::PathBuf, u64)> {
        let mut files = Vec::new();
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.filter_map(|e| e.ok()) {
                let path = entry.path();
                if path.is_file() {
                    if let Ok(metadata) = fs::metadata(&path) {
                        files.push((path, metadata.len()));
                    }
                } else if path.is_dir() {
                    files.extend(collect_files(&path));
                }
            }
        }
        files
    }

    let file_list = collect_files(&torrent_dir);

    let warmup_iterations = 100;
    for _ in 0..warmup_iterations {
        for (path, _) in &file_list {
            let _ = fs::read(path);
        }
    }

    let bench_iterations = 1000;
    let bench_start = Instant::now();
    let mut total_bytes = 0usize;

    for _ in 0..bench_iterations {
        for (path, _size) in &file_list {
            if let Ok(data) = fs::read(path) {
                total_bytes += data.len();
            }
        }
    }

    let bench_duration = bench_start.elapsed();
    drop(guard);

    let total_ops = bench_iterations * file_list.len();
    let ops_per_sec = total_ops as f64 / bench_duration.as_secs_f64();
    let throughput_mbps = (total_bytes as f64 / 1_048_576.0) / bench_duration.as_secs_f64();

    println!("\n{}", "=".repeat(60));
    println!("PERFORMANCE BENCHMARK REPORT");
    println!("{}", "=".repeat(60));
    println!("Files tested: {}", file_list.len());
    println!("Total iterations: {}", bench_iterations);
    println!("Total operations: {}", total_ops);
    println!("Total bytes read: {:.2} MB", total_bytes as f64 / 1_048_576.0);
    println!("Duration: {:?}", bench_duration);
    println!("Operations/sec: {:.2}", ops_per_sec);
    println!("Throughput: {:.2} MB/s", throughput_mbps);
    println!("Latency per read: {:?}", Duration::from_secs_f64(bench_duration.as_secs_f64() / total_ops as f64));
    println!("{}", "=".repeat(60));

    assert!(ops_per_sec > 0.0, "Should have completed operations");
}

#[test]
fn test_repeated_mount_unmount() {
    let iterations = 10;
    let initial_fd = get_open_fd_count();

    for i in 0..iterations {
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

        thread::sleep(Duration::from_millis(100));

        let src = first_torrent_file();
        if let Some(src) = src {
            let dest = mount_path.join("metadata").join(src.file_name().unwrap());
            let _ = fs::copy(&src, &dest);
            thread::sleep(Duration::from_millis(50));
        }

        drop(guard);
        drop(runtime);
        
        let current_fd = get_open_fd_count();
        println!("Iteration {}: FD count = {}", i + 1, current_fd);
    }

    let final_fd = get_open_fd_count();
    let total_fd_growth = final_fd.saturating_sub(initial_fd);

    println!("\nRepeated Mount/Unmount Test Summary:");
    println!("  Iterations: {}", iterations);
    println!("  Initial FD count: {}", initial_fd);
    println!("  Final FD count: {}", final_fd);
    println!("  FD growth: {}", total_fd_growth);

    assert!(
        total_fd_growth < iterations * 10,
        "FD growth should be minimal across mount/unmount cycles. Growth: {}",
        total_fd_growth
    );
}

#[test]
fn test_stress_high_concurrency() {
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

    let src = first_torrent_file()
        .expect("No .torrent files found in test_data directory");

    let dest = mount_path.join("metadata").join(src.file_name().unwrap());
    fs::copy(&src, &dest).expect("Failed to copy .torrent file");

    let max_wait = Duration::from_secs(10);
    let wait_start = Instant::now();
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

    let num_threads = 50;
    let test_duration = Duration::from_secs(60);
    let start_time = Instant::now();

    let total_operations = Arc::new(AtomicU64::new(0));
    let total_errors = Arc::new(AtomicU64::new(0));
    let running = Arc::new(AtomicBool::new(true));

    let mut handles = Vec::new();

    for _ in 0..num_threads {
        let files = file_list.clone();
        let ops = Arc::clone(&total_operations);
        let errs = Arc::clone(&total_errors);
        let run = Arc::clone(&running);

        let handle = thread::spawn(move || {
            let mut local_ops = 0u64;
            let mut local_errs = 0u64;

            while run.load(Ordering::Relaxed) && start_time.elapsed() < test_duration {
                for file_path in &files {
                    if file_path.is_file() {
                        match fs::read(file_path) {
                            Ok(_) => local_ops += 1,
                            Err(e) => {
                                if e.kind() != std::io::ErrorKind::BrokenPipe {
                                    local_errs += 1;
                                }
                            }
                        }
                    } else {
                        match fs::read_dir(file_path) {
                            Ok(_) => local_ops += 1,
                            Err(_) => local_errs += 1,
                        }
                    }
                }
            }

            ops.fetch_add(local_ops, Ordering::Relaxed);
            errs.fetch_add(local_errs, Ordering::Relaxed);
        });

        handles.push(handle);
    }

    let mut last_report = Instant::now();
    while start_time.elapsed() < test_duration {
        if last_report.elapsed() >= Duration::from_secs(10) {
            let ops = total_operations.load(Ordering::Relaxed);
            let errs = total_errors.load(Ordering::Relaxed);
            println!(
                "[{:.1}s] Ops: {}, Errors: {}, Threads: {}",
                start_time.elapsed().as_secs_f64(),
                ops,
                errs,
                num_threads
            );
            last_report = Instant::now();
        }
        thread::sleep(Duration::from_millis(100));
    }

    running.store(false, Ordering::Relaxed);

    for handle in handles {
        handle.join().expect("Thread should not panic");
    }

    drop(guard);

    let final_ops = total_operations.load(Ordering::Relaxed);
    let final_errs = total_errors.load(Ordering::Relaxed);

    println!("\nHigh Concurrency Stress Test Summary:");
    println!("  Threads: {}", num_threads);
    println!("  Duration: {:?}", test_duration);
    println!("  Total operations: {}", final_ops);
    println!("  Total errors: {}", final_errs);
    println!("  Ops/sec: {:.2}", final_ops as f64 / test_duration.as_secs_f64());

    assert_eq!(
        final_errs, 0,
        "No errors should occur during high concurrency test"
    );
}
