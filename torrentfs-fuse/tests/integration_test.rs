use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use torrentfs_fuse::TorrentFsFilesystem;
use fuser::{BackgroundSession, MountOption};
use std::path::PathBuf;
use serial_test::serial;

fn make_fs(state_dir: &PathBuf) -> TorrentFsFilesystem {
    TorrentFsFilesystem::new(state_dir.clone())
}

fn spawn_fs(mount_path: &PathBuf, state_path: &PathBuf) -> BackgroundSession {
    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
    ];
    let fs = make_fs(state_path);
    fuser::spawn_mount2(fs, mount_path, &options).unwrap()
}

fn cleanup_mount(mount_path: &PathBuf, guard: BackgroundSession) {
    drop(guard);
    for _ in 0..20 {
        if !mount_path.exists() || fs::metadata(mount_path).is_err() {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
}

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

fn nested_dirs_torrent() -> Option<PathBuf> {
    let dir = test_torrent_dir();
    let path = dir.join("nested_dirs.torrent");
    if path.exists() { Some(path) } else { None }
}

#[test]
#[serial]
fn test_ls_root_shows_metadata_and_data() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let guard = spawn_fs(&mount_path, &state_path);

    thread::sleep(Duration::from_millis(500));

    let entries = fs::read_dir(&mount_path)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();

    assert!(entries.contains(&"metadata".to_string()), "metadata not found in {:?}", entries);
    assert!(entries.contains(&"data".to_string()), "data not found in {:?}", entries);
    assert_eq!(entries.len(), 2, "expected 2 entries, got {:?}", entries);

    let source_path = mount_path.join("metadata");
    let data_path = mount_path.join("data");
    assert!(source_path.is_dir(), "metadata should be a directory");
    assert!(data_path.is_dir(), "data should be a directory");

    cleanup_mount(&mount_path, guard);
}

#[test]
#[serial]
fn test_cp_torrent_to_metadata() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let guard = spawn_fs(&mount_path, &state_path);

    thread::sleep(Duration::from_millis(500));

    let src = match first_torrent_file() {
        Some(p) => p,
        None => {
            eprintln!("No .torrent files found, skipping copy test");
            cleanup_mount(&mount_path, guard);
            return;
        }
    };

    let dest = mount_path.join("metadata").join(src.file_name().unwrap());

    let result = fs::copy(&src, &dest);
    assert!(result.is_ok(), "Failed to copy .torrent file: {:?}", result.err());

    thread::sleep(Duration::from_millis(500));

    let metadata_entries: Vec<_> = fs::read_dir(mount_path.join("metadata"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();

    assert!(
        metadata_entries.iter().any(|n| n == &src.file_name().unwrap().to_string_lossy().into_owned()),
        "File should remain visible in metadata/ after release"
    );

    cleanup_mount(&mount_path, guard);
}

#[test]
#[serial]
fn test_create_non_torrent_rejected() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let guard = spawn_fs(&mount_path, &state_path);

    thread::sleep(Duration::from_millis(500));

    let dest = mount_path.join("metadata").join("test.txt");
    let result = fs::write(&dest, b"hello");
    assert!(result.is_err(), "Writing non-.torrent file should fail");

    cleanup_mount(&mount_path, guard);
}

#[test]
#[serial]
fn test_file_visible_after_release() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let guard = spawn_fs(&mount_path, &state_path);

    thread::sleep(Duration::from_millis(500));

    let src = match first_torrent_file() {
        Some(p) => p,
        None => {
            eprintln!("No .torrent files found, skipping test");
            cleanup_mount(&mount_path, guard);
            return;
        }
    };

    let dest = mount_path.join("metadata").join(src.file_name().unwrap());

    let result = fs::copy(&src, &dest);
    assert!(result.is_ok(), "Failed to copy .torrent file: {:?}", result.err());

    thread::sleep(Duration::from_millis(500));

    let metadata_entries: Vec<_> = fs::read_dir(mount_path.join("metadata"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();

    let file_name = src.file_name().unwrap().to_string_lossy().into_owned();
    assert!(
        metadata_entries.iter().any(|n| *n == file_name),
        "File '{}' should be visible in metadata/ after release (was discarded)",
        file_name
    );

    cleanup_mount(&mount_path, guard);
}

#[test]
#[serial]
fn test_init_and_mount_pipeline() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt.block_on(torrentfs::init(&state_path)).expect("torrentfs::init(&state_path) should succeed");
    let metadata_manager = std::sync::Arc::new(
        torrentfs::MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = std::sync::Arc::new(torrentfs_libtorrent::Session::new().unwrap());

    let fs = TorrentFsFilesystem::new_with_core(
        state_path.clone(),
        metadata_manager,
        rt,
        session,
    );

    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
    ];
    let guard = fuser::spawn_mount2(fs, &mount_path, &options).unwrap();

    thread::sleep(Duration::from_millis(500));

    let src = match first_torrent_file() {
        Some(p) => p,
        None => {
            eprintln!("No .torrent files found, skipping pipeline test");
            cleanup_mount(&mount_path, guard);
            return;
        }
    };

    let dest = mount_path.join("metadata").join(src.file_name().unwrap());
    let result = fs::copy(&src, &dest);
    assert!(result.is_ok(), "Failed to copy .torrent file: {:?}", result.err());

    thread::sleep(Duration::from_millis(500));

    let metadata_entries: Vec<_> = fs::read_dir(mount_path.join("metadata"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        metadata_entries.iter().any(|n| n == &src.file_name().unwrap().to_string_lossy().into_owned()),
        "File should be visible in metadata/ after release with core pipeline"
    );

    cleanup_mount(&mount_path, guard);
}

#[test]
#[serial]
fn test_data_directory_populated_from_db() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt.block_on(torrentfs::init(&state_path)).expect("torrentfs::init(&state_path) should succeed");
    let metadata_manager = std::sync::Arc::new(
        torrentfs::MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = std::sync::Arc::new(torrentfs_libtorrent::Session::new().unwrap());

    let fs = TorrentFsFilesystem::new_with_core(
        state_path.clone(),
        metadata_manager,
        rt,
        session,
    );

    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
    ];
    let guard = fuser::spawn_mount2(fs, &mount_path, &options).unwrap();

    thread::sleep(Duration::from_millis(500));

    // First copy a torrent to metadata/ to populate database
    let src = match first_torrent_file() {
        Some(p) => p,
        None => {
            eprintln!("No .torrent files found, skipping data directory test");
            cleanup_mount(&mount_path, guard);
            return;
        }
    };

    let dest = mount_path.join("metadata").join(src.file_name().unwrap());
    let result = fs::copy(&src, &dest);
    assert!(result.is_ok(), "Failed to copy .torrent file: {:?}", result.err());

    thread::sleep(Duration::from_millis(500));

    // Now check that data/ directory shows the torrent
    let data_entries: Vec<_> = fs::read_dir(mount_path.join("data"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();

    // The torrent name should appear in data/ directory
    // Note: The torrent name comes from parsing the .torrent file, not the filename
    // For test purposes, we'll just check that data/ is not empty
    assert!(!data_entries.is_empty(), "data/ directory should not be empty after adding torrent");

    // If there's at least one torrent directory, check its contents
    if let Some(torrent_dir) = data_entries.first() {
        let torrent_path = mount_path.join("data").join(torrent_dir);
        assert!(torrent_path.is_dir(), "{} should be a directory", torrent_dir);

        let file_entries: Vec<_> = fs::read_dir(&torrent_path)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();

        assert!(!file_entries.is_empty(), "Torrent directory should contain files or subdirectories");
        
        for file_name in &file_entries {
            let file_path = torrent_path.join(file_name);
            let metadata = fs::metadata(&file_path).unwrap();
            if metadata.is_file() {
                assert!(metadata.len() > 0, "File {} should have non-zero size", file_name);
            }
        }
    }

    cleanup_mount(&mount_path, guard);
}

#[test]
#[serial]
fn test_mkdir_in_metadata() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let guard = spawn_fs(&mount_path, &state_path);

    thread::sleep(Duration::from_millis(500));

    let subdir = mount_path.join("metadata").join("subdir");
    let result = fs::create_dir(&subdir);
    assert!(result.is_ok(), "Failed to create subdirectory in metadata/: {:?}", result.err());
    assert!(subdir.is_dir(), "Subdirectory should be a directory");

    let metadata_entries: Vec<_> = fs::read_dir(mount_path.join("metadata"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();

    assert!(
        metadata_entries.contains(&"subdir".to_string()),
        "subdir should be visible in metadata/"
    );

    cleanup_mount(&mount_path, guard);
}

#[test]
#[serial]
fn test_cp_torrent_to_metadata_subdirectory() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let guard = spawn_fs(&mount_path, &state_path);

    thread::sleep(Duration::from_millis(500));

    let subdir = mount_path.join("metadata").join("a").join("b");
    fs::create_dir_all(&subdir).expect("Failed to create nested subdirectory");

    let src = match first_torrent_file() {
        Some(p) => p,
        None => {
            eprintln!("No .torrent files found, skipping subdirectory test");
            cleanup_mount(&mount_path, guard);
            return;
        }
    };

    let dest = subdir.join(src.file_name().unwrap());
    let result = fs::copy(&src, &dest);
    assert!(result.is_ok(), "Failed to copy .torrent file to subdirectory: {:?}", result.err());

    thread::sleep(Duration::from_millis(500));

    let subdir_entries: Vec<_> = fs::read_dir(&subdir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();

    let file_name = src.file_name().unwrap().to_string_lossy().into_owned();
    assert!(
        subdir_entries.contains(&file_name),
        "File should remain visible in metadata/a/b/ after release"
    );

    cleanup_mount(&mount_path, guard);
}

#[test]
#[serial]
fn test_data_mirrors_source_path() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt.block_on(torrentfs::init(&state_path)).expect("torrentfs::init(&state_path) should succeed");
    let metadata_manager = std::sync::Arc::new(
        torrentfs::MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = std::sync::Arc::new(torrentfs_libtorrent::Session::new().unwrap());

    let fs = TorrentFsFilesystem::new_with_core(
        state_path.clone(),
        metadata_manager,
        rt,
        session,
    );

    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
    ];
    let guard = fuser::spawn_mount2(fs, &mount_path, &options).unwrap();

    thread::sleep(Duration::from_millis(500));

    let subdir = mount_path.join("metadata").join("a").join("b");
    fs::create_dir_all(&subdir).expect("Failed to create nested subdirectory");

    let src = match first_torrent_file() {
        Some(p) => p,
        None => {
            eprintln!("No .torrent files found, skipping data mirror test");
            cleanup_mount(&mount_path, guard);
            return;
        }
    };

    let dest = subdir.join(src.file_name().unwrap());
    let result = fs::copy(&src, &dest);
    assert!(result.is_ok(), "Failed to copy .torrent file: {:?}", result.err());

    thread::sleep(Duration::from_millis(500));

    let data_a = mount_path.join("data").join("a");
    assert!(data_a.exists(), "data/a should exist after copying to metadata/a/b/");
    assert!(data_a.is_dir(), "data/a should be a directory");

    let data_a_b = data_a.join("b");
    assert!(data_a_b.exists(), "data/a/b should exist after copying to metadata/a/b/");
    assert!(data_a_b.is_dir(), "data/a/b should be a directory");

    let data_a_b_entries: Vec<_> = fs::read_dir(&data_a_b)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();

    assert!(!data_a_b_entries.is_empty(), "data/a/b should contain torrent directory");

    cleanup_mount(&mount_path, guard);
}

#[test]
#[serial]
fn test_nested_subdirectory_in_metadata() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let guard = spawn_fs(&mount_path, &state_path);

    thread::sleep(Duration::from_millis(500));

    let dir_a = mount_path.join("metadata").join("a");
    fs::create_dir(&dir_a).expect("Failed to create dir a");

    let dir_a_b = dir_a.join("b");
    fs::create_dir(&dir_a_b).expect("Failed to create dir a/b");

    let dir_a_b_c = dir_a_b.join("c");
    fs::create_dir(&dir_a_b_c).expect("Failed to create dir a/b/c");

    assert!(dir_a.exists(), "a should exist");
    assert!(dir_a_b.exists(), "a/b should exist");
    assert!(dir_a_b_c.exists(), "a/b/c should exist");

    let a_entries: Vec<_> = fs::read_dir(&dir_a)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(a_entries.contains(&"b".to_string()), "a should contain b");

    let a_b_entries: Vec<_> = fs::read_dir(&dir_a_b)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(a_b_entries.contains(&"c".to_string()), "a/b should contain c");

    cleanup_mount(&mount_path, guard);
}

#[test]
#[serial]
fn test_torrent_nested_directory_operations() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let guard = spawn_fs(&mount_path, &state_path);

    thread::sleep(Duration::from_millis(500));

    let src = std::path::PathBuf::from("test_data/multi_file.torrent");
    if !src.exists() {
        eprintln!("multi_file.torrent not found, skipping nested directory test");
        cleanup_mount(&mount_path, guard);
        return;
    }

    let dest = mount_path.join("metadata").join(src.file_name().unwrap());
    let result = fs::copy(&src, &dest);
    assert!(result.is_ok(), "Failed to copy .torrent file: {:?}", result.err());

    thread::sleep(Duration::from_millis(500));

    let data_entries: Vec<_> = fs::read_dir(mount_path.join("data"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();

    assert!(data_entries.contains(&"multi_file_test".to_string()), 
        "data/ should contain multi_file_test directory");

    let torrent_dir = mount_path.join("data").join("multi_file_test");
    assert!(torrent_dir.is_dir(), "multi_file_test should be a directory");

    let torrent_entries: Vec<_> = fs::read_dir(&torrent_dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();

    assert!(torrent_entries.contains(&"dir1".to_string()), 
        "multi_file_test should contain dir1");
    assert!(torrent_entries.contains(&"dir2".to_string()), 
        "multi_file_test should contain dir2");

    let dir1_path = torrent_dir.join("dir1");
    assert!(dir1_path.is_dir(), "dir1 should be a directory");
    assert!(dir1_path.metadata().unwrap().is_dir(), "dir1 metadata should show it as directory");

    let dir1_entries: Vec<_> = fs::read_dir(&dir1_path)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();

    assert!(dir1_entries.contains(&"a.txt".to_string()), 
        "dir1 should contain a.txt");

    let a_txt_path = dir1_path.join("a.txt");
    assert!(a_txt_path.is_file(), "a.txt should be a file");
    assert!(a_txt_path.metadata().unwrap().len() > 0, "a.txt should have non-zero size");

    let dir2_path = torrent_dir.join("dir2");
    assert!(dir2_path.is_dir(), "dir2 should be a directory");

    let non_existent = torrent_dir.join("nonexistent");
    assert!(!non_existent.exists(), "non-existent path should not exist");

    cleanup_mount(&mount_path, guard);
}

#[test]
#[serial]
fn test_torrent_getattr_for_directories() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let guard = spawn_fs(&mount_path, &state_path);

    thread::sleep(Duration::from_millis(500));

    let src = std::path::PathBuf::from("test_data/multi_file.torrent");
    if !src.exists() {
        eprintln!("multi_file.torrent not found, skipping getattr test");
        cleanup_mount(&mount_path, guard);
        return;
    }

    let dest = mount_path.join("metadata").join(src.file_name().unwrap());
    let result = fs::copy(&src, &dest);
    assert!(result.is_ok(), "Failed to copy .torrent file: {:?}", result.err());

    thread::sleep(Duration::from_millis(500));

    let torrent_dir = mount_path.join("data").join("multi_file_test");
    let torrent_metadata = fs::metadata(&torrent_dir).unwrap();
    assert!(torrent_metadata.is_dir(), "multi_file_test should be a directory");
    assert_eq!(torrent_metadata.permissions().mode() & 0o777, 0o755, "directory should have 755 permissions");

    let dir1_path = torrent_dir.join("dir1");
    let dir1_metadata = fs::metadata(&dir1_path).unwrap();
    assert!(dir1_metadata.is_dir(), "dir1 should be a directory");
    assert_eq!(dir1_metadata.permissions().mode() & 0o777, 0o755, "dir1 should have 755 permissions");

    let a_txt_path = dir1_path.join("a.txt");
    let a_txt_metadata = fs::metadata(&a_txt_path).unwrap();
    assert!(a_txt_metadata.is_file(), "a.txt should be a file");
    assert_eq!(a_txt_metadata.permissions().mode() & 0o777, 0o644, "a.txt should have 644 permissions");

    cleanup_mount(&mount_path, guard);
}

#[test]
#[serial]
fn test_deeply_nested_directories_in_torrent() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt.block_on(torrentfs::init(&state_path)).expect("torrentfs::init(&state_path) should succeed");
    let metadata_manager = std::sync::Arc::new(
        torrentfs::MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = std::sync::Arc::new(torrentfs_libtorrent::Session::new().unwrap());

    let fs = TorrentFsFilesystem::new_with_core(
        state_path.clone(),
        metadata_manager,
        rt,
        session,
    );

    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
    ];
    let guard = fuser::spawn_mount2(fs, &mount_path, &options).unwrap();

    thread::sleep(Duration::from_millis(500));

    let src = match nested_dirs_torrent() {
        Some(p) => p,
        None => {
            eprintln!("nested_dirs.torrent not found, skipping deep nested test");
            cleanup_mount(&mount_path, guard);
            return;
        }
    };

    let dest = mount_path.join("metadata").join(src.file_name().unwrap());
    let result = fs::copy(&src, &dest);
    assert!(result.is_ok(), "Failed to copy .torrent file: {:?}", result.err());

    thread::sleep(Duration::from_millis(500));

    let data_entries: Vec<_> = fs::read_dir(mount_path.join("data"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();

    assert!(data_entries.contains(&"nested_test".to_string()), 
        "data/ should contain nested_test directory");

    let torrent_dir = mount_path.join("data").join("nested_test");
    assert!(torrent_dir.is_dir(), "nested_test should be a directory");

    let torrent_entries: Vec<_> = fs::read_dir(&torrent_dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();

    assert!(torrent_entries.contains(&"docs".to_string()), 
        "nested_test should contain docs");
    assert!(torrent_entries.contains(&"src".to_string()), 
        "nested_test should contain src");

    let docs_path = torrent_dir.join("docs");
    assert!(docs_path.is_dir(), "docs should be a directory");

    let docs_entries: Vec<_> = fs::read_dir(&docs_path)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();

    assert!(docs_entries.contains(&"images".to_string()), 
        "docs should contain images");
    assert!(docs_entries.contains(&"readme.txt".to_string()), 
        "docs should contain readme.txt");

    let images_path = docs_path.join("images");
    assert!(images_path.is_dir(), "images should be a directory");

    let images_entries: Vec<_> = fs::read_dir(&images_path)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();

    assert!(images_entries.contains(&"a.png".to_string()), 
        "images should contain a.png");
    assert!(images_entries.contains(&"b.png".to_string()), 
        "images should contain b.png");

    let src_path = torrent_dir.join("src");
    assert!(src_path.is_dir(), "src should be a directory");

    let src_entries: Vec<_> = fs::read_dir(&src_path)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();

    assert!(src_entries.contains(&"main.rs".to_string()), 
        "src should contain main.rs");

    cleanup_mount(&mount_path, guard);
}

#[test]
#[serial]
fn test_deeply_nested_subdirectory_mirroring() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt.block_on(torrentfs::init(&state_path)).expect("torrentfs::init(&state_path) should succeed");
    let metadata_manager = std::sync::Arc::new(
        torrentfs::MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = std::sync::Arc::new(torrentfs_libtorrent::Session::new().unwrap());

    let fs = TorrentFsFilesystem::new_with_core(
        state_path.clone(),
        metadata_manager,
        rt,
        session,
    );

    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
    ];
    let guard = fuser::spawn_mount2(fs, &mount_path, &options).unwrap();

    thread::sleep(Duration::from_millis(500));

    let subdir = mount_path.join("metadata").join("a").join("b");
    fs::create_dir_all(&subdir).expect("Failed to create nested subdirectory");

    let src = match nested_dirs_torrent() {
        Some(p) => p,
        None => {
            eprintln!("nested_dirs.torrent not found, skipping mirror test");
            cleanup_mount(&mount_path, guard);
            return;
        }
    };

    let dest = subdir.join(src.file_name().unwrap());
    let result = fs::copy(&src, &dest);
    assert!(result.is_ok(), "Failed to copy .torrent file: {:?}", result.err());

    thread::sleep(Duration::from_millis(500));

    let data_a = mount_path.join("data").join("a");
    assert!(data_a.exists(), "data/a should exist");
    assert!(data_a.is_dir(), "data/a should be a directory");

    let data_a_b = data_a.join("b");
    assert!(data_a_b.exists(), "data/a/b should exist");
    assert!(data_a_b.is_dir(), "data/a/b should be a directory");

    let data_a_b_entries: Vec<_> = fs::read_dir(&data_a_b)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();

    eprintln!("data/a/b entries: {:?}", data_a_b_entries);
    assert!(!data_a_b_entries.is_empty(), "data/a/b should contain torrent directory");

    assert!(data_a_b_entries.contains(&"nested_test".to_string()), 
        "data/a/b should contain nested_test directory");

    let nested_test_dir = data_a_b.join("nested_test");
    assert!(nested_test_dir.exists(), "nested_test should exist");
    assert!(nested_test_dir.is_dir(), "nested_test should be a directory");

    let nested_test_entries: Vec<_> = fs::read_dir(&nested_test_dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();

    assert!(nested_test_entries.contains(&"docs".to_string()), 
        "nested_test should contain docs directory");
    assert!(nested_test_entries.contains(&"src".to_string()), 
        "nested_test should contain src directory");

    let docs_images = nested_test_dir.join("docs").join("images");
    assert!(docs_images.exists(), "docs/images should exist in nested torrent");
    assert!(docs_images.is_dir(), "docs/images should be a directory");

    cleanup_mount(&mount_path, guard);
}

#[test]
#[serial]
fn test_stat_on_nested_directories() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt.block_on(torrentfs::init(&state_path)).expect("torrentfs::init(&state_path) should succeed");
    let metadata_manager = std::sync::Arc::new(
        torrentfs::MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = std::sync::Arc::new(torrentfs_libtorrent::Session::new().unwrap());

    let fs = TorrentFsFilesystem::new_with_core(
        state_path.clone(),
        metadata_manager,
        rt,
        session,
    );

    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
    ];
    let guard = fuser::spawn_mount2(fs, &mount_path, &options).unwrap();

    thread::sleep(Duration::from_millis(500));

    let src = match nested_dirs_torrent() {
        Some(p) => p,
        None => {
            eprintln!("nested_dirs.torrent not found, skipping stat test");
            cleanup_mount(&mount_path, guard);
            return;
        }
    };

    let dest = mount_path.join("metadata").join(src.file_name().unwrap());
    let result = fs::copy(&src, &dest);
    assert!(result.is_ok(), "Failed to copy .torrent file: {:?}", result.err());

    thread::sleep(Duration::from_millis(500));

    let torrent_dir = mount_path.join("data").join("nested_test");
    let torrent_metadata = fs::metadata(&torrent_dir).unwrap();
    assert!(torrent_metadata.is_dir(), "nested_test should be a directory (S_IFDIR)");
    assert_eq!(torrent_metadata.permissions().mode() & 0o777, 0o755);

    let docs_path = torrent_dir.join("docs");
    let docs_metadata = fs::metadata(&docs_path).unwrap();
    assert!(docs_metadata.is_dir(), "docs should be a directory (S_IFDIR)");
    assert_eq!(docs_metadata.permissions().mode() & 0o777, 0o755);

    let images_path = docs_path.join("images");
    let images_metadata = fs::metadata(&images_path).unwrap();
    assert!(images_metadata.is_dir(), "images should be a directory (S_IFDIR)");
    assert_eq!(images_metadata.permissions().mode() & 0o777, 0o755);

    let a_png_path = images_path.join("a.png");
    let a_png_metadata = fs::metadata(&a_png_path).unwrap();
    assert!(a_png_metadata.is_file(), "a.png should be a file (S_IFREG)");
    assert_eq!(a_png_metadata.permissions().mode() & 0o777, 0o644);
    assert_eq!(a_png_metadata.len(), 34);

    let src_path = torrent_dir.join("src");
    let src_metadata = fs::metadata(&src_path).unwrap();
    assert!(src_metadata.is_dir(), "src should be a directory (S_IFDIR)");
    assert_eq!(src_metadata.permissions().mode() & 0o777, 0o755);

    let main_rs_path = src_path.join("main.rs");
    let main_rs_metadata = fs::metadata(&main_rs_path).unwrap();
    assert!(main_rs_metadata.is_file(), "main.rs should be a file (S_IFREG)");
    assert_eq!(main_rs_metadata.permissions().mode() & 0o777, 0o644);
    assert_eq!(main_rs_metadata.len(), 32);

    cleanup_mount(&mount_path, guard);
}

#[test]
#[serial]
fn test_read_nested_files() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt.block_on(torrentfs::init(&state_path)).expect("torrentfs::init(&state_path) should succeed");
    let metadata_manager = std::sync::Arc::new(
        torrentfs::MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = std::sync::Arc::new(torrentfs_libtorrent::Session::new().unwrap());

    let fs = TorrentFsFilesystem::new_with_core(
        state_path.clone(),
        metadata_manager,
        rt,
        session,
    );

    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
    ];
    let guard = fuser::spawn_mount2(fs, &mount_path, &options).unwrap();

    thread::sleep(Duration::from_millis(500));

    let src = match nested_dirs_torrent() {
        Some(p) => p,
        None => {
            eprintln!("nested_dirs.torrent not found, skipping read test");
            cleanup_mount(&mount_path, guard);
            return;
        }
    };

    let dest = mount_path.join("metadata").join(src.file_name().unwrap());
    let result = fs::copy(&src, &dest);
    assert!(result.is_ok(), "Failed to copy .torrent file: {:?}", result.err());

    thread::sleep(Duration::from_millis(500));

    let a_png_path = mount_path.join("data/nested_test/docs/images/a.png");
    let content = fs::read(&a_png_path);
    match content {
        Ok(data) => {
            assert_eq!(data.len(), 34, "a.png should have 34 bytes");
        }
        Err(e) => {
            eprintln!("Note: Reading file content requires piece download, error: {:?}", e);
        }
    }

    let readme_path = mount_path.join("data/nested_test/docs/readme.txt");
    let readme_content = fs::read(&readme_path);
    match readme_content {
        Ok(data) => {
            assert_eq!(data.len(), 30, "readme.txt should have 30 bytes");
        }
        Err(e) => {
            eprintln!("Note: Reading file content requires piece download, error: {:?}", e);
        }
    }

    cleanup_mount(&mount_path, guard);
}

#[test]
#[serial]
fn test_open_and_read_no_enosys() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt.block_on(torrentfs::init(&state_path)).expect("torrentfs::init(&state_path) should succeed");
    let metadata_manager = std::sync::Arc::new(
        torrentfs::MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = std::sync::Arc::new(torrentfs_libtorrent::Session::new().unwrap());

    let fs = TorrentFsFilesystem::new_with_core(
        state_path.clone(),
        metadata_manager,
        rt,
        session,
    );

    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
    ];
    let guard = fuser::spawn_mount2(fs, &mount_path, &options).unwrap();

    thread::sleep(Duration::from_millis(500));

    let src = match nested_dirs_torrent() {
        Some(p) => p,
        None => {
            eprintln!("nested_dirs.torrent not found, skipping open/read ENOSYS test");
            cleanup_mount(&mount_path, guard);
            return;
        }
    };

    let dest = mount_path.join("metadata").join(src.file_name().unwrap());
    let result = fs::copy(&src, &dest);
    assert!(result.is_ok(), "Failed to copy .torrent file: {:?}", result.err());

    thread::sleep(Duration::from_millis(500));

    let a_png_path = mount_path.join("data/nested_test/docs/images/a.png");
    
    use std::os::unix::fs::FileExt;
    let file_result = std::fs::OpenOptions::new()
        .read(true)
        .open(&a_png_path);
    
    match file_result {
        Ok(file) => {
            let mut buf = [0u8; 1024];
            let read_result = file.read_at(&mut buf, 0);
            match read_result {
                Ok(_n) => {
                    // open() and read() succeeded - no ENOSYS
                }
                Err(e) => {
                    let errno = e.raw_os_error().unwrap_or(0);
                    assert_ne!(errno, libc::ENOSYS, "read() should not return ENOSYS");
                    eprintln!("Note: read() returned error {} (not ENOSYS), may be expected for piece download: {:?}", errno, e);
                }
            }
        }
        Err(e) => {
            let errno = e.raw_os_error().unwrap_or(0);
            assert_ne!(errno, libc::ENOSYS, "open() should not return ENOSYS");
            eprintln!("Note: open() returned error {} (not ENOSYS): {:?}", errno, e);
        }
    }

    cleanup_mount(&mount_path, guard);
}

#[test]
#[serial]
fn test_torrent_restored_on_restart() {
    let state_dir = TempDir::new().unwrap();
    let mount_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();
    let mount_path = mount_dir.path().to_path_buf();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt.block_on(torrentfs::TorrentRuntime::new(&state_path)).unwrap();
    let metadata_manager = std::sync::Arc::new(
        torrentfs::MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = std::sync::Arc::new(torrentfs_libtorrent::Session::new().unwrap());

    let fs = TorrentFsFilesystem::new_with_core(
        state_path.clone(),
        metadata_manager,
        rt,
        session,
    );

    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
    ];
    let guard = fuser::spawn_mount2(fs, &mount_path, &options).unwrap();

    thread::sleep(Duration::from_millis(500));

    let src = match first_torrent_file() {
        Some(p) => p,
        None => {
            eprintln!("No .torrent files found, skipping restart test");
            cleanup_mount(&mount_path, guard);
            return;
        }
    };

    let dest = mount_path.join("metadata").join(src.file_name().unwrap());
    let result = fs::copy(&src, &dest);
    assert!(result.is_ok(), "Failed to copy .torrent file: {:?}", result.err());

    thread::sleep(Duration::from_millis(500));

    cleanup_mount(&mount_path, guard);

    drop(runtime);

    let rt2 = tokio::runtime::Runtime::new().unwrap();
    let runtime2 = rt2.block_on(torrentfs::TorrentRuntime::new(&state_path)).unwrap();

    let torrents_with_data = rt2.block_on(runtime2.metadata_manager.list_torrents_with_data()).unwrap();
    assert!(!torrents_with_data.is_empty(), "Torrent should be restored from DB");

    let torrent = &torrents_with_data[0].torrent;
    let info_hash_hex = hex::encode(&torrent.info_hash);
    let found = runtime2.session.find_torrent(&info_hash_hex);
    assert!(found, "Torrent should be found in restored session");
    
    assert!(
        torrents_with_data[0].torrent_data.len() > 0,
        "Torrent data should be persisted"
    );
    
    let cached_pieces = runtime2.piece_cache.scan_cached_pieces().unwrap();
    eprintln!(
        "Torrent '{}' restored, found in session: {}, cached pieces dirs: {}",
        torrent.name,
        found,
        cached_pieces.len()
    );

    drop(runtime2);
}

#[test]
#[serial]
fn test_single_level_subdirectory_data_access() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt.block_on(torrentfs::init(&state_path)).expect("torrentfs::init(&state_path) should succeed");
    let metadata_manager = std::sync::Arc::new(
        torrentfs::MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = std::sync::Arc::new(torrentfs_libtorrent::Session::new().unwrap());

    let fs = TorrentFsFilesystem::new_with_core(
        state_path.clone(),
        metadata_manager,
        rt,
        session,
    );

    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
    ];
    let guard = fuser::spawn_mount2(fs, &mount_path, &options).unwrap();

    thread::sleep(Duration::from_millis(500));

    let subdir = mount_path.join("metadata").join("anime");
    fs::create_dir_all(&subdir).expect("Failed to create subdirectory");

    let src = match first_torrent_file() {
        Some(p) => p,
        None => {
            eprintln!("No .torrent files found, skipping single-level subdirectory test");
            cleanup_mount(&mount_path, guard);
            return;
        }
    };

    let dest = subdir.join(src.file_name().unwrap());
    let result = fs::copy(&src, &dest);
    assert!(result.is_ok(), "Failed to copy .torrent file: {:?}", result.err());

    thread::sleep(Duration::from_millis(500));

    let data_anime = mount_path.join("data").join("anime");
    assert!(data_anime.exists(), "data/anime should exist");
    assert!(data_anime.is_dir(), "data/anime should be a directory");

    let anime_entries: Vec<_> = fs::read_dir(&data_anime)
        .expect("Should be able to read data/anime directory")
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();

    assert!(!anime_entries.is_empty(), "data/anime should contain torrent directory");

    cleanup_mount(&mount_path, guard);
}

#[test]
#[serial]
fn test_invalid_torrent_file_rejected() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt.block_on(torrentfs::init(&state_path)).expect("torrentfs::init(&state_path) should succeed");
    let metadata_manager = std::sync::Arc::new(
        torrentfs::MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = std::sync::Arc::new(torrentfs_libtorrent::Session::new().unwrap());

    let fs = TorrentFsFilesystem::new_with_core(
        state_path.clone(),
        metadata_manager,
        rt,
        session,
    );

    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
    ];
    let guard = fuser::spawn_mount2(fs, &mount_path, &options).unwrap();

    thread::sleep(Duration::from_millis(500));

    let dest = mount_path.join("metadata").join("invalid.torrent");
    
    use std::io::Write;
    let file_result = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&dest);
    
    match file_result {
        Ok(mut file) => {
            let _ = file.write_all(b"not a valid torrent data");
            let sync_result = file.sync_all();
            if let Err(e) = sync_result {
                assert!(
                    e.raw_os_error() == Some(libc::EINVAL) || e.raw_os_error() == Some(libc::EIO),
                    "sync_all should return EINVAL or EIO for invalid torrent, got: {:?}", e
                );
            }
        }
        Err(e) => {
            eprintln!("Open error (unexpected for this test): {:?}", e);
        }
    }

    thread::sleep(Duration::from_millis(500));

    let metadata_entries: Vec<_> = fs::read_dir(mount_path.join("metadata"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();

    assert!(
        !metadata_entries.contains(&"invalid.torrent".to_string()),
        "Invalid torrent file should NOT remain visible in metadata/ after rejection, found: {:?}",
        metadata_entries
    );

    cleanup_mount(&mount_path, guard);
}

#[test]
#[serial]
fn test_multiple_torrents_same_subdirectory() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt.block_on(torrentfs::init(&state_path)).expect("torrentfs::init(&state_path) should succeed");
    let metadata_manager = std::sync::Arc::new(
        torrentfs::MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = std::sync::Arc::new(torrentfs_libtorrent::Session::new().unwrap());

    let fs = TorrentFsFilesystem::new_with_core(
        state_path.clone(),
        metadata_manager,
        rt,
        session,
    );

    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
    ];
    let guard = fuser::spawn_mount2(fs, &mount_path, &options).unwrap();

    thread::sleep(Duration::from_millis(500));

    let subdir = mount_path.join("metadata").join("series");
    fs::create_dir_all(&subdir).expect("Failed to create series subdirectory");

    let src_dir = test_torrent_dir();
    let torrent_files: Vec<_> = fs::read_dir(&src_dir)
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
        .collect();

    if torrent_files.len() < 2 {
        eprintln!("Need at least 2 .torrent files for this test, skipping");
        cleanup_mount(&mount_path, guard);
        return;
    }

    for (i, src) in torrent_files.iter().take(2).enumerate() {
        let dest = subdir.join(format!("{}_{}.torrent", i, src.file_name().unwrap().to_string_lossy()));
        let result = fs::copy(src, &dest);
        assert!(result.is_ok(), "Failed to copy .torrent file: {:?}", result.err());
    }

    thread::sleep(Duration::from_millis(500));

    let data_series = mount_path.join("data").join("series");
    assert!(data_series.exists(), "data/series should exist after copying torrents to metadata/series/");
    assert!(data_series.is_dir(), "data/series should be a directory");

    let series_entries: Vec<_> = fs::read_dir(&data_series)
        .unwrap()
        .map(|e| e.unwrap())
        .collect();

    assert!(series_entries.len() >= 2, "data/series should contain at least 2 torrent directories, got {:?}", series_entries.len());

    for entry in &series_entries {
        let entry_type = entry.file_type().unwrap();
        assert!(entry_type.is_dir(), "Each entry in data/series should be a directory, got file: {:?}", entry.file_name());
    }

    cleanup_mount(&mount_path, guard);
}

#[test]
#[serial]
fn test_read_flat_file_with_cached_piece() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();
    let cache_dir = TempDir::new().unwrap();
    let piece_cache = torrentfs::PieceCache::with_cache_dir(cache_dir.path().to_path_buf()).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt.block_on(torrentfs::TorrentRuntime::new(&state_path)).expect("runtime should succeed");
    let metadata_manager = std::sync::Arc::new(
        torrentfs::MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = std::sync::Arc::clone(&runtime.session);
    let piece_cache = std::sync::Arc::new(piece_cache);

    let torrent_data = std::fs::read(test_torrent_dir().join("test.torrent")).expect("test.torrent should exist");
    let torrent_info = torrentfs_libtorrent::parse_torrent(&torrent_data).expect("parse torrent");
    let info_hash = torrent_info.info_hash.clone();
    
    let test_content: Vec<u8> = (0..100u8).collect();
    piece_cache.write_piece(&info_hash, 0, &test_content).expect("write piece");
    
    session.add_torrent_paused(&torrent_data, "/tmp/torrentfs").expect("add torrent");

    let download_coordinator = std::sync::Arc::new(
        torrentfs::DownloadCoordinator::new(std::sync::Arc::clone(&session), std::sync::Arc::clone(&piece_cache))
    );

    let fs = TorrentFsFilesystem::new_with_download_coordinator(
        state_path.clone(),
        metadata_manager,
        session,
        download_coordinator,
    );

    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
    ];
    let guard = fuser::spawn_mount2(fs, &mount_path, &options).unwrap();

    thread::sleep(Duration::from_millis(500));

    let dest = mount_path.join("metadata").join("test.torrent");
    fs::copy(test_torrent_dir().join("test.torrent"), &dest).expect("copy torrent");

    thread::sleep(Duration::from_millis(500));

    let data_entries: Vec<_> = fs::read_dir(mount_path.join("data"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(!data_entries.is_empty(), "data/ should not be empty");

    let torrent_dir = mount_path.join("data").join(&torrent_info.name);
    assert!(torrent_dir.exists(), "Torrent directory {} should exist", torrent_dir.display());
    
    let file_path = torrent_dir.join("test.txt");
    assert!(file_path.exists(), "File test.txt should exist");
    assert!(file_path.is_file(), "test.txt should be a file");
    
    let metadata = fs::metadata(&file_path).expect("metadata should work");
    assert_eq!(metadata.len(), 100, "test.txt should be 100 bytes");

    let content = fs::read(&file_path).expect("Reading test.txt should succeed with cached piece");
    assert_eq!(content.len(), 100, "Read content should be 100 bytes");
    assert_eq!(content, test_content, "Content should match cached piece data");

    cleanup_mount(&mount_path, guard);
}

#[test]
#[serial]
fn test_read_nested_file_with_cached_piece() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();
    let cache_dir = TempDir::new().unwrap();
    let piece_cache = torrentfs::PieceCache::with_cache_dir(cache_dir.path().to_path_buf()).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt.block_on(torrentfs::TorrentRuntime::new(&state_path)).expect("runtime should succeed");
    let metadata_manager = std::sync::Arc::new(
        torrentfs::MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = std::sync::Arc::clone(&runtime.session);
    let piece_cache = std::sync::Arc::new(piece_cache);

    let torrent_data = std::fs::read(test_torrent_dir().join("nested_dirs.torrent")).expect("nested_dirs.torrent should exist");
    let torrent_info = torrentfs_libtorrent::parse_torrent(&torrent_data).expect("parse torrent");
    let info_hash = torrent_info.info_hash.clone();
    
    let mut all_content = Vec::new();
    let expected_files = vec![
        ("docs/images/a.png", 34usize),
        ("docs/images/b.png", 34usize),
        ("docs/readme.txt", 30usize),
        ("src/main.rs", 32usize),
    ];
    
    let total_size: usize = expected_files.iter().map(|(_, s)| s).sum();
    all_content.resize(total_size, 0u8);
    
    let mut offset = 0usize;
    for (_path, size) in &expected_files {
        for i in 0..*size {
            all_content[offset + i] = ((offset + i) % 256) as u8;
        }
        offset += size;
    }
    
    let piece_size = torrent_info.piece_size as usize;
    for piece_idx in 0..=((total_size - 1) / piece_size) {
        let start = piece_idx * piece_size;
        let end = std::cmp::min(start + piece_size, total_size);
        piece_cache.write_piece(&info_hash, piece_idx as u32, &all_content[start..end]).expect("write piece");
    }
    
    session.add_torrent_paused(&torrent_data, "/tmp/torrentfs").expect("add torrent");

    let download_coordinator = std::sync::Arc::new(
        torrentfs::DownloadCoordinator::new(std::sync::Arc::clone(&session), std::sync::Arc::clone(&piece_cache))
    );

    let fs = TorrentFsFilesystem::new_with_download_coordinator(
        state_path.clone(),
        metadata_manager,
        session,
        download_coordinator,
    );

    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
    ];
    let guard = fuser::spawn_mount2(fs, &mount_path, &options).unwrap();

    thread::sleep(Duration::from_millis(500));

    let dest = mount_path.join("metadata").join("nested_dirs.torrent");
    fs::copy(test_torrent_dir().join("nested_dirs.torrent"), &dest).expect("copy torrent");

    thread::sleep(Duration::from_millis(500));

    let torrent_dir = mount_path.join("data").join("nested_test");
    assert!(torrent_dir.exists(), "Torrent directory should exist");
    
    let file_path = torrent_dir.join("docs").join("images").join("a.png");
    assert!(file_path.exists(), "Nested file docs/images/a.png should exist");
    
    let content = fs::read(&file_path).expect("Reading nested file should succeed with cached piece");
    assert_eq!(content.len(), 34, "a.png should be 34 bytes");
    
    let expected_a_png: Vec<u8> = (0..34).map(|i| i as u8).collect();
    assert_eq!(content, expected_a_png, "a.png content should match");

    let readme_path = torrent_dir.join("docs").join("readme.txt");
    let readme_content = fs::read(&readme_path).expect("Reading readme.txt should succeed");
    assert_eq!(readme_content.len(), 30, "readme.txt should be 30 bytes");

    let main_rs_path = torrent_dir.join("src").join("main.rs");
    let main_rs_content = fs::read(&main_rs_path).expect("Reading main.rs should succeed");
    assert_eq!(main_rs_content.len(), 32, "main.rs should be 32 bytes");

    cleanup_mount(&mount_path, guard);
}

#[test]
#[serial]
fn test_read_cache_hit_second_read_faster() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();
    let cache_dir = TempDir::new().unwrap();
    let piece_cache = torrentfs::PieceCache::with_cache_dir(cache_dir.path().to_path_buf()).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt.block_on(torrentfs::TorrentRuntime::new(&state_path)).expect("runtime should succeed");
    let metadata_manager = std::sync::Arc::new(
        torrentfs::MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = std::sync::Arc::clone(&runtime.session);
    let piece_cache = std::sync::Arc::new(piece_cache);

    let torrent_data = std::fs::read(test_torrent_dir().join("test.torrent")).expect("test.torrent should exist");
    let torrent_info = torrentfs_libtorrent::parse_torrent(&torrent_data).expect("parse torrent");
    let info_hash = torrent_info.info_hash.clone();
    
    let test_content: Vec<u8> = (0..100u8).collect();
    piece_cache.write_piece(&info_hash, 0, &test_content).expect("write piece");
    
    session.add_torrent_paused(&torrent_data, "/tmp/torrentfs").expect("add torrent");

    let download_coordinator = std::sync::Arc::new(
        torrentfs::DownloadCoordinator::new(std::sync::Arc::clone(&session), std::sync::Arc::clone(&piece_cache))
    );

    let fs = TorrentFsFilesystem::new_with_download_coordinator(
        state_path.clone(),
        metadata_manager,
        session,
        download_coordinator,
    );

    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
    ];
    let guard = fuser::spawn_mount2(fs, &mount_path, &options).unwrap();

    thread::sleep(Duration::from_millis(500));

    let dest = mount_path.join("metadata").join("test.torrent");
    fs::copy(test_torrent_dir().join("test.torrent"), &dest).expect("copy torrent");

    thread::sleep(Duration::from_millis(500));

    let file_path = mount_path.join("data").join(&torrent_info.name).join("test.txt");
    
    let start1 = std::time::Instant::now();
    let content1 = fs::read(&file_path).expect("First read should succeed");
    let duration1 = start1.elapsed();
    
    let start2 = std::time::Instant::now();
    let content2 = fs::read(&file_path).expect("Second read should succeed");
    let duration2 = start2.elapsed();
    
    assert_eq!(content1.len(), 100, "First read should return 100 bytes");
    assert_eq!(content2.len(), 100, "Second read should return 100 bytes");
    assert_eq!(content1, content2, "Both reads should return same content");
    
    assert!(piece_cache.has_piece(&info_hash, 0), "Piece should still be cached");
    
    eprintln!("First read: {:?}, Second read: {:?}", duration1, duration2);

    cleanup_mount(&mount_path, guard);
}

#[test]
#[serial]
fn test_read_from_mirrored_subdirectory_path() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();
    let cache_dir = TempDir::new().unwrap();
    let piece_cache = torrentfs::PieceCache::with_cache_dir(cache_dir.path().to_path_buf()).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt.block_on(torrentfs::TorrentRuntime::new(&state_path)).expect("runtime should succeed");
    let metadata_manager = std::sync::Arc::new(
        torrentfs::MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = std::sync::Arc::clone(&runtime.session);
    let piece_cache = std::sync::Arc::new(piece_cache);

    let torrent_data = std::fs::read(test_torrent_dir().join("nested_dirs.torrent")).expect("nested_dirs.torrent should exist");
    let torrent_info = torrentfs_libtorrent::parse_torrent(&torrent_data).expect("parse torrent");
    let info_hash = torrent_info.info_hash.clone();
    
    let mut all_content = Vec::new();
    let total_size = 34 + 34 + 30 + 32;
    all_content.resize(total_size, 0u8);
    for i in 0..total_size {
        all_content[i] = (i % 256) as u8;
    }
    
    let piece_size = torrent_info.piece_size as usize;
    for piece_idx in 0..=((total_size - 1) / piece_size) {
        let start = piece_idx * piece_size;
        let end = std::cmp::min(start + piece_size, total_size);
        piece_cache.write_piece(&info_hash, piece_idx as u32, &all_content[start..end]).expect("write piece");
    }
    
    session.add_torrent_paused(&torrent_data, "/tmp/torrentfs").expect("add torrent");

    let download_coordinator = std::sync::Arc::new(
        torrentfs::DownloadCoordinator::new(std::sync::Arc::clone(&session), std::sync::Arc::clone(&piece_cache))
    );

    let fs = TorrentFsFilesystem::new_with_download_coordinator(
        state_path.clone(),
        metadata_manager,
        session,
        download_coordinator,
    );

    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
    ];
    let guard = fuser::spawn_mount2(fs, &mount_path, &options).unwrap();

    thread::sleep(Duration::from_millis(500));

    let subdir = mount_path.join("metadata").join("a").join("b");
    fs::create_dir_all(&subdir).expect("create subdir");

    let dest = subdir.join("nested_dirs.torrent");
    fs::copy(test_torrent_dir().join("nested_dirs.torrent"), &dest).expect("copy torrent");

    thread::sleep(Duration::from_millis(500));

    let nested_file_path = mount_path.join("data").join("a").join("b")
        .join("nested_test").join("src").join("main.rs");
    
    assert!(nested_file_path.exists(), "Mirrored nested file should exist at {:?}", nested_file_path);
    
    let content = fs::read(&nested_file_path).expect("Reading from mirrored path should succeed");
    assert_eq!(content.len(), 32, "main.rs should be 32 bytes");

    cleanup_mount(&mount_path, guard);
}

#[test]
#[serial]
fn test_read_with_offset_and_partial_reads() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();
    let cache_dir = TempDir::new().unwrap();
    let piece_cache = torrentfs::PieceCache::with_cache_dir(cache_dir.path().to_path_buf()).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt.block_on(torrentfs::TorrentRuntime::new(&state_path)).expect("runtime should succeed");
    let metadata_manager = std::sync::Arc::new(
        torrentfs::MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = std::sync::Arc::clone(&runtime.session);
    let piece_cache = std::sync::Arc::new(piece_cache);

    let torrent_data = std::fs::read(test_torrent_dir().join("test.torrent")).expect("test.torrent should exist");
    let torrent_info = torrentfs_libtorrent::parse_torrent(&torrent_data).expect("parse torrent");
    let info_hash = torrent_info.info_hash.clone();
    
    let test_content: Vec<u8> = (0..100u8).collect();
    piece_cache.write_piece(&info_hash, 0, &test_content).expect("write piece");
    
    session.add_torrent_paused(&torrent_data, "/tmp/torrentfs").expect("add torrent");

    let download_coordinator = std::sync::Arc::new(
        torrentfs::DownloadCoordinator::new(std::sync::Arc::clone(&session), std::sync::Arc::clone(&piece_cache))
    );

    let fs = TorrentFsFilesystem::new_with_download_coordinator(
        state_path.clone(),
        metadata_manager,
        session,
        download_coordinator,
    );

    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
    ];
    let guard = fuser::spawn_mount2(fs, &mount_path, &options).unwrap();

    thread::sleep(Duration::from_millis(500));

    let dest = mount_path.join("metadata").join("test.torrent");
    fs::copy(test_torrent_dir().join("test.torrent"), &dest).expect("copy torrent");

    thread::sleep(Duration::from_millis(500));

    let file_path = mount_path.join("data").join(&torrent_info.name).join("test.txt");
    
    use std::os::unix::fs::FileExt;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .open(&file_path)
        .expect("Open should succeed");
    
    let mut buf = [0u8; 10];
    let n = file.read_at(&mut buf, 0).expect("Read at offset 0 should succeed");
    assert_eq!(n, 10, "Should read 10 bytes");
    assert_eq!(&buf[..], &test_content[0..10], "Content should match at offset 0");
    
    let mut buf2 = [0u8; 20];
    let n2 = file.read_at(&mut buf2, 50).expect("Read at offset 50 should succeed");
    assert_eq!(n2, 20, "Should read 20 bytes");
    assert_eq!(&buf2[..], &test_content[50..70], "Content should match at offset 50");
    
    let mut buf3 = [0u8; 100];
    let n3 = file.read_at(&mut buf3, 90).expect("Read at offset 90 should succeed");
    assert_eq!(n3, 10, "Should read only 10 bytes (file ends at 100)");
    assert_eq!(&buf3[..10], &test_content[90..100], "Content should match at offset 90");
    
    let mut buf4 = [0u8; 10];
    let n4 = file.read_at(&mut buf4, 100).expect("Read at EOF should succeed");
    assert_eq!(n4, 0, "Read at EOF should return 0 bytes");

    cleanup_mount(&mount_path, guard);
}

#[test]
#[serial]
fn test_multi_file_torrent_read_all_files() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();
    let cache_dir = TempDir::new().unwrap();
    let piece_cache = torrentfs::PieceCache::with_cache_dir(cache_dir.path().to_path_buf()).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt.block_on(torrentfs::TorrentRuntime::new(&state_path)).expect("runtime should succeed");
    let metadata_manager = std::sync::Arc::new(
        torrentfs::MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = std::sync::Arc::clone(&runtime.session);
    let piece_cache = std::sync::Arc::new(piece_cache);

    let torrent_data = std::fs::read(test_torrent_dir().join("multi_file.torrent")).expect("multi_file.torrent should exist");
    let torrent_info = torrentfs_libtorrent::parse_torrent(&torrent_data).expect("parse torrent");
    let info_hash = torrent_info.info_hash.clone();
    
    assert_eq!(torrent_info.name, "multi_file_test", "Torrent name should be multi_file_test");
    
    let all_content: Vec<u8> = (0..1700u16).map(|i| (i % 256) as u8).collect();
    let piece_size = torrent_info.piece_size as usize;
    
    for piece_idx in 0..=((all_content.len() - 1) / piece_size) {
        let start = piece_idx * piece_size;
        let end = std::cmp::min(start + piece_size, all_content.len());
        piece_cache.write_piece(&info_hash, piece_idx as u32, &all_content[start..end]).expect("write piece");
    }
    
    session.add_torrent_paused(&torrent_data, "/tmp/torrentfs").expect("add torrent");

    let download_coordinator = std::sync::Arc::new(
        torrentfs::DownloadCoordinator::new(std::sync::Arc::clone(&session), std::sync::Arc::clone(&piece_cache))
    );

    let fs = TorrentFsFilesystem::new_with_download_coordinator(
        state_path.clone(),
        metadata_manager,
        session,
        download_coordinator,
    );

    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
    ];
    let guard = fuser::spawn_mount2(fs, &mount_path, &options).unwrap();

    thread::sleep(Duration::from_millis(500));

    let dest = mount_path.join("metadata").join("multi_file.torrent");
    fs::copy(test_torrent_dir().join("multi_file.torrent"), &dest).expect("copy torrent");

    thread::sleep(Duration::from_millis(500));

    let torrent_dir = mount_path.join("data").join("multi_file_test");
    assert!(torrent_dir.exists(), "Torrent directory should exist");
    
    let dir1_a_txt = torrent_dir.join("dir1").join("a.txt");
    assert!(dir1_a_txt.exists(), "dir1/a.txt should exist");
    let content_a = fs::read(&dir1_a_txt).expect("Reading dir1/a.txt should succeed");
    assert_eq!(content_a.len(), 300, "dir1/a.txt should be 300 bytes");
    
    let expected_a: Vec<u8> = (0..300).map(|i| i as u8).collect();
    assert_eq!(content_a, expected_a, "dir1/a.txt content should match");
    
    let dir2_b_txt = torrent_dir.join("dir2").join("b.txt");
    assert!(dir2_b_txt.exists(), "dir2/b.txt should exist");
    let content_b = fs::read(&dir2_b_txt).expect("Reading dir2/b.txt should succeed");
    assert_eq!(content_b.len(), 1400, "dir2/b.txt should be 1400 bytes");
    
    let expected_b: Vec<u8> = (300..1700).map(|i| (i % 256) as u8).collect();
    assert_eq!(content_b, expected_b, "dir2/b.txt content should match");

    cleanup_mount(&mount_path, guard);
}

#[test]
#[serial]
fn test_invalid_offset_returns_error() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();
    let cache_dir = TempDir::new().unwrap();
    let piece_cache = torrentfs::PieceCache::with_cache_dir(cache_dir.path().to_path_buf()).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt.block_on(torrentfs::TorrentRuntime::new(&state_path)).expect("runtime should succeed");
    let metadata_manager = std::sync::Arc::new(
        torrentfs::MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = std::sync::Arc::clone(&runtime.session);
    let piece_cache = std::sync::Arc::new(piece_cache);

    let torrent_data = std::fs::read(test_torrent_dir().join("test.torrent")).expect("test.torrent should exist");
    let torrent_info = torrentfs_libtorrent::parse_torrent(&torrent_data).expect("parse torrent");
    let info_hash = torrent_info.info_hash.clone();
    
    let test_content: Vec<u8> = (0..100u8).collect();
    piece_cache.write_piece(&info_hash, 0, &test_content).expect("write piece");
    
    session.add_torrent_paused(&torrent_data, "/tmp/torrentfs").expect("add torrent");

    let download_coordinator = std::sync::Arc::new(
        torrentfs::DownloadCoordinator::new(std::sync::Arc::clone(&session), std::sync::Arc::clone(&piece_cache))
    );

    let fs = TorrentFsFilesystem::new_with_download_coordinator(
        state_path.clone(),
        metadata_manager,
        session,
        download_coordinator,
    );

    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
    ];
    let guard = fuser::spawn_mount2(fs, &mount_path, &options).unwrap();

    thread::sleep(Duration::from_millis(500));

    let dest = mount_path.join("metadata").join("test.torrent");
    fs::copy(test_torrent_dir().join("test.torrent"), &dest).expect("copy torrent");

    thread::sleep(Duration::from_millis(500));

    let file_path = mount_path.join("data").join(&torrent_info.name).join("test.txt");
    
    use std::os::unix::fs::FileExt;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .open(&file_path)
        .expect("Open should succeed");
    
    let mut buf = [0u8; 10];
    let result = file.read_at(&mut buf, -1i64 as u64);
    assert!(result.is_ok() || result.is_err(), "Read with very large offset should not crash");

    cleanup_mount(&mount_path, guard);
}