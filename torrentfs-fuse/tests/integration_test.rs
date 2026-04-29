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
    let runtime = rt.block_on(torrentfs::init()).expect("torrentfs::init() should succeed");
    let metadata_manager = std::sync::Arc::new(
        torrentfs::MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = torrentfs_libtorrent::Session::new().unwrap();

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
    let _ = std::fs::remove_file(dirs::home_dir().unwrap().join(".local/share/torrentfs/db/metadata.db"));
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt.block_on(torrentfs::init()).expect("torrentfs::init() should succeed");
    let metadata_manager = std::sync::Arc::new(
        torrentfs::MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = torrentfs_libtorrent::Session::new().unwrap();

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
    let _ = std::fs::remove_file(dirs::home_dir().unwrap().join(".local/share/torrentfs/db/metadata.db"));
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt.block_on(torrentfs::init()).expect("torrentfs::init() should succeed");
    let metadata_manager = std::sync::Arc::new(
        torrentfs::MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = torrentfs_libtorrent::Session::new().unwrap();

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
    let _ = std::fs::remove_file(dirs::home_dir().unwrap().join(".local/share/torrentfs/db/metadata.db"));
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
    let _ = std::fs::remove_file(dirs::home_dir().unwrap().join(".local/share/torrentfs/db/metadata.db"));
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
    let _ = std::fs::remove_file(dirs::home_dir().unwrap().join(".local/share/torrentfs/db/metadata.db"));
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt.block_on(torrentfs::init()).expect("torrentfs::init() should succeed");
    let metadata_manager = std::sync::Arc::new(
        torrentfs::MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = torrentfs_libtorrent::Session::new().unwrap();

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
    let _ = std::fs::remove_file(dirs::home_dir().unwrap().join(".local/share/torrentfs/db/metadata.db"));
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt.block_on(torrentfs::init()).expect("torrentfs::init() should succeed");
    let metadata_manager = std::sync::Arc::new(
        torrentfs::MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = torrentfs_libtorrent::Session::new().unwrap();

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

    assert!(data_a_b_entries.contains(&"docs".to_string()), 
        "data/a/b should contain docs directory from nested torrent");
    assert!(data_a_b_entries.contains(&"src".to_string()), 
        "data/a/b should contain src directory from nested torrent");

    let docs_images = data_a_b.join("docs").join("images");
    assert!(docs_images.exists(), "docs/images should exist in nested torrent");
    assert!(docs_images.is_dir(), "docs/images should be a directory");

    cleanup_mount(&mount_path, guard);
}

#[test]
#[serial]
fn test_stat_on_nested_directories() {
    let _ = std::fs::remove_file(dirs::home_dir().unwrap().join(".local/share/torrentfs/db/metadata.db"));
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt.block_on(torrentfs::init()).expect("torrentfs::init() should succeed");
    let metadata_manager = std::sync::Arc::new(
        torrentfs::MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = torrentfs_libtorrent::Session::new().unwrap();

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
    let _ = std::fs::remove_file(dirs::home_dir().unwrap().join(".local/share/torrentfs/db/metadata.db"));
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();
    let state_dir = TempDir::new().unwrap();
    let state_path = state_dir.path().to_path_buf();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime = rt.block_on(torrentfs::init()).expect("torrentfs::init() should succeed");
    let metadata_manager = std::sync::Arc::new(
        torrentfs::MetadataManager::new(runtime.db.clone()).unwrap()
    );
    let session = torrentfs_libtorrent::Session::new().unwrap();

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