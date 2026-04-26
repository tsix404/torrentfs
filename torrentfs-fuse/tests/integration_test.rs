use std::fs;
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

    let metadata_path = mount_path.join("metadata");
    let data_path = mount_path.join("data");
    assert!(metadata_path.is_dir(), "metadata should be a directory");
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

    let src_dir = PathBuf::from("/workspace/torrentfs");
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
        .take(1)
        .collect();

    if torrent_files.is_empty() {
        eprintln!("No .torrent files found in /workspace/torrentfs, skipping copy test");
        cleanup_mount(&mount_path, guard);
        return;
    }

    let src = &torrent_files[0];
    let dest = mount_path.join("metadata").join(src.file_name().unwrap());

    let result = fs::copy(src, &dest);
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

    let src_dir = PathBuf::from("/workspace/torrentfs");
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
        .take(1)
        .collect();

    if torrent_files.is_empty() {
        eprintln!("No .torrent files found in /workspace/torrentfs, skipping test");
        cleanup_mount(&mount_path, guard);
        return;
    }

    let src = &torrent_files[0];
    let dest = mount_path.join("metadata").join(src.file_name().unwrap());

    let result = fs::copy(src, &dest);
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
