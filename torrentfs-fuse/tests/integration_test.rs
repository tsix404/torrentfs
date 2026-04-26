use std::fs;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use torrentfs_fuse::TorrentFsFilesystem;
use fuser::MountOption;

#[test]
fn test_ls_root_shows_metadata_and_data() {
    let mount_dir = TempDir::new().unwrap();
    let mount_path = mount_dir.path().to_owned();

    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
    ];

    let fs = TorrentFsFilesystem;
    let _guard = fuser::spawn_mount2(fs, &mount_path, &options).unwrap();

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
}
