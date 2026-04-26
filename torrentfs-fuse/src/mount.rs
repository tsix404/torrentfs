use anyhow::Result;
use crate::TorrentFsFilesystem;
use fuser::MountOption;
use std::path::Path;

pub fn mount(mount_point: &str, state_dir: &Path) -> Result<()> {
    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
        MountOption::AllowOther,
    ];

    let fs = TorrentFsFilesystem::new(state_dir.to_path_buf());
    fuser::mount2(fs, mount_point, &options)?;
    Ok(())
}

pub fn spawn_mount(mount_point: &str, state_dir: &Path) -> Result<fuser::BackgroundSession> {
    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
    ];

    let fs = TorrentFsFilesystem::new(state_dir.to_path_buf());
    let session = fuser::spawn_mount2(fs, mount_point, &options)?;
    Ok(session)
}
