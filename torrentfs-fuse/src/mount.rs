use anyhow::Result;
use crate::TorrentFsFilesystem;
use fuser::MountOption;

pub fn mount(mount_point: &str) -> Result<()> {
    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
        MountOption::AllowOther,
    ];

    let fs = TorrentFsFilesystem;
    fuser::mount2(fs, mount_point, &options)?;
    Ok(())
}

pub fn spawn_mount(mount_point: &str) -> Result<fuser::BackgroundSession> {
    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
    ];

    let fs = TorrentFsFilesystem;
    let session = fuser::spawn_mount2(fs, mount_point, &options)?;
    Ok(session)
}
