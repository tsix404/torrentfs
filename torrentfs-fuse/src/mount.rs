use anyhow::Result;
use crate::TorrentFsFilesystem;
use fuser::MountOption;
use std::path::Path;
use std::sync::Arc;
use tokio::runtime::Runtime;
use torrentfs::TorrentRuntime;

pub fn init_and_mount(mount_point: &str, state_dir: &Path) -> Result<()> {
    let rt = Runtime::new()?;

    let runtime = rt.block_on(TorrentRuntime::new())?;

    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
        MountOption::AllowOther,
    ];

    let fs = TorrentFsFilesystem::new_with_core(
        state_dir.to_path_buf(),
        runtime.metadata_manager.clone(),
        rt,
        Arc::clone(&runtime.session),
    );
    fuser::mount2(fs, mount_point, &options)?;
    Ok(())
}

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
