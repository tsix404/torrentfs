use anyhow::Result;
use crate::TorrentFsFilesystem;
use fuser::MountOption;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::time::Duration;
use tokio::runtime::Runtime;
use torrentfs::TorrentRuntime;

static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);
static RECEIVED_SIGNAL: AtomicI32 = AtomicI32::new(0);

extern "C" fn signal_handler(sig: libc::c_int) {
    RECEIVED_SIGNAL.store(sig, Ordering::SeqCst);
    SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
}

static mut OLD_SIGINT_HANDLER: Option<libc::sigaction> = None;
static mut OLD_SIGTERM_HANDLER: Option<libc::sigaction> = None;

fn setup_signal_handlers() {
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = signal_handler as *const () as usize;
        sa.sa_flags = 0;
        libc::sigemptyset(&mut sa.sa_mask);
        
        let mut old_sa: libc::sigaction = std::mem::zeroed();
        libc::sigaction(libc::SIGINT, &sa, &mut old_sa);
        OLD_SIGINT_HANDLER = Some(old_sa);
        
        let mut old_sa: libc::sigaction = std::mem::zeroed();
        libc::sigaction(libc::SIGTERM, &sa, &mut old_sa);
        OLD_SIGTERM_HANDLER = Some(old_sa);
    }
}

pub fn init_and_mount(mount_point: &str, state_dir: &Path) -> Result<()> {
    setup_signal_handlers();
    
    let rt = Arc::new(Runtime::new()?);

    let runtime = Arc::new(rt.block_on(TorrentRuntime::new(state_dir))?);

    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
        MountOption::AllowOther,
    ];

    let fs = TorrentFsFilesystem::new_with_async(
        state_dir.to_path_buf(),
        runtime.metadata_manager.clone(),
        Arc::clone(&runtime.session),
    );
    
    let bg_session = fuser::spawn_mount2(fs, mount_point, &options)?;
    
    tracing::info!("torrentfs mounted at {}, waiting for shutdown signal", mount_point);
    
    while !SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
        std::thread::park_timeout(Duration::from_millis(100));
    }
    
    let sig = RECEIVED_SIGNAL.load(Ordering::SeqCst);
    tracing::info!("Received signal {} ({}), initiating graceful shutdown...", 
        sig, 
        if sig == libc::SIGINT { "SIGINT" } else if sig == libc::SIGTERM { "SIGTERM" } else { "unknown" }
    );
    
    let shutdown_timeout = Duration::from_secs(30);
    let start = std::time::Instant::now();
    
    let result = rt.block_on(async {
        tokio::time::timeout(shutdown_timeout, runtime.graceful_shutdown()).await
    });
    
    match result {
        Ok(Ok(())) => tracing::info!("Graceful shutdown completed in {:?}", start.elapsed()),
        Ok(Err(e)) => tracing::error!("Graceful shutdown error: {}", e),
        Err(_) => {
            tracing::warn!("Graceful shutdown timed out after {:?}, forcing exit", shutdown_timeout);
        }
    }
    
    tracing::info!("Unmounting FUSE filesystem...");
    drop(bg_session);
    
    unsafe { restore_signal_handlers(); }
    
    tracing::info!("torrentfs exited cleanly");
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

pub fn request_shutdown() {
    SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
}

pub fn is_shutdown_requested() -> bool {
    SHUTDOWN_REQUESTED.load(Ordering::SeqCst)
}

pub fn get_received_signal() -> i32 {
    RECEIVED_SIGNAL.load(Ordering::SeqCst)
}

pub fn reset_shutdown_flag() {
    SHUTDOWN_REQUESTED.store(false, Ordering::SeqCst);
    RECEIVED_SIGNAL.store(0, Ordering::SeqCst);
}

pub unsafe fn restore_signal_handlers() {
    if let Some(old_sa) = OLD_SIGINT_HANDLER {
        libc::sigaction(libc::SIGINT, &old_sa, std::ptr::null_mut());
    }
    if let Some(old_sa) = OLD_SIGTERM_HANDLER {
        libc::sigaction(libc::SIGTERM, &old_sa, std::ptr::null_mut());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_shutdown_flag() {
        reset_shutdown_flag();
        assert!(!is_shutdown_requested());
        
        request_shutdown();
        assert!(is_shutdown_requested());
        
        reset_shutdown_flag();
        assert!(!is_shutdown_requested());
    }
    
    #[test]
    fn test_signal_handler_sets_flag() {
        reset_shutdown_flag();
        
        signal_handler(libc::SIGINT);
        assert!(is_shutdown_requested());
        
        reset_shutdown_flag();
        signal_handler(libc::SIGTERM);
        assert!(is_shutdown_requested());
        
        reset_shutdown_flag();
    }
}
