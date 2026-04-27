use crate::alert::AlertList;

use anyhow::{Result, bail};
use std::ffi::CStr;
use std::sync::Mutex;
use std::time::Duration;

#[derive(Debug)]
pub struct Session {
    inner: Mutex<*mut libtorrent_sys::libtorrent_session_t>,
}

unsafe impl Send for Session {}
unsafe impl Sync for Session {}

impl Session {
    pub fn new() -> Result<Self> {
        let inner = unsafe { libtorrent_sys::libtorrent_create_session() };
        if inner.is_null() {
            bail!("Failed to create libtorrent session");
        }
        Ok(Self { inner: Mutex::new(inner) })
    }

    pub fn add_torrent_paused(&self, data: &[u8], save_path: &str) -> Result<()> {
        let inner = *self.inner.lock().unwrap();
        let params = libtorrent_sys::libtorrent_add_torrent_params_t {
            torrent_data: data.as_ptr(),
            torrent_size: data.len(),
        };

        let save_path_c = std::ffi::CString::new(save_path)?;
        let mut error_msg: *mut std::os::raw::c_char = std::ptr::null_mut();
        let err = unsafe {
            libtorrent_sys::libtorrent_add_torrent_ex(inner, &params, save_path_c.as_ptr(), save_path.len(), &mut error_msg)
        };

        if err != libtorrent_sys::libtorrent_error_t_LIBTORRENT_OK {
            let msg = if !error_msg.is_null() {
                unsafe { CStr::from_ptr(error_msg) }
                    .to_string_lossy()
                    .into_owned()
            } else {
                format!("libtorrent error code: {}", err)
            };

            if !error_msg.is_null() {
                unsafe { libc::free(error_msg as *mut std::ffi::c_void) };
            }

            bail!("Failed to add torrent: {}", msg);
        }

        Ok(())
    }

    pub fn pop_alerts(&self) -> AlertList {
        let inner = *self.inner.lock().unwrap();
        let alerts = unsafe { libtorrent_sys::libtorrent_pop_alerts(inner) };
        AlertList::from_ffi(alerts)
    }

    pub fn wait_for_alert(&self, timeout: Duration) -> bool {
        let inner = *self.inner.lock().unwrap();
        let timeout_ms = timeout.as_millis() as i32;
        let result = unsafe { libtorrent_sys::libtorrent_wait_for_alert(inner, timeout_ms) };
        result != 0
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        if let Ok(inner) = self.inner.lock() {
            let ptr = *inner;
            if !ptr.is_null() {
                unsafe { libtorrent_sys::libtorrent_destroy_session(ptr) };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_create_session() {
        let session = Session::new();
        assert!(session.is_ok(), "Failed to create session: {:?}", session.err());
    }

    fn session_test_torrent_dir() -> std::path::PathBuf {
        let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        manifest_dir.join("../test_data") // torrentfs-libtorrent/../test_data
    }

    fn first_torrent_file_session() -> Option<std::path::PathBuf> {
        let dir = session_test_torrent_dir();
        std::fs::read_dir(&dir).ok()?.filter_map(|e| {
            let e = e.ok()?;
            if e.file_name().to_string_lossy().ends_with(".torrent") {
                Some(e.path())
            } else {
                None
            }
        }).next()
    }

    #[test]
    fn test_add_torrent_paused() {
        let session = Session::new().unwrap();
        let test_file = first_torrent_file_session().expect("No .torrent file found");
        let data = fs::read(&test_file).expect("Failed to read test torrent file");
        let result = session.add_torrent_paused(&data, "/tmp/torrentfs");
        assert!(result.is_ok(), "Failed to add torrent: {:?}", result.err());
    }

    #[test]
    fn test_session_drop_does_not_crash() {
        let session = Session::new().unwrap();
        drop(session);
    }
}
