use crate::alert::AlertList;
use crate::error::{LibtorrentError, LibtorrentErrorCode};

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
        let guard = self.inner.lock().unwrap();
        let inner = *guard;
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

    pub fn add_torrent_with_resume(&self, data: &[u8], save_path: &str, resume_data: Option<&[u8]>) -> Result<()> {
        let guard = self.inner.lock().unwrap();
        let inner = *guard;
        let params = libtorrent_sys::libtorrent_add_torrent_params_t {
            torrent_data: data.as_ptr(),
            torrent_size: data.len(),
        };

        let save_path_c = std::ffi::CString::new(save_path)?;
        let mut error_msg: *mut std::os::raw::c_char = std::ptr::null_mut();
        
        let err = match resume_data {
            Some(resume) => {
                unsafe {
                    libtorrent_sys::libtorrent_add_torrent_with_resume(
                        inner,
                        &params,
                        save_path_c.as_ptr(),
                        save_path.len(),
                        resume.as_ptr(),
                        resume.len(),
                        &mut error_msg
                    )
                }
            }
            None => {
                unsafe {
                    libtorrent_sys::libtorrent_add_torrent_ex(
                        inner,
                        &params,
                        save_path_c.as_ptr(),
                        save_path.len(),
                        &mut error_msg
                    )
                }
            }
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

            bail!("Failed to add torrent with resume data: {}", msg);
        }

        Ok(())
    }

    pub fn pop_alerts(&self) -> AlertList {
        let guard = self.inner.lock().unwrap();
        let alerts = unsafe { libtorrent_sys::libtorrent_pop_alerts(*guard) };
        AlertList::from_ffi(alerts)
    }

    pub fn wait_for_alert(&self, timeout: Duration) -> bool {
        let guard = self.inner.lock().unwrap();
        let timeout_ms = timeout.as_millis() as i32;
        let result = unsafe { libtorrent_sys::libtorrent_wait_for_alert(*guard, timeout_ms) };
        result != 0
    }

    pub fn set_alert_mask(&self, mask: u64) {
        let guard = self.inner.lock().unwrap();
        unsafe { libtorrent_sys::libtorrent_set_alert_mask(*guard, mask) };
    }

    pub fn find_torrent(&self, info_hash_hex: &str) -> bool {
        let guard = self.inner.lock().unwrap();
        let info_hash_c = match std::ffi::CString::new(info_hash_hex) {
            Ok(c) => c,
            Err(_) => return false,
        };
        let result = unsafe {
            libtorrent_sys::libtorrent_find_torrent(*guard, info_hash_c.as_ptr())
        };
        result != 0
    }

    pub fn resume_torrent(&self, info_hash_hex: &str) -> Result<()> {
        let guard = self.inner.lock().unwrap();
        let info_hash_c = std::ffi::CString::new(info_hash_hex)?;
        
        let err = unsafe {
            libtorrent_sys::libtorrent_resume_torrent(*guard, info_hash_c.as_ptr())
        };

        if err != libtorrent_sys::libtorrent_error_t_LIBTORRENT_OK {
            let code = LibtorrentErrorCode::from_ffi(err as i32);
            bail!(LibtorrentError::new(code, "Failed to resume torrent"));
        }

        Ok(())
    }

    pub fn is_seeding(&self, info_hash_hex: &str) -> bool {
        let guard = self.inner.lock().unwrap();
        let info_hash_c = match std::ffi::CString::new(info_hash_hex) {
            Ok(c) => c,
            Err(_) => return false,
        };
        
        let result = unsafe {
            libtorrent_sys::libtorrent_is_seeding(*guard, info_hash_c.as_ptr())
        };
        
        result != 0
    }

    pub fn get_torrents(&self) -> Vec<String> {
        let guard = self.inner.lock().unwrap();
        let list = unsafe { libtorrent_sys::libtorrent_get_torrents(*guard) };
        
        let mut result = Vec::new();
        if !list.info_hashes.is_null() && list.count > 0 {
            for i in 0..list.count {
                unsafe {
                    if !(*list.info_hashes.add(i)).is_null() {
                        let c_str = std::ffi::CStr::from_ptr(*list.info_hashes.add(i));
                        if let Ok(s) = c_str.to_str() {
                            result.push(s.to_string());
                        }
                    }
                }
            }
            unsafe { libtorrent_sys::libtorrent_free_info_hash_list(&list as *const _ as *mut _) };
        }
        
        result
    }

    pub fn save_resume_data(&self, info_hash_hex: &str) -> Result<()> {
        let guard = self.inner.lock().unwrap();
        let info_hash_c = std::ffi::CString::new(info_hash_hex)?;
        
        let err = unsafe {
            libtorrent_sys::libtorrent_save_resume_data(*guard, info_hash_c.as_ptr())
        };

        if err != libtorrent_sys::libtorrent_error_t_LIBTORRENT_OK {
            let code = LibtorrentErrorCode::from_ffi(err as i32);
            bail!(LibtorrentError::new(code, "Failed to save resume data"));
        }

        Ok(())
    }

    pub fn pause_torrent(&self, info_hash_hex: &str) -> Result<()> {
        let guard = self.inner.lock().unwrap();
        let info_hash_c = std::ffi::CString::new(info_hash_hex)?;
        
        let err = unsafe {
            libtorrent_sys::libtorrent_pause_torrent(*guard, info_hash_c.as_ptr())
        };

        if err != libtorrent_sys::libtorrent_error_t_LIBTORRENT_OK {
            let code = LibtorrentErrorCode::from_ffi(err as i32);
            bail!(LibtorrentError::new(code, "Failed to pause torrent"));
        }

        Ok(())
    }

    pub fn set_piece_deadline(&self, info_hash_hex: &str, piece_index: u32, deadline_ms: i32) -> Result<()> {
        let guard = self.inner.lock().unwrap();
        let info_hash_c = std::ffi::CString::new(info_hash_hex)?;
        
        let err = unsafe {
            libtorrent_sys::libtorrent_set_piece_deadline(*guard, info_hash_c.as_ptr(), piece_index, deadline_ms)
        };

        if err != libtorrent_sys::libtorrent_error_t_LIBTORRENT_OK {
            let code = LibtorrentErrorCode::from_ffi(err as i32);
            bail!(LibtorrentError::new(code, "Failed to set piece deadline"));
        }

        Ok(())
    }

    /// Reads a piece from the torrent.
    /// 
    /// # Blocking Behavior
    /// This function uses `wait_for_alert()` internally and may block for up to 30 seconds.
    /// For concurrent filesystem operations, consider spawning blocking tasks on a separate
    /// thread pool rather than calling this directly from async code.
    pub fn read_piece(&self, info_hash_hex: &str, piece_index: u32) -> Result<Vec<u8>> {
        let guard = self.inner.lock().unwrap();
        let info_hash_c = std::ffi::CString::new(info_hash_hex)?;
        
        let result = unsafe {
            libtorrent_sys::libtorrent_read_piece(*guard, info_hash_c.as_ptr(), piece_index)
        };

        if result.error_code != libtorrent_sys::libtorrent_error_t_LIBTORRENT_OK {
            let code = LibtorrentErrorCode::from_ffi(result.error_code as i32);
            let msg = if !result.error_message.is_null() {
                let msg = unsafe { CStr::from_ptr(result.error_message) }
                    .to_string_lossy()
                    .into_owned();
                unsafe { libc::free(result.error_message as *mut std::ffi::c_void) };
                msg
            } else {
                String::new()
            };
            bail!(LibtorrentError::new(code, msg));
        }

        let data = if !result.data.is_null() && result.size > 0 {
            let vec = unsafe {
                std::slice::from_raw_parts(result.data, result.size).to_vec()
            };
            unsafe { libc::free(result.data as *mut std::ffi::c_void) };
            vec
        } else {
            Vec::new()
        };

        Ok(data)
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

    #[test]
    fn test_is_seeding_returns_false_for_nonexistent_torrent() {
        let session = Session::new().unwrap();
        let fake_info_hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        assert!(!session.is_seeding(fake_info_hash));
    }

    #[test]
    fn test_is_seeding_returns_false_for_paused_torrent() {
        let session = Session::new().unwrap();
        let test_file = first_torrent_file_session().expect("No .torrent file found");
        let data = fs::read(&test_file).expect("Failed to read test torrent file");
        session.add_torrent_paused(&data, "/tmp/torrentfs").unwrap();
        
        let ti = crate::torrent::parse_torrent(&data).unwrap();
        assert!(!session.is_seeding(&ti.info_hash));
    }
}
