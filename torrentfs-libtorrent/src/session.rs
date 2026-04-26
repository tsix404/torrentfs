use anyhow::{Result, bail};
use std::ffi::CStr;

#[derive(Debug)]
pub struct Session {
    inner: *mut libtorrent_sys::libtorrent_session_t,
}

unsafe impl Send for Session {}
unsafe impl Sync for Session {}

impl Session {
    pub fn new() -> Result<Self> {
        let inner = unsafe { libtorrent_sys::libtorrent_create_session() };
        if inner.is_null() {
            bail!("Failed to create libtorrent session");
        }
        Ok(Self { inner })
    }

    pub fn add_torrent_paused(&self, data: &[u8]) -> Result<()> {
        let params = libtorrent_sys::libtorrent_add_torrent_params_t {
            torrent_data: data.as_ptr(),
            torrent_size: data.len(),
        };

        let mut error_msg: *mut std::os::raw::c_char = std::ptr::null_mut();
        let err = unsafe {
            libtorrent_sys::libtorrent_add_torrent(self.inner, &params, &mut error_msg)
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
}

impl Drop for Session {
    fn drop(&mut self) {
        if !self.inner.is_null() {
            unsafe { libtorrent_sys::libtorrent_destroy_session(self.inner) };
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

    #[test]
    fn test_add_torrent_paused() {
        let session = Session::new().unwrap();
        let test_file = "/workspace/torrentfs/77c8dd8e37d712522b49a3f2e62757d90e233c84.torrent";
        let data = fs::read(test_file).expect("Failed to read test torrent file");
        let result = session.add_torrent_paused(&data);
        assert!(result.is_ok(), "Failed to add torrent: {:?}", result.err());
    }

    #[test]
    fn test_session_drop_does_not_crash() {
        let session = Session::new().unwrap();
        drop(session);
    }
}
