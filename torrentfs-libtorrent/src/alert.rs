use std::ffi::CStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertType {
    Unknown,
    PieceFinished,
    TorrentFinished,
    SaveResumeData,
    AddTorrent,
    MetadataReceived,
    PieceRead,
}

impl AlertType {
    pub fn from_ffi(ffi_type: libtorrent_sys::libtorrent_alert_type_t) -> Self {
        match ffi_type {
            libtorrent_sys::libtorrent_alert_type_t_LIBTORRENT_ALERT_PIECE_FINISHED => AlertType::PieceFinished,
            libtorrent_sys::libtorrent_alert_type_t_LIBTORRENT_ALERT_TORRENT_FINISHED => AlertType::TorrentFinished,
            libtorrent_sys::libtorrent_alert_type_t_LIBTORRENT_ALERT_SAVE_RESUME_DATA => AlertType::SaveResumeData,
            libtorrent_sys::libtorrent_alert_type_t_LIBTORRENT_ALERT_ADD_TORRENT => AlertType::AddTorrent,
            libtorrent_sys::libtorrent_alert_type_t_LIBTORRENT_ALERT_METADATA_RECEIVED => AlertType::MetadataReceived,
            libtorrent_sys::libtorrent_alert_type_t_LIBTORRENT_ALERT_PIECE_READ => AlertType::PieceRead,
            _ => AlertType::Unknown,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Alert {
    pub alert_type: AlertType,
    pub type_name: String,
    pub message: String,
    pub info_hash: Option<String>,
    pub piece_index: u32,
}

impl Alert {
    pub(crate) fn from_ffi(ffi_alert: &libtorrent_sys::libtorrent_alert_t) -> Self {
        let alert_type = AlertType::from_ffi(ffi_alert.type_);
        
        let type_name = if !ffi_alert.alert_type_name.is_null() {
            unsafe { CStr::from_ptr(ffi_alert.alert_type_name) }
                .to_string_lossy()
                .into_owned()
        } else {
            String::from("unknown")
        };
        
        let message = if !ffi_alert.message.is_null() {
            unsafe { CStr::from_ptr(ffi_alert.message) }
                .to_string_lossy()
                .into_owned()
        } else {
            String::new()
        };
        
        let info_hash = if !ffi_alert.info_hash_hex.is_null() {
            let hash = unsafe { CStr::from_ptr(ffi_alert.info_hash_hex) }
                .to_string_lossy()
                .into_owned();
            if hash.len() == 40 {
                Some(hash)
            } else {
                None
            }
        } else {
            None
        };
        
        Alert {
            alert_type,
            type_name,
            message,
            info_hash,
            piece_index: ffi_alert.piece_index,
        }
    }
}

#[derive(Debug)]
pub struct AlertList {
    inner: libtorrent_sys::libtorrent_alert_list_t,
}

impl AlertList {
    pub(crate) fn from_ffi(inner: libtorrent_sys::libtorrent_alert_list_t) -> Self {
        Self { inner }
    }
    
    pub fn iter(&self) -> AlertIter<'_> {
        AlertIter {
            alerts: self.inner.alerts,
            count: self.inner.count,
            index: 0,
            _marker: std::marker::PhantomData,
        }
    }
    
    pub fn len(&self) -> usize {
        self.inner.count
    }
    
    pub fn is_empty(&self) -> bool {
        self.inner.count == 0
    }
}

impl Drop for AlertList {
    fn drop(&mut self) {
        unsafe {
            libtorrent_sys::libtorrent_free_alert_list(&mut self.inner);
        }
    }
}

pub struct AlertIter<'a> {
    alerts: *const libtorrent_sys::libtorrent_alert_t,
    count: usize,
    index: usize,
    _marker: std::marker::PhantomData<&'a AlertList>,
}

impl<'a> Iterator for AlertIter<'a> {
    type Item = Alert;
    
    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.count {
            return None;
        }
        
        let alert = unsafe { &*self.alerts.add(self.index) };
        self.index += 1;
        Some(Alert::from_ffi(alert))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_alert_type_from_ffi() {
        assert_eq!(
            AlertType::from_ffi(libtorrent_sys::libtorrent_alert_type_t_LIBTORRENT_ALERT_PIECE_FINISHED),
            AlertType::PieceFinished
        );
        assert_eq!(
            AlertType::from_ffi(libtorrent_sys::libtorrent_alert_type_t_LIBTORRENT_ALERT_TORRENT_FINISHED),
            AlertType::TorrentFinished
        );
        assert_eq!(
            AlertType::from_ffi(libtorrent_sys::libtorrent_alert_type_t_LIBTORRENT_ALERT_UNKNOWN),
            AlertType::Unknown
        );
    }
}
