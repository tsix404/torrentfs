use std::collections::HashMap;
use std::ffi::CString;
use std::path::Path;
use std::ptr;
use std::sync::{Arc, Mutex};

use crate::cache::CacheManager;
use crate::error::{TorrentError, TorrentResult, error_from_c};

pub struct Session {
    inner: libtorrent_sys::lt_session_t,
}

pub struct TorrentHandle {
    inner: libtorrent_sys::lt_torrent_handle_t,
    info_hash: String,
    #[allow(dead_code)]
    session: libtorrent_sys::lt_session_t,
}

pub struct DownloadManager {
    session: Arc<Mutex<Session>>,
    handles: HashMap<String, Arc<Mutex<TorrentHandle>>>,
    cache_dir: String,
    cache_manager: Arc<Mutex<CacheManager>>,
}

impl Session {
    pub fn new(listen_interface: Option<&str>) -> TorrentResult<Self> {
        let listen = listen_interface.map(|s| CString::new(s).unwrap()).unwrap_or_default();
        
        let mut error = libtorrent_sys::lt_error_t {
            message: ptr::null(),
            code: 0,
        };
        
        let inner = unsafe {
            libtorrent_sys::lt_session_create(
                if listen_interface.is_some() { listen.as_ptr() } else { ptr::null() },
                &mut error,
            )
        };
        
        if inner.is_null() {
            Err(unsafe { error_from_c(&error) })
        } else {
            Ok(Session { inner })
        }
    }
    
    pub fn add_torrent(&mut self, info: &crate::TorrentInfo, save_path: &Path) -> TorrentResult<TorrentHandle> {
        let save_path_c = CString::new(save_path.to_string_lossy().into_owned())
            .map_err(|_| TorrentError::InvalidFile("Save path contains null byte".to_string()))?;
        
        let mut error = libtorrent_sys::lt_error_t {
            message: ptr::null(),
            code: 0,
        };
        
        let handle = unsafe {
            libtorrent_sys::lt_session_add_torrent(
                self.inner,
                info.inner,
                save_path_c.as_ptr(),
                &mut error,
            )
        };
        
        if handle.is_null() {
            Err(unsafe { error_from_c(&error) })
        } else {
            let info_hash = hex::encode(info.info_hash()?);
            Ok(TorrentHandle { 
                inner: handle, 
                info_hash,
                session: self.inner,
            })
        }
    }
    
    pub fn remove_torrent(&mut self, handle: TorrentHandle, remove_files: bool) {
        unsafe {
            libtorrent_sys::lt_session_remove_torrent(
                self.inner,
                handle.inner,
                if remove_files { 1 } else { 0 },
            );
        }
    }
    
    fn inner(&self) -> libtorrent_sys::lt_session_t {
        self.inner
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        unsafe {
            libtorrent_sys::lt_session_destroy(self.inner);
        }
    }
}

unsafe impl Send for Session {}

impl TorrentHandle {
    pub fn is_valid(&self) -> bool {
        unsafe { libtorrent_sys::lt_torrent_handle_is_valid(self.inner) != 0 }
    }
    
    pub fn status(&self) -> TorrentResult<TorrentStatus> {
        let mut state: i32 = 0;
        let mut progress: f32 = 0.0;
        let mut total_done: u64 = 0;
        let mut total: u64 = 0;
        
        let result = unsafe {
            libtorrent_sys::lt_torrent_handle_status(
                self.inner,
                &mut state,
                &mut progress,
                &mut total_done,
                &mut total,
            )
        };
        
        if result != 0 {
            Err(TorrentError::Unknown {
                code: result,
                message: "Failed to get torrent status".to_string(),
            })
        } else {
            Ok(TorrentStatus {
                state: TorrentState::from(state),
                progress,
                total_done,
                total,
            })
        }
    }
    
    pub fn read_piece(&self, session: &Session, piece_index: i32) -> TorrentResult<Vec<u8>> {
        let mut data_out: *mut u8 = ptr::null_mut();
        let mut size_out: usize = 0;
        
        let mut error = libtorrent_sys::lt_error_t {
            message: ptr::null(),
            code: 0,
        };
        
        let result = unsafe {
            libtorrent_sys::lt_torrent_handle_read_piece(
                session.inner(),
                self.inner,
                piece_index,
                &mut data_out,
                &mut size_out,
                &mut error,
            )
        };
        
        if result != 0 {
            Err(unsafe { error_from_c(&error) })
        } else if data_out.is_null() || size_out == 0 {
            Ok(Vec::new())
        } else {
            let slice = unsafe { std::slice::from_raw_parts(data_out, size_out) };
            let data = slice.to_vec();
            unsafe { libtorrent_sys::lt_piece_data_free(data_out) };
            Ok(data)
        }
    }
    
    pub fn get_file_piece_info(&self, file_index: i32) -> TorrentResult<FilePieceInfo> {
        let mut first_piece: i64 = 0;
        let mut num_pieces: i64 = 0;
        let mut file_offset: i64 = 0;
        
        let result = unsafe {
            libtorrent_sys::lt_torrent_handle_get_piece_info(
                self.inner,
                file_index,
                &mut first_piece,
                &mut num_pieces,
                &mut file_offset,
            )
        };
        
        if result != 0 {
            Err(TorrentError::Unknown {
                code: result,
                message: "Failed to get file piece info".to_string(),
            })
        } else {
            Ok(FilePieceInfo {
                first_piece,
                num_pieces,
                file_offset,
            })
        }
    }
    
    pub fn info_hash(&self) -> &str {
        &self.info_hash
    }
}

impl Drop for TorrentHandle {
    fn drop(&mut self) {
        if !self.inner.is_null() {
            unsafe {
                libtorrent_sys::lt_torrent_handle_destroy(self.inner);
            }
        }
    }
}

unsafe impl Send for TorrentHandle {}

#[derive(Debug, Clone)]
pub struct TorrentStatus {
    pub state: TorrentState,
    pub progress: f32,
    pub total_done: u64,
    pub total: u64,
}

#[derive(Debug, Clone, Copy)]
pub enum TorrentState {
    QueuedForChecking,
    CheckingFiles,
    DownloadingMetadata,
    Downloading,
    Finished,
    Seeding,
    Allocating,
    CheckingResumeData,
    Unknown,
}

impl From<i32> for TorrentState {
    fn from(value: i32) -> Self {
        match value {
            0 => TorrentState::QueuedForChecking,
            1 => TorrentState::CheckingFiles,
            2 => TorrentState::DownloadingMetadata,
            3 => TorrentState::Downloading,
            4 => TorrentState::Finished,
            5 => TorrentState::Seeding,
            6 => TorrentState::Allocating,
            7 => TorrentState::CheckingResumeData,
            _ => TorrentState::Unknown,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FilePieceInfo {
    pub first_piece: i64,
    pub num_pieces: i64,
    pub file_offset: i64,
}

impl DownloadManager {
    pub fn new(cache_dir: &Path) -> TorrentResult<Self> {
        let session = Session::new(None)?;
        let cache_dir_str = cache_dir.to_string_lossy().into_owned();
        
        let cache_manager = CacheManager::new(cache_dir, 1024 * 1024 * 1024)?;
        
        Ok(DownloadManager {
            session: Arc::new(Mutex::new(session)),
            handles: HashMap::new(),
            cache_dir: cache_dir_str,
            cache_manager: Arc::new(Mutex::new(cache_manager)),
        })
    }
    
    pub fn get_cache_manager(&self) -> Arc<Mutex<CacheManager>> {
        self.cache_manager.clone()
    }
    
    pub fn get_or_create_handle(&mut self, info: &crate::TorrentInfo) -> TorrentResult<Arc<Mutex<TorrentHandle>>> {
        let info_hash = hex::encode(info.info_hash()?);
        
        if let Some(handle) = self.handles.get(&info_hash) {
            return Ok(handle.clone());
        }
        
        let cache_path = Path::new(&self.cache_dir).join(&info_hash);
        std::fs::create_dir_all(&cache_path)
            .map_err(|e| TorrentError::IoError(e.to_string()))?;
        
        let mut session = self.session.lock()
            .map_err(|_| TorrentError::Unknown { code: -1, message: "Session lock poisoned".to_string() })?;
        
        let handle = session.add_torrent(info, &cache_path)?;
        let handle = Arc::new(Mutex::new(handle));
        
        self.handles.insert(info_hash.clone(), handle.clone());
        
        Ok(handle)
    }
    
    fn make_piece_key(info_hash: &str, piece_idx: i32) -> String {
        format!("{}:piece:{}", info_hash, piece_idx)
    }
    
    pub fn read_file_range(
        &mut self,
        info: &crate::TorrentInfo,
        file_index: i32,
        offset: u64,
        size: u32,
    ) -> TorrentResult<Vec<u8>> {
        let handle = self.get_or_create_handle(info)?;
        let handle_guard = handle.lock()
            .map_err(|_| TorrentError::Unknown { code: -1, message: "Handle lock poisoned".to_string() })?;
        
        if !handle_guard.is_valid() {
            return Err(TorrentError::InvalidFile("Torrent handle is invalid".to_string()));
        }
        
        let status = handle_guard.status()?;
        tracing::debug!(
            "read_file_range: torrent state = {:?}, progress = {:.2}%",
            status.state, status.progress * 100.0
        );
        
        let info_hash = handle_guard.info_hash().to_string();
        let piece_info = handle_guard.get_file_piece_info(file_index)?;
        let metadata = info.metadata()?;
        let piece_length = metadata.piece_length as u64;
        let num_pieces = metadata.num_pieces as i32;
        
        let file_start_offset = piece_info.file_offset as u64;
        let absolute_offset = file_start_offset + offset;
        
        if num_pieces <= 0 {
            return Err(TorrentError::InvalidFile(format!(
                "Invalid torrent: num_pieces = {}",
                num_pieces
            )));
        }
        
        let start_piece = (absolute_offset / piece_length) as i32;
        let end_offset = absolute_offset + size as u64;
        let end_piece = if size > 0 {
            std::cmp::min(
                ((end_offset - 1) / piece_length) as i32,
                num_pieces - 1
            )
        } else {
            start_piece
        };
        
        if start_piece >= num_pieces {
            return Err(TorrentError::InvalidFile(format!(
                "start_piece {} exceeds num_pieces {} (absolute_offset={}, piece_length={})",
                start_piece, num_pieces, absolute_offset, piece_length
            )));
        }
        
        if start_piece > end_piece {
            return Ok(Vec::new());
        }
        
        tracing::debug!(
            "read_file_range: file_index={}, offset={}, size={}, start_piece={}, end_piece={}, num_pieces={}, piece_length={}",
            file_index, offset, size, start_piece, end_piece, num_pieces, piece_length
        );
        
        let session = self.session.lock()
            .map_err(|_| TorrentError::Unknown { code: -1, message: "Session lock poisoned".to_string() })?;
        
        let mut result = Vec::with_capacity(size as usize);
        let mut bytes_read = 0usize;
        
        for piece_idx in start_piece..=end_piece {
            let piece_key = Self::make_piece_key(&info_hash, piece_idx);
            let piece_data = {
                let mut cache = self.cache_manager.lock()
                    .map_err(|_| TorrentError::Unknown { code: -1, message: "Cache lock poisoned".to_string() })?;
                
                if cache.has_piece(&piece_key) {
                    let piece_path = cache.piece_path(&piece_key);
                    if let Ok(data) = std::fs::read(&piece_path) {
                        if let Err(e) = cache.record_access(&piece_key) {
                            tracing::warn!("Failed to record cache access for {}: {:?}", piece_key, e);
                        }
                        data
                    } else {
                        drop(cache);
                        let data = handle_guard.read_piece(&session, piece_idx)?;
                        let mut cache = self.cache_manager.lock()
                            .map_err(|_| TorrentError::Unknown { code: -1, message: "Cache lock poisoned".to_string() })?;
                        let piece_path = cache.ensure_piece_dir(&piece_key)?;
                        if let Err(e) = std::fs::write(&piece_path, &data) {
                            tracing::warn!("Failed to write cache piece {}: {:?}", piece_key, e);
                        }
                        if let Err(e) = cache.add_piece(&piece_key, data.len() as u64) {
                            tracing::warn!("Failed to add piece {} to cache metadata: {:?}", piece_key, e);
                        }
                        data
                    }
                } else {
                    drop(cache);
                    let data = handle_guard.read_piece(&session, piece_idx)?;
                    let mut cache = self.cache_manager.lock()
                        .map_err(|_| TorrentError::Unknown { code: -1, message: "Cache lock poisoned".to_string() })?;
                    let piece_path = cache.ensure_piece_dir(&piece_key)?;
                    if let Err(e) = std::fs::write(&piece_path, &data) {
                        tracing::warn!("Failed to write cache piece {}: {:?}", piece_key, e);
                    }
                    if let Err(e) = cache.add_piece(&piece_key, data.len() as u64) {
                        tracing::warn!("Failed to add piece {} to cache metadata: {:?}", piece_key, e);
                    }
                    data
                }
            };
            
            let piece_start = (piece_idx as u64) * piece_length;
            let piece_end = piece_start + piece_data.len() as u64;
            
            let read_start = std::cmp::max(absolute_offset, piece_start);
            let read_end = std::cmp::min(end_offset, piece_end);
            
            if read_start < read_end {
                let local_start = (read_start - piece_start) as usize;
                let local_end = (read_end - piece_start) as usize;
                
                let chunk = &piece_data[local_start..local_end];
                result.extend_from_slice(chunk);
                bytes_read += chunk.len();
                
                if bytes_read >= size as usize {
                    break;
                }
            }
        }
        
        Ok(result)
    }
}

unsafe impl Send for DownloadManager {}
