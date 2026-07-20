use std::collections::HashMap;
use std::ffi::CString;
use std::path::Path;
use std::ptr;
use std::sync::{Arc, Mutex};

use crate::cache::CacheManager;
use crate::config::TorrentfsConfig;
use crate::error::{error_from_c, TorrentError, TorrentResult};
use crate::seeding::SeedingManager;

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
    custom_storage_active: bool,
    read_timeout_secs: u64,
}

impl Session {
    pub fn new(config: &TorrentfsConfig) -> TorrentResult<Self> {
        let mut error = libtorrent_sys::lt_error_t {
            message: ptr::null(),
            code: 0,
        };

        // Create session with default settings (alert_mask only, no listen_interface)
        let inner = unsafe { libtorrent_sys::lt_session_create(ptr::null(), &mut error) };

        if inner.is_null() {
            return Err(unsafe { error_from_c(&error) });
        }

        let session = Session { inner };

        // Apply user configuration via JSON
        let settings_json = config.to_settings_json();
        if settings_json != "{}" {
            let json_c = CString::new(settings_json).unwrap_or_default();
            unsafe {
                libtorrent_sys::lt_session_apply_settings(session.inner, json_c.as_ptr());
            }
        }

        Ok(session)
    }

    pub fn add_torrent(
        &mut self,
        info: &crate::TorrentInfo,
        save_path: &Path,
    ) -> TorrentResult<TorrentHandle> {
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

    pub fn add_torrent_with_custom_storage(
        &mut self,
        info: &crate::TorrentInfo,
        piece_cache_dir: &Path,
    ) -> TorrentResult<TorrentHandle> {
        let piece_cache_dir_c = CString::new(piece_cache_dir.to_string_lossy().into_owned())
            .map_err(|_| TorrentError::InvalidFile("Piece cache dir contains null byte".to_string()))?;

        let mut error = libtorrent_sys::lt_error_t {
            message: ptr::null(),
            code: 0,
        };

        let handle = unsafe {
            libtorrent_sys::lt_session_add_torrent_with_custom_storage(
                self.inner,
                info.inner,
                piece_cache_dir_c.as_ptr(),
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

    #[allow(dead_code)]
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

    /// Get session-level statistics (rates, connections, DHT nodes).
    pub fn get_stats(&self) -> TorrentResult<SessionStats> {
        let mut stats = libtorrent_sys::lt_session_stats_t {
            download_rate: 0,
            upload_rate: 0,
            total_downloaded: 0,
            total_uploaded: 0,
            dht_nodes: 0,
            peers_connected: 0,
            half_open_connections: 0,
        };
        let mut status: i32 = -1;

        let result =
            unsafe { libtorrent_sys::lt_session_get_stats(self.inner, &mut stats, &mut status) };

        if result != 0 {
            Err(TorrentError::Unknown {
                code: result,
                message: "Failed to get session stats".to_string(),
            })
        } else {
            Ok(SessionStats {
                download_rate: stats.download_rate,
                upload_rate: stats.upload_rate,
                total_downloaded: stats.total_downloaded,
                total_uploaded: stats.total_uploaded,
                dht_nodes: stats.dht_nodes,
                peers_connected: stats.peers_connected,
                half_open_connections: stats.half_open_connections,
            })
        }
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
        let mut download_rate: i64 = 0;
        let mut upload_rate: i64 = 0;
        let mut total_download: i64 = 0;
        let mut total_upload: i64 = 0;
        let mut num_peers: i32 = 0;
        let mut num_seeds: i32 = 0;

        let result = unsafe {
            libtorrent_sys::lt_torrent_handle_status(
                self.inner,
                &mut state,
                &mut progress,
                &mut total_done,
                &mut total,
                &mut download_rate,
                &mut upload_rate,
                &mut total_download,
                &mut total_upload,
                &mut num_peers,
                &mut num_seeds,
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
                download_rate,
                upload_rate,
                total_download,
                total_upload,
                num_peers,
                num_seeds,
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

    pub fn get_torrent_info(&self) -> TorrentResult<(i64, i64)> {
        let mut piece_length: i64 = 0;
        let mut num_pieces: i64 = 0;

        let result = unsafe {
            libtorrent_sys::lt_torrent_handle_get_torrent_info(
                self.inner,
                &mut piece_length,
                &mut num_pieces,
            )
        };

        if result != 0 {
            Err(TorrentError::Unknown {
                code: result,
                message: "Failed to get torrent info from handle".to_string(),
            })
        } else {
            Ok((piece_length, num_pieces))
        }
    }

    pub fn have_piece(&self, piece_index: i32) -> bool {
        unsafe { libtorrent_sys::lt_torrent_handle_have_piece(self.inner, piece_index) != 0 }
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
pub struct SessionStats {
    pub download_rate: i64,
    pub upload_rate: i64,
    pub total_downloaded: i64,
    pub total_uploaded: i64,
    pub dht_nodes: i32,
    pub peers_connected: i32,
    pub half_open_connections: i32,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TorrentStatus {
    pub state: TorrentState,
    pub progress: f32,
    pub total_done: u64,
    pub total: u64,
    pub download_rate: i64,
    pub upload_rate: i64,
    pub total_download: i64,
    pub total_upload: i64,
    pub num_peers: i32,
    pub num_seeds: i32,
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
#[allow(dead_code)]
pub struct FilePieceInfo {
    pub first_piece: i64,
    pub num_pieces: i64,
    pub file_offset: i64,
}

impl DownloadManager {
    pub fn new(cache_dir: &Path, config: &TorrentfsConfig) -> TorrentResult<Self> {
        let session = Session::new(config)?;
        let cache_dir_str = cache_dir.to_string_lossy().into_owned();

        let cache_manager = CacheManager::new(cache_dir, 1024 * 1024 * 1024)?;

        let read_timeout_secs = config
            .timeouts
            .read_timeout_secs
            .map(|v| if v > 0 { v as u64 } else { 30 })
            .unwrap_or(30);

        Ok(DownloadManager {
            session: Arc::new(Mutex::new(session)),
            handles: HashMap::new(),
            cache_dir: cache_dir_str,
            cache_manager: Arc::new(Mutex::new(cache_manager)),
            custom_storage_active: false,
            read_timeout_secs,
        })
    }

    #[allow(dead_code)]
    pub fn get_cache_manager(&self) -> Arc<Mutex<CacheManager>> {
        self.cache_manager.clone()
    }

    /// Register a SeedingManager to receive eviction callbacks from the CacheManager.
    /// When CacheManager evicts cached pieces, the affected infohash will be sent to
    /// the SeedingManager so it can stop seeding the corresponding torrent.
    pub fn register_seeding_callback(&self, seeding: Arc<SeedingManager>) {
        let mut cache = self
            .cache_manager
            .lock()
            .expect("CacheManager lock poisoned");
        seeding.register_eviction_callback(&mut cache);
    }

    /// Get session-level stats.
    pub fn get_session_stats(&self) -> TorrentResult<SessionStats> {
        let session = self.session.lock().map_err(|_| TorrentError::Unknown {
            code: -1,
            message: "Session lock poisoned".to_string(),
        })?;
        session.get_stats()
    }

    /// Get all torrent handles and their info hashes.
    pub fn get_all_handles(&self) -> Vec<(String, Arc<Mutex<TorrentHandle>>)> {
        self.handles
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn get_or_create_handle(
        &mut self,
        info: &crate::TorrentInfo,
    ) -> TorrentResult<Arc<Mutex<TorrentHandle>>> {
        let info_hash = hex::encode(info.info_hash()?);

        if let Some(handle) = self.handles.get(&info_hash) {
            return Ok(handle.clone());
        }

        // Use cache/pieces/ as the piece storage directory.
        // Note: the C++ PieceStorageDiskIO creates a "pieces/" subdirectory
        // under the given path, so we pass the base cache_dir (not cache/pieces/).
        let cache_base = Path::new(&self.cache_dir);
        let pieces_dir = cache_base.join("pieces");
        std::fs::create_dir_all(&pieces_dir)
            .map_err(|e| TorrentError::IoError(e.to_string()))?;

        let mut session = self.session.lock().map_err(|_| TorrentError::Unknown {
            code: -1,
            message: "Session lock poisoned".to_string(),
        })?;

        let handle = if !self.custom_storage_active {
            // First torrent: replace session with custom-storage session
            let h = session.add_torrent_with_custom_storage(info, cache_base)?;
            self.custom_storage_active = true;
            h
        } else {
            // Custom storage already active: use regular add_torrent on the custom-storage session
            session.add_torrent(info, &pieces_dir)?
        };

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
        let handle_guard = handle.lock().map_err(|_| TorrentError::Unknown {
            code: -1,
            message: "Handle lock poisoned".to_string(),
        })?;

        if !handle_guard.is_valid() {
            return Err(TorrentError::InvalidFile(
                "Torrent handle is invalid".to_string(),
            ));
        }

        let mut status = handle_guard.status()?;
        tracing::debug!(
            "read_file_range: initial torrent state = {:?}, progress = {:.2}%",
            status.state,
            status.progress * 100.0
        );

        // Wait up to read_timeout_secs for CheckingFiles/re-verification to complete.
        // Configurable via [timeouts] read_timeout_secs in config.toml.
        let max_wait_secs = self.read_timeout_secs;
        let start = std::time::Instant::now();
        while matches!(
            status.state,
            TorrentState::QueuedForChecking
                | TorrentState::CheckingFiles
                | TorrentState::Allocating
                | TorrentState::CheckingResumeData
        ) {
            if start.elapsed().as_secs() > max_wait_secs {
                return Err(TorrentError::InvalidFile(format!(
                    "Torrent stuck in state {:?} for {} seconds",
                    status.state, max_wait_secs
                )));
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
            status = handle_guard.status()?;
            tracing::debug!(
                "read_file_range: waiting for torrent state = {:?}, progress = {:.2}%",
                status.state,
                status.progress * 100.0
            );
        }

        tracing::debug!(
            "read_file_range: final torrent state = {:?}, progress = {:.2}%, peers = {}, seeds = {}",
            status.state,
            status.progress * 100.0,
            status.num_peers,
            status.num_seeds
        );

        // Health check: if tracker returned 0 peers and 0 seeds,
        // provide a clear error message instead of timing out silently.
        if status.num_peers == 0 && status.num_seeds == 0 {
            return Err(TorrentError::NoPeers(format!(
                "Torrent has {} peers and {} seeds (progress: {:.2}%, state: {:?}). \
                 The tracker may be unreachable or the torrent has no active peers.",
                status.num_peers,
                status.num_seeds,
                status.progress * 100.0,
                status.state
            )));
        }

        std::thread::sleep(std::time::Duration::from_millis(100));

        let info_hash = handle_guard.info_hash().to_string();
        let piece_info = handle_guard.get_file_piece_info(file_index)?;
        let (handle_piece_length, handle_num_pieces) = handle_guard.get_torrent_info()?;
        let piece_length = handle_piece_length as u64;
        let num_pieces = handle_num_pieces as i32;

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
            std::cmp::min(((end_offset - 1) / piece_length) as i32, num_pieces - 1)
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

        let piece_wait_timeout = std::time::Duration::from_secs(self.read_timeout_secs);
        for piece_idx in start_piece..=end_piece {
            if !handle_guard.have_piece(piece_idx) {
                tracing::debug!(
                    "read_file_range: piece {} not available, waiting for download...",
                    piece_idx
                );
                let piece_wait_start = std::time::Instant::now();
                loop {
                    if piece_wait_start.elapsed() >= piece_wait_timeout {
                        status = handle_guard.status()?;
                        return Err(TorrentError::InvalidFile(format!(
                            "Timed out waiting for piece {} after {:.0}s. \
                             Torrent progress: {:.2}%, peers: {}, seeds: {}",
                            piece_idx,
                            piece_wait_timeout.as_secs(),
                            status.progress * 100.0,
                            status.num_peers,
                            status.num_seeds
                        )));
                    }
                    std::thread::sleep(std::time::Duration::from_millis(200));
                    if handle_guard.have_piece(piece_idx) {
                        tracing::debug!(
                            "read_file_range: piece {} is now available after {:.1}s",
                            piece_idx,
                            piece_wait_start.elapsed().as_secs_f64()
                        );
                        break;
                    }
                }
            }
        }

        let session = self.session.lock().map_err(|_| TorrentError::Unknown {
            code: -1,
            message: "Session lock poisoned".to_string(),
        })?;

        let mut result = Vec::with_capacity(size as usize);
        let mut bytes_read = 0usize;

        for piece_idx in start_piece..=end_piece {
            let piece_key = Self::make_piece_key(&info_hash, piece_idx);
            let piece_data = {
                let mut cache = self
                    .cache_manager
                    .lock()
                    .map_err(|_| TorrentError::Unknown {
                        code: -1,
                        message: "Cache lock poisoned".to_string(),
                    })?;

                if cache.has_piece(&piece_key) {
                    let piece_path = cache.piece_path(&piece_key);
                    if let Ok(data) = std::fs::read(&piece_path) {
                        if let Err(e) = cache.record_access(&piece_key) {
                            tracing::warn!(
                                "Failed to record cache access for {}: {:?}",
                                piece_key,
                                e
                            );
                        }
                        data
                    } else {
                        drop(cache);
                        let data = handle_guard.read_piece(&session, piece_idx)?;
                        let mut cache =
                            self.cache_manager
                                .lock()
                                .map_err(|_| TorrentError::Unknown {
                                    code: -1,
                                    message: "Cache lock poisoned".to_string(),
                                })?;
                        let piece_path = cache.ensure_piece_dir(&piece_key)?;
                        if let Err(e) = std::fs::write(&piece_path, &data) {
                            tracing::warn!("Failed to write cache piece {}: {:?}", piece_key, e);
                        }
                        if let Err(e) = cache.add_piece(&piece_key, data.len() as u64) {
                            tracing::warn!(
                                "Failed to add piece {} to cache metadata: {:?}",
                                piece_key,
                                e
                            );
                        }
                        data
                    }
                } else {
                    drop(cache);
                    let data = handle_guard.read_piece(&session, piece_idx)?;
                    let mut cache =
                        self.cache_manager
                            .lock()
                            .map_err(|_| TorrentError::Unknown {
                                code: -1,
                                message: "Cache lock poisoned".to_string(),
                            })?;
                    let piece_path = cache.ensure_piece_dir(&piece_key)?;
                    if let Err(e) = std::fs::write(&piece_path, &data) {
                        tracing::warn!("Failed to write cache piece {}: {:?}", piece_key, e);
                    }
                    if let Err(e) = cache.add_piece(&piece_key, data.len() as u64) {
                        tracing::warn!(
                            "Failed to add piece {} to cache metadata: {:?}",
                            piece_key,
                            e
                        );
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
