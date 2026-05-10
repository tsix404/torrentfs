use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::piece_cache::PieceCache;
use torrentfs_libtorrent::{LibtorrentError, LibtorrentErrorCode, Session};

const PIECE_DEADLINE_MS: i32 = 30_000;
const MAX_RETRIES: u32 = 3;
const INITIAL_RETRY_DELAY_MS: u64 = 1000;

#[derive(Debug, Clone)]
pub struct DownloadError {
    pub code: LibtorrentErrorCode,
    pub message: String,
}

impl DownloadError {
    pub fn from_libtorrent(err: &LibtorrentError) -> Self {
        Self {
            code: err.code,
            message: err.message.clone(),
        }
    }

    pub fn is_permanent(&self) -> bool {
        self.code.is_permanent()
    }

    pub fn is_transient(&self) -> bool {
        self.code.is_transient()
    }
}

impl std::fmt::Display for DownloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for DownloadError {}

pub struct DownloadCoordinator {
    session: Arc<Session>,
    piece_cache: Arc<PieceCache>,
}

impl DownloadCoordinator {
    pub fn new(session: Arc<Session>, piece_cache: Arc<PieceCache>) -> Self {
        Self { session, piece_cache }
    }

    pub fn get_piece(&self, info_hash: &str, piece_index: u32) -> Result<Vec<u8>, DownloadError> {
        if self.piece_cache.has_piece(info_hash, piece_index) {
            return self.piece_cache.read_piece(info_hash, piece_index)
                .map_err(|e| DownloadError { 
                    code: LibtorrentErrorCode::Unknown, 
                    message: e.to_string() 
                });
        }

        self.download_piece_with_retry(info_hash, piece_index)
    }

    fn download_piece_with_retry(&self, info_hash: &str, piece_index: u32) -> Result<Vec<u8>, DownloadError> {
        let mut last_error = DownloadError { 
            code: LibtorrentErrorCode::Unknown, 
            message: String::new() 
        };
        
        for attempt in 0..MAX_RETRIES {
            match self.download_piece(info_hash, piece_index) {
                Ok(data) => return Ok(data),
                Err(e) => {
                    if e.is_permanent() {
                        return Err(e);
                    }
                    
                    last_error = e;
                    
                    if attempt + 1 < MAX_RETRIES {
                        let delay = INITIAL_RETRY_DELAY_MS * (1 << attempt);
                        tracing::warn!(
                            info_hash = %info_hash,
                            piece_index = piece_index,
                            attempt = attempt + 1,
                            delay_ms = delay,
                            error = %last_error,
                            "Transient error, retrying"
                        );
                        thread::sleep(Duration::from_millis(delay));
                    }
                }
            }
        }
        
        Err(last_error)
    }

    fn download_piece(&self, info_hash: &str, piece_index: u32) -> Result<Vec<u8>, DownloadError> {
        if !self.session.find_torrent(info_hash) {
            return Err(DownloadError { 
                code: LibtorrentErrorCode::InvalidData,
                message: format!("Torrent not found: {}", info_hash) 
            });
        }

        self.session.resume_torrent(info_hash)
            .map_err(|e| {
                if let Some(lt_err) = e.downcast_ref::<LibtorrentError>() {
                    DownloadError::from_libtorrent(lt_err)
                } else {
                    DownloadError { 
                        code: LibtorrentErrorCode::Unknown, 
                        message: e.to_string() 
                    }
                }
            })?;

        self.session.set_piece_deadline(info_hash, piece_index, PIECE_DEADLINE_MS)
            .map_err(|e| {
                if let Some(lt_err) = e.downcast_ref::<LibtorrentError>() {
                    DownloadError::from_libtorrent(lt_err)
                } else {
                    DownloadError { 
                        code: LibtorrentErrorCode::Unknown, 
                        message: e.to_string() 
                    }
                }
            })?;

        let data = self.session.read_piece(info_hash, piece_index)
            .map_err(|e| {
                if let Some(lt_err) = e.downcast_ref::<LibtorrentError>() {
                    DownloadError::from_libtorrent(lt_err)
                } else {
                    DownloadError { 
                        code: LibtorrentErrorCode::Unknown, 
                        message: e.to_string() 
                    }
                }
            })?;

        if let Err(e) = self.piece_cache.write_piece(info_hash, piece_index, &data) {
            tracing::warn!(
                info_hash = %info_hash,
                piece_index = piece_index,
                error = %e,
                "Failed to cache piece"
            );
        }

        Ok(data)
    }
}

unsafe impl Send for DownloadCoordinator {}
unsafe impl Sync for DownloadCoordinator {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (TempDir, Arc<PieceCache>) {
        let temp_dir = TempDir::new().unwrap();
        let cache = Arc::new(
            PieceCache::with_cache_dir(temp_dir.path().join("cache")).unwrap()
        );
        (temp_dir, cache)
    }

    #[test]
    fn test_download_coordinator_new() {
        let session = Arc::new(Session::new().unwrap());
        let (_temp_dir, cache) = setup();
        
        let _coordinator = DownloadCoordinator::new(session, cache);
    }

    #[test]
    fn test_get_piece_cache_hit() {
        let session = Arc::new(Session::new().unwrap());
        let (_temp_dir, cache) = setup();
        
        let info_hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let piece_index = 0u32;
        let data = b"cached piece data";
        
        cache.write_piece(info_hash, piece_index, data).unwrap();
        
        let coordinator = DownloadCoordinator::new(session, cache);
        let result = coordinator.get_piece(info_hash, piece_index);
        
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), data);
    }

    #[test]
    fn test_get_piece_torrent_not_found() {
        let session = Arc::new(Session::new().unwrap());
        let (_temp_dir, cache) = setup();
        
        let coordinator = DownloadCoordinator::new(session, cache);
        let result = coordinator.get_piece("nonexistent_hash_12345678901234567890", 0);
        
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.is_permanent());
        assert!(err.message.contains("not found"));
    }

    #[test]
    fn test_error_code_is_permanent() {
        assert!(LibtorrentErrorCode::InvalidData.is_permanent());
        assert!(LibtorrentErrorCode::ParseFailed.is_permanent());
        assert!(LibtorrentErrorCode::AllocationFailed.is_permanent());
        assert!(!LibtorrentErrorCode::Timeout.is_permanent());
        assert!(!LibtorrentErrorCode::Unknown.is_permanent());
    }

    #[test]
    fn test_error_code_is_transient() {
        assert!(LibtorrentErrorCode::Timeout.is_transient());
        assert!(LibtorrentErrorCode::Unknown.is_transient());
        assert!(!LibtorrentErrorCode::InvalidData.is_transient());
    }
}
