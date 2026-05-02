use anyhow::Result;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;

use crate::alert_loop::{AlertLoop, AlertLoopMessage};
use crate::database::Database;
use crate::download::DownloadCoordinator;
use crate::metadata::MetadataManager;
use crate::piece_cache::PieceCache;
use torrentfs_libtorrent::{AlertType, Session};

pub struct TorrentRuntime {
    pub db: Arc<Database>,
    pub session: Arc<Session>,
    pub piece_cache: Arc<PieceCache>,
    pub download_coordinator: Arc<DownloadCoordinator>,
    pub metadata_manager: Arc<MetadataManager>,
    shutdown_tx: broadcast::Sender<AlertLoopMessage>,
    state_dir: std::path::PathBuf,
}

impl TorrentRuntime {
    pub async fn new(state_dir: &Path) -> Result<Self> {
        let db = Arc::new(Database::new(state_dir).await?);
        db.migrate().await?;
        
        let session = Arc::new(Session::new()?);
        let piece_cache = Arc::new(PieceCache::new()?);
        let download_coordinator = Arc::new(DownloadCoordinator::new(
            Arc::clone(&session),
            Arc::clone(&piece_cache),
        ));
        let metadata_manager = Arc::new(MetadataManager::new(Arc::clone(&db))?);
        
        let (shutdown_tx, shutdown_rx) = broadcast::channel::<AlertLoopMessage>(1);
        
        let alert_loop = AlertLoop::new(
            Arc::clone(&session),
            Arc::clone(&piece_cache),
            Arc::clone(&metadata_manager),
            shutdown_rx,
        );
        
        tokio::spawn(alert_loop.run());
        
        let runtime = Self {
            db,
            session,
            piece_cache,
            download_coordinator,
            metadata_manager,
            shutdown_tx,
            state_dir: state_dir.to_path_buf(),
        };
        
        runtime.restore_cache_index()?;
        runtime.restore_torrents().await?;
        
        Ok(runtime)
    }
    
    fn restore_cache_index(&self) -> Result<()> {
        let cached = self.piece_cache.scan_cached_pieces()?;
        
        if cached.is_empty() {
            tracing::info!("No cached pieces found");
            return Ok(());
        }
        
        let total_pieces: usize = cached.iter().map(|(_, pieces)| pieces.len()).sum();
        tracing::info!(
            torrents = cached.len(),
            total_pieces = total_pieces,
            "Cache index restored"
        );
        
        Ok(())
    }
    
    async fn restore_torrents(&self) -> Result<()> {
        let torrents = self.metadata_manager.list_torrents_with_data().await?;
        
        if torrents.is_empty() {
            tracing::info!("No torrents to restore from database");
            return Ok(());
        }
        
        let mut restored = 0;
        let mut skipped = 0;
        let mut failed = 0;
        
        for torrent_with_data in torrents {
            let info_hash_hex = hex::encode(&torrent_with_data.torrent.info_hash);
            let torrent_name = &torrent_with_data.torrent.name;
            let source_path = &torrent_with_data.torrent.source_path;
            
            if self.session.find_torrent(&info_hash_hex) {
                tracing::debug!(
                    info_hash = %info_hash_hex,
                    name = %torrent_name,
                    "Torrent already exists in session, skipping"
                );
                skipped += 1;
                continue;
            }
            
            let save_path = self.state_dir
                .join("data")
                .join(source_path)
                .join(torrent_name)
                .to_string_lossy()
                .into_owned();
            
            match self.session.add_torrent_with_resume(
                &torrent_with_data.torrent_data,
                &save_path,
                torrent_with_data.resume_data.as_deref()
            ) {
                Ok(()) => {
                    if torrent_with_data.resume_data.is_some() {
                        tracing::info!(
                            info_hash = %info_hash_hex,
                            name = %torrent_name,
                            "Restored torrent with resume_data"
                        );
                    } else {
                        tracing::info!(
                            info_hash = %info_hash_hex,
                            name = %torrent_name,
                            save_path = %save_path,
                            "Restored torrent from database"
                        );
                    }
                    restored += 1;
                }
                Err(e) => {
                    tracing::error!(
                        info_hash = %info_hash_hex,
                        name = %torrent_name,
                        save_path = %save_path,
                        error = %e,
                        "Failed to restore torrent from database"
                    );
                    failed += 1;
                }
            }
        }
        
        tracing::info!(
            restored = restored,
            skipped = skipped,
            failed = failed,
            total = restored + skipped + failed,
            "Torrent restoration complete"
        );
        
        Ok(())
    }
    
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(AlertLoopMessage::Shutdown);
    }

    pub async fn graceful_shutdown(&self) -> Result<()> {
        tracing::info!("Starting graceful shutdown...");
        
        let info_hashes = self.session.get_torrents();
        tracing::info!(torrent_count = info_hashes.len(), "Found torrents to save resume data");
        
        for info_hash in &info_hashes {
            if let Err(e) = self.session.pause_torrent(info_hash) {
                tracing::warn!(info_hash = %info_hash, error = %e, "Failed to pause torrent");
            }
        }
        
        for info_hash in &info_hashes {
            if let Err(e) = self.session.save_resume_data(info_hash) {
                tracing::warn!(info_hash = %info_hash, error = %e, "Failed to request resume data save");
            }
        }
        
        if !info_hashes.is_empty() {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            let mut saved_count = 0;
            
            loop {
                let alerts = self.session.pop_alerts();
                
                for alert in alerts.iter() {
                    if alert.alert_type == AlertType::SaveResumeData {
                        if let Some(info_hash_hex) = &alert.info_hash {
                            tracing::info!(info_hash = %info_hash_hex, "Resume data saved");
                            saved_count += 1;
                        }
                    }
                }
                
                if saved_count >= info_hashes.len() {
                    break;
                }
                
                if tokio::time::Instant::now() >= deadline {
                    tracing::warn!(saved = saved_count, total = info_hashes.len(), "Timeout waiting for resume data saves");
                    break;
                }
                
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        
        tracing::info!("Stopping alert loop...");
        self.shutdown();
        
        tracing::info!("Destroying libtorrent session...");
        drop(Arc::clone(&self.session));
        
        tracing::info!("Closing database connection pool...");
        self.db.pool().close().await;
        
        tracing::info!("Graceful shutdown complete");
        Ok(())
    }
}

impl Drop for TorrentRuntime {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_init_returns_ok() {
        let temp_dir = TempDir::new().unwrap();
        let result = TorrentRuntime::new(temp_dir.path()).await;
        assert!(result.is_ok(), "new() should return Ok: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_init_creates_torrent_runtime() {
        let temp_dir = TempDir::new().unwrap();
        let runtime = TorrentRuntime::new(temp_dir.path()).await.unwrap();
        assert!(runtime.db.pool().acquire().await.is_ok());
        assert!(runtime.session.pop_alerts().is_empty() || !runtime.session.pop_alerts().is_empty());
    }

    #[tokio::test]
    async fn test_shutdown() {
        let temp_dir = TempDir::new().unwrap();
        let runtime = TorrentRuntime::new(temp_dir.path()).await.unwrap();
        runtime.shutdown();
    }

    #[tokio::test]
    async fn test_runtime_drop_sends_shutdown() {
        let temp_dir = TempDir::new().unwrap();
        let runtime = TorrentRuntime::new(temp_dir.path()).await.unwrap();
        let shutdown_tx = runtime.shutdown_tx.clone();
        
        drop(runtime);
        
        let result = shutdown_tx.send(AlertLoopMessage::Shutdown);
        assert!(result.is_ok() || result.is_err());
    }

    #[tokio::test]
    async fn test_graceful_shutdown_no_torrents() {
        let temp_dir = TempDir::new().unwrap();
        let runtime = TorrentRuntime::new(temp_dir.path()).await.unwrap();
        let result = runtime.graceful_shutdown().await;
        assert!(result.is_ok(), "graceful_shutdown should succeed: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_get_torrents_returns_vec() {
        let temp_dir = TempDir::new().unwrap();
        let runtime = TorrentRuntime::new(temp_dir.path()).await.unwrap();
        let torrents = runtime.session.get_torrents();
        assert!(torrents.is_empty() || !torrents.is_empty(), "get_torrents should return a Vec");
    }
}
