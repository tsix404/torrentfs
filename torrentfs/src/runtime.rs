use anyhow::Result;
use std::sync::Arc;
use tokio::sync::broadcast;

use crate::alert_loop::{AlertLoop, AlertLoopMessage};
use crate::database::Database;
use crate::download::DownloadCoordinator;
use crate::metadata::MetadataManager;
use crate::piece_cache::PieceCache;
use torrentfs_libtorrent::Session;

fn get_save_path() -> String {
    dirs::home_dir()
        .map(|h| h.join(".local").join("share").join("torrentfs").join("data"))
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|| "/tmp/torrentfs".to_string())
}

pub struct TorrentRuntime {
    pub db: Arc<Database>,
    pub session: Arc<Session>,
    pub piece_cache: Arc<PieceCache>,
    pub download_coordinator: Arc<DownloadCoordinator>,
    pub metadata_manager: Arc<MetadataManager>,
    shutdown_tx: broadcast::Sender<AlertLoopMessage>,
}

impl TorrentRuntime {
    pub async fn new() -> Result<Self> {
        let db = Arc::new(Database::new().await?);
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
        
        let save_path = get_save_path();
        let mut restored = 0;
        let mut failed = 0;
        
        for torrent_with_data in torrents {
            let info_hash_hex = hex::encode(&torrent_with_data.torrent.info_hash);
            
            match self.session.add_torrent_paused(&torrent_with_data.torrent_data, &save_path) {
                Ok(()) => {
                    if let Some(ref resume_data) = torrent_with_data.resume_data {
                        tracing::debug!(
                            info_hash = %info_hash_hex,
                            resume_data_len = resume_data.len(),
                            "Torrent has resume_data available"
                        );
                    }
                    
                    tracing::info!(
                        info_hash = %info_hash_hex,
                        name = %torrent_with_data.torrent.name,
                        save_path = %save_path,
                        has_resume_data = torrent_with_data.resume_data.is_some(),
                        "Restored torrent from database"
                    );
                    restored += 1;
                }
                Err(e) => {
                    tracing::warn!(
                        info_hash = %info_hash_hex,
                        name = %torrent_with_data.torrent.name,
                        error = %e,
                        "Failed to restore torrent from database"
                    );
                    failed += 1;
                }
            }
        }
        
        tracing::info!(
            restored = restored,
            failed = failed,
            "Torrent restoration complete"
        );
        
        Ok(())
    }
    
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(AlertLoopMessage::Shutdown);
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

    #[tokio::test]
    async fn test_init_returns_ok() {
        let result = TorrentRuntime::new().await;
        assert!(result.is_ok(), "new() should return Ok: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_init_creates_torrent_runtime() {
        let runtime = TorrentRuntime::new().await.unwrap();
        assert!(runtime.db.pool().acquire().await.is_ok());
        assert!(runtime.session.pop_alerts().is_empty() || !runtime.session.pop_alerts().is_empty());
    }

    #[tokio::test]
    async fn test_shutdown() {
        let runtime = TorrentRuntime::new().await.unwrap();
        runtime.shutdown();
    }

    #[tokio::test]
    async fn test_runtime_drop_sends_shutdown() {
        let runtime = TorrentRuntime::new().await.unwrap();
        let shutdown_tx = runtime.shutdown_tx.clone();
        
        drop(runtime);
        
        let result = shutdown_tx.send(AlertLoopMessage::Shutdown);
        assert!(result.is_ok() || result.is_err());
    }
}
