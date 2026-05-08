use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::broadcast;
use torrentfs_libtorrent::{Alert, AlertType, Session};

use crate::metadata::MetadataManager;
use crate::piece_cache::PieceCache;

const ALERT_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub enum AlertLoopMessage {
    Shutdown,
}

pub struct AlertLoop {
    session: Arc<Session>,
    piece_cache: Arc<PieceCache>,
    metadata_manager: Arc<MetadataManager>,
    shutdown_rx: broadcast::Receiver<AlertLoopMessage>,
    downloaded_pieces: Arc<DashMap<String, HashSet<u32>>>,
}

unsafe impl Send for AlertLoop {}

impl AlertLoop {
    pub fn new(
        session: Arc<Session>,
        piece_cache: Arc<PieceCache>,
        metadata_manager: Arc<MetadataManager>,
        shutdown_rx: broadcast::Receiver<AlertLoopMessage>,
    ) -> Self {
        Self {
            session,
            piece_cache,
            metadata_manager,
            shutdown_rx,
            downloaded_pieces: Arc::new(DashMap::new()),
        }
    }

    pub async fn run(mut self) {
        tracing::info!("Alert loop started");
        
        loop {
            if let Ok(AlertLoopMessage::Shutdown) = self.shutdown_rx.try_recv() {
                tracing::info!("Alert loop received shutdown signal, exiting");
                break;
            }
            
            let session = Arc::clone(&self.session);
            let has_alert = match tokio::task::spawn_blocking(move || {
                session.wait_for_alert(ALERT_WAIT_TIMEOUT)
            }).await {
                Ok(result) => result,
                Err(e) => {
                    tracing::error!("Alert wait task failed: {}, retrying", e);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            
            if has_alert {
                let session = Arc::clone(&self.session);
                let alerts: Vec<Alert> = match tokio::task::spawn_blocking(move || {
                    session.pop_alerts().iter().collect()
                }).await {
                    Ok(alerts) => alerts,
                    Err(e) => {
                        tracing::error!("Alert pop task failed: {}, retrying", e);
                        continue;
                    }
                };
                
                for alert in &alerts {
                    self.handle_alert(alert).await;
                }
            }
        }
        
        tracing::info!("Alert loop stopped");
    }

    async fn handle_alert(&self, alert: &Alert) {
        match alert.alert_type {
            AlertType::PieceFinished => {
                self.handle_piece_finished(alert).await;
            }
            AlertType::TorrentFinished => {
                self.handle_torrent_finished(alert).await;
            }
            AlertType::SaveResumeData => {
                self.handle_save_resume_data(alert).await;
            }
            AlertType::AddTorrent => {
                tracing::debug!(
                    info_hash = ?alert.info_hash,
                    "Torrent added: {}",
                    alert.message
                );
            }
            AlertType::MetadataReceived => {
                tracing::debug!(
                    info_hash = ?alert.info_hash,
                    "Metadata received: {}",
                    alert.message
                );
            }
            AlertType::PieceRead => {
                tracing::trace!(
                    info_hash = ?alert.info_hash,
                    piece_index = alert.piece_index,
                    "Piece read: {}",
                    alert.message
                );
            }
            AlertType::Unknown => {
                tracing::debug!(
                    type_name = %alert.type_name,
                    "Unknown alert: {}",
                    alert.message
                );
            }
        }
    }

    pub fn get_downloaded_pieces(&self, info_hash: &str) -> Vec<u32> {
        if let Some(pieces) = self.downloaded_pieces.get(info_hash) {
            let mut result: Vec<u32> = pieces.iter().copied().collect();
            result.sort();
            result
        } else {
            Vec::new()
        }
    }
    
    async fn handle_piece_finished(&self, alert: &Alert) {
        if let Some(info_hash) = &alert.info_hash {
            let piece_index = alert.piece_index;
            
            tracing::info!(
                info_hash = %info_hash,
                piece_index = piece_index,
                "Piece finished downloading, reading data"
            );
            
            let info_hash_clone = info_hash.clone();
            let session = Arc::clone(&self.session);
            
            match tokio::task::spawn_blocking(move || {
                session.read_piece(&info_hash_clone, piece_index)
            }).await {
                Ok(Ok(data)) => {
                    match self.piece_cache.write_piece(info_hash, piece_index, &data) {
                        Ok(()) => {
                            self.downloaded_pieces
                                .entry(info_hash.clone())
                                .or_insert_with(HashSet::new)
                                .insert(piece_index);
                            
                            tracing::info!(
                                info_hash = %info_hash,
                                piece_index = piece_index,
                                size = data.len(),
                                "Piece cached successfully"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                info_hash = %info_hash,
                                piece_index = piece_index,
                                error = %e,
                                "Failed to cache piece"
                            );
                        }
                    }
                }
                Ok(Err(e)) => {
                    tracing::warn!(
                        info_hash = %info_hash,
                        piece_index = piece_index,
                        error = %e,
                        "Failed to read piece data"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        info_hash = %info_hash,
                        piece_index = piece_index,
                        error = %e,
                        "Spawn blocking task failed"
                    );
                }
            }
        } else {
            tracing::debug!(
                piece_index = alert.piece_index,
                "Piece finished (no info_hash): {}",
                alert.message
            );
        }
    }

    async fn handle_torrent_finished(&self, alert: &Alert) {
        if let Some(info_hash) = &alert.info_hash {
            tracing::info!(
                info_hash = %info_hash,
                "Torrent download completed"
            );
            
            if !self.session.is_seeding(info_hash) {
                match self.session.resume_torrent(info_hash) {
                    Ok(()) => {
                        tracing::info!(
                            info_hash = %info_hash,
                            "Auto-seeding started after download completion"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            info_hash = %info_hash,
                            error = %e,
                            "Failed to start auto-seeding"
                        );
                    }
                }
            }
            
            if self.session.is_seeding(info_hash) {
                tracing::info!(
                    info_hash = %info_hash,
                    "Torrent is now seeding"
                );
            }
        } else {
            tracing::info!(
                "Torrent download completed: {}",
                alert.message
            );
        }
    }

    async fn handle_save_resume_data(&self, alert: &Alert) {
        if let Some(info_hash_hex) = &alert.info_hash {
            tracing::debug!(
                info_hash = %info_hash_hex,
                "Save resume data alert received"
            );
            
            if let Ok(info_hash_bytes) = hex::decode(info_hash_hex) {
                match self.metadata_manager.update_resume_data(&info_hash_bytes, alert.message.as_bytes()).await {
                    Ok(()) => {
                        tracing::info!(
                            info_hash = %info_hash_hex,
                            resume_data_len = alert.message.len(),
                            "Resume data persisted to database"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            info_hash = %info_hash_hex,
                            error = %e,
                            "Failed to persist resume data"
                        );
                    }
                }
            }
        } else {
            tracing::debug!(
                "Save resume data: {}",
                alert.message
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Database;
    use tokio::sync::broadcast;
    use tempfile::TempDir;
    use sqlx::sqlite::SqliteConnectOptions;
    use sqlx::SqlitePool;
    use std::str::FromStr;

    async fn setup_test_db() -> (TempDir, Database) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let options = SqliteConnectOptions::from_str(&db_path.to_string_lossy())
            .unwrap()
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(options).await.unwrap();
        let db = Database::with_pool(pool);
        db.migrate().await.unwrap();
        (temp_dir, db)
    }

    #[tokio::test]
    async fn test_alert_loop_can_be_created() {
        let session = Arc::new(Session::new().expect("Failed to create session"));
        let piece_cache = Arc::new(
            PieceCache::new().expect("Failed to create piece cache")
        );
        let (_temp_dir, db) = setup_test_db().await;
        let metadata_manager = Arc::new(MetadataManager::new(Arc::new(db)).expect("Failed to create metadata manager"));
        let (_tx, rx) = broadcast::channel::<AlertLoopMessage>(1);
        
        let _alert_loop = AlertLoop::new(session, piece_cache, metadata_manager, rx);
    }

    #[tokio::test]
    async fn test_alert_loop_shutdown() {
        let session = Arc::new(Session::new().expect("Failed to create session"));
        let piece_cache = Arc::new(
            PieceCache::new().expect("Failed to create piece cache")
        );
        let (_temp_dir, db) = setup_test_db().await;
        let metadata_manager = Arc::new(MetadataManager::new(Arc::new(db)).expect("Failed to create metadata manager"));
        let (tx, rx) = broadcast::channel::<AlertLoopMessage>(1);
        
        let alert_loop = AlertLoop::new(session, piece_cache, metadata_manager, rx);
        
        let handle = tokio::spawn(alert_loop.run());
        
        tokio::time::sleep(Duration::from_millis(100)).await;
        tx.send(AlertLoopMessage::Shutdown).expect("Failed to send shutdown");
        
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("Alert loop should exit within timeout")
            .expect("Alert loop task should complete successfully");
    }
}
