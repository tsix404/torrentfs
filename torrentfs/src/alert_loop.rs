use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;
use torrentfs_libtorrent::{Alert, AlertType, Session};

use crate::piece_cache::PieceCache;

const ALERT_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub enum AlertLoopMessage {
    Shutdown,
}

pub struct AlertLoop {
    session: Arc<Session>,
    _piece_cache: Arc<PieceCache>,
    shutdown_rx: broadcast::Receiver<AlertLoopMessage>,
}

unsafe impl Send for AlertLoop {}

impl AlertLoop {
    pub fn new(
        session: Arc<Session>,
        piece_cache: Arc<PieceCache>,
        shutdown_rx: broadcast::Receiver<AlertLoopMessage>,
    ) -> Self {
        Self {
            session,
            _piece_cache: piece_cache,
            shutdown_rx,
        }
    }

    pub async fn run(mut self) {
        tracing::info!("Alert loop started");
        
        loop {
            let session = Arc::clone(&self.session);
            let has_alert = tokio::task::spawn_blocking(move || {
                session.wait_for_alert(ALERT_WAIT_TIMEOUT)
            }).await.unwrap_or(false);
            
            if has_alert {
                let session = Arc::clone(&self.session);
                let alerts: Vec<Alert> = tokio::task::spawn_blocking(move || {
                    session.pop_alerts().iter().collect()
                }).await.unwrap_or_default();
                
                for alert in &alerts {
                    self.handle_alert(alert).await;
                }
            }
            
            if let Ok(AlertLoopMessage::Shutdown) = self.shutdown_rx.try_recv() {
                tracing::info!("Alert loop received shutdown signal, exiting");
                break;
            }
            
            tokio::task::yield_now().await;
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

    async fn handle_piece_finished(&self, alert: &Alert) {
        if let Some(info_hash) = &alert.info_hash {
            tracing::info!(
                info_hash = %info_hash,
                piece_index = alert.piece_index,
                "Piece finished downloading"
            );
            
            // TODO: In MVP-5, this will:
            // 1. Read piece data from torrent handle
            // 2. Write to PieceCache
            // 3. Notify any waiting oneshot channels
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
            
            // TODO: Update database status to "completed"
        } else {
            tracing::info!(
                "Torrent download completed: {}",
                alert.message
            );
        }
    }

    async fn handle_save_resume_data(&self, alert: &Alert) {
        if let Some(info_hash) = &alert.info_hash {
            tracing::debug!(
                info_hash = %info_hash,
                "Save resume data: {}",
                alert.message
            );
            
            // TODO: Persist resume data to ~/.local/share/torrentfs/state/resume/
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
    use tokio::sync::broadcast;

    #[tokio::test]
    async fn test_alert_loop_can_be_created() {
        let session = Arc::new(Session::new().expect("Failed to create session"));
        let piece_cache = Arc::new(
            PieceCache::new().expect("Failed to create piece cache")
        );
        let (_tx, rx) = broadcast::channel::<AlertLoopMessage>(1);
        
        let _alert_loop = AlertLoop::new(session, piece_cache, rx);
    }

    #[tokio::test]
    async fn test_alert_loop_shutdown() {
        let session = Arc::new(Session::new().expect("Failed to create session"));
        let piece_cache = Arc::new(
            PieceCache::new().expect("Failed to create piece cache")
        );
        let (tx, rx) = broadcast::channel::<AlertLoopMessage>(1);
        
        let alert_loop = AlertLoop::new(session, piece_cache, rx);
        
        tx.send(AlertLoopMessage::Shutdown).expect("Failed to send shutdown");
        
        let handle = tokio::spawn(alert_loop.run());
        
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("Alert loop should exit quickly")
            .expect("Alert loop task should complete successfully");
    }
}
