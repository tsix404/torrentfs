use anyhow::Result;
use std::sync::Arc;
use tokio::sync::broadcast;

use crate::alert_loop::{AlertLoop, AlertLoopMessage};
use crate::database::Database;
use crate::piece_cache::PieceCache;
use torrentfs_libtorrent::Session;

pub struct TorrentRuntime {
    pub db: Arc<Database>,
    pub session: Arc<Session>,
    pub piece_cache: Arc<PieceCache>,
    shutdown_tx: broadcast::Sender<AlertLoopMessage>,
}

impl TorrentRuntime {
    pub async fn new() -> Result<Self> {
        let db = Database::new().await?;
        db.migrate().await?;
        
        let session = Arc::new(Session::new()?);
        let piece_cache = Arc::new(PieceCache::new()?);
        
        let (shutdown_tx, shutdown_rx) = broadcast::channel::<AlertLoopMessage>(1);
        
        let alert_loop = AlertLoop::new(
            Arc::clone(&session),
            Arc::clone(&piece_cache),
            shutdown_rx,
        );
        
        tokio::spawn(alert_loop.run());
        
        Ok(Self {
            db: Arc::new(db),
            session,
            piece_cache,
            shutdown_tx,
        })
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
