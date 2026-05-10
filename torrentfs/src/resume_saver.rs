use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;
use torrentfs_libtorrent::Session;

use crate::alert_loop::AlertLoopMessage;

const DEFAULT_SAVE_INTERVAL_SECS: u64 = 300;

#[derive(Debug, Clone)]
pub struct ResumeSaverConfig {
    pub save_interval: Duration,
}

impl Default for ResumeSaverConfig {
    fn default() -> Self {
        Self {
            save_interval: Duration::from_secs(DEFAULT_SAVE_INTERVAL_SECS),
        }
    }
}

impl ResumeSaverConfig {
    pub fn new(save_interval: Duration) -> Self {
        Self { save_interval }
    }

    pub fn from_secs(secs: u64) -> Self {
        Self {
            save_interval: Duration::from_secs(secs),
        }
    }
}

pub struct ResumeSaver {
    session: Arc<Session>,
    config: ResumeSaverConfig,
    shutdown_rx: broadcast::Receiver<AlertLoopMessage>,
}

unsafe impl Send for ResumeSaver {}
unsafe impl Sync for ResumeSaver {}

impl ResumeSaver {
    pub fn new(
        session: Arc<Session>,
        config: ResumeSaverConfig,
        shutdown_rx: broadcast::Receiver<AlertLoopMessage>,
    ) -> Self {
        Self {
            session,
            config,
            shutdown_rx,
        }
    }

    pub async fn run(mut self) {
        tracing::info!(
            interval_secs = self.config.save_interval.as_secs(),
            "Resume saver started with periodic save interval"
        );

        let mut interval = tokio::time::interval(self.config.save_interval);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.save_all_resume_data().await;
                }

                Ok(AlertLoopMessage::Shutdown) = self.shutdown_rx.recv() => {
                    tracing::info!("Resume saver received shutdown signal, performing final save");
                    self.save_all_resume_data().await;
                    tracing::info!("Resume saver stopped");
                    break;
                }
            }
        }
    }

    async fn save_all_resume_data(&self) {
        let session = Arc::clone(&self.session);
        let info_hashes: Vec<String> = match tokio::task::spawn_blocking(move || {
            session.get_torrents()
        }).await {
            Ok(hashes) => hashes,
            Err(e) => {
                tracing::error!("Failed to get torrent list: {}", e);
                return;
            }
        };

        if info_hashes.is_empty() {
            tracing::debug!("No torrents to save resume data");
            return;
        }

        tracing::info!(
            torrent_count = info_hashes.len(),
            "Starting periodic resume data save"
        );

        let mut saved_count = 0;
        let mut failed_count = 0;

        for info_hash in &info_hashes {
            let session = Arc::clone(&self.session);
            let info_hash_clone = info_hash.clone();
            
            let result = tokio::task::spawn_blocking(move || {
                session.save_resume_data(&info_hash_clone)
            }).await;

            match result {
                Ok(Ok(())) => {
                    saved_count += 1;
                    tracing::trace!(info_hash = %info_hash, "Requested resume data save");
                }
                Ok(Err(e)) => {
                    failed_count += 1;
                    tracing::warn!(info_hash = %info_hash, error = %e, "Failed to request resume data save");
                }
                Err(e) => {
                    failed_count += 1;
                    tracing::error!(info_hash = %info_hash, error = %e, "Spawn blocking task failed");
                }
            }
        }

        tracing::info!(
            total = info_hashes.len(),
            saved = saved_count,
            failed = failed_count,
            "Periodic resume data save completed"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::broadcast;

    #[test]
    fn test_config_default() {
        let config = ResumeSaverConfig::default();
        assert_eq!(config.save_interval, Duration::from_secs(300));
    }

    #[test]
    fn test_config_from_secs() {
        let config = ResumeSaverConfig::from_secs(60);
        assert_eq!(config.save_interval, Duration::from_secs(60));
    }

    #[test]
    fn test_config_new() {
        let config = ResumeSaverConfig::new(Duration::from_secs(120));
        assert_eq!(config.save_interval, Duration::from_secs(120));
    }

    #[tokio::test]
    async fn test_resume_saver_can_be_created() {
        let session = Arc::new(Session::new().expect("Failed to create session"));
        let config = ResumeSaverConfig::default();
        let (_tx, rx) = broadcast::channel::<AlertLoopMessage>(1);

        let _saver = ResumeSaver::new(session, config, rx);
    }

    #[tokio::test]
    async fn test_resume_saver_shutdown() {
        let session = Arc::new(Session::new().expect("Failed to create session"));
        let config = ResumeSaverConfig::from_secs(1);
        let (tx, rx) = broadcast::channel::<AlertLoopMessage>(1);

        let saver = ResumeSaver::new(session, config, rx);
        
        let handle = tokio::spawn(saver.run());

        tokio::time::sleep(Duration::from_millis(50)).await;
        tx.send(AlertLoopMessage::Shutdown).expect("Failed to send shutdown");

        let result = tokio::time::timeout(Duration::from_secs(5), handle).await;
        assert!(result.is_ok(), "Resume saver should exit within timeout");
    }

    #[tokio::test]
    async fn test_resume_saver_no_torrents() {
        let session = Arc::new(Session::new().expect("Failed to create session"));
        let config = ResumeSaverConfig::from_secs(1);
        let (tx, rx) = broadcast::channel::<AlertLoopMessage>(1);

        let saver = ResumeSaver::new(session, config, rx);
        
        let handle = tokio::spawn(saver.run());

        tokio::time::sleep(Duration::from_millis(100)).await;
        tx.send(AlertLoopMessage::Shutdown).expect("Failed to send shutdown");

        let result = tokio::time::timeout(Duration::from_secs(5), handle).await;
        assert!(result.is_ok(), "Resume saver should exit cleanly with no torrents");
    }
}
