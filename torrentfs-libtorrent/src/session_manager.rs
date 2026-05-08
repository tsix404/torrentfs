use crate::alert::{Alert, AlertType};
use crate::session::Session;
use crate::torrent::TorrentInfo;

use anyhow::{bail, Result};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, info};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TorrentStatus {
    Pending,
    Downloading,
    Seeding,
    Paused,
    Error,
    Removed,
}

impl Default for TorrentStatus {
    fn default() -> Self {
        TorrentStatus::Pending
    }
}

#[derive(Debug, Clone)]
pub struct TorrentProgress {
    pub info_hash: String,
    pub status: TorrentStatus,
    pub total_size: u64,
    pub downloaded: u64,
    pub uploaded: u64,
    pub download_rate: u64,
    pub upload_rate: u64,
    pub progress: f32,
    pub num_peers: u32,
    pub num_seeds: u32,
}

impl Default for TorrentProgress {
    fn default() -> Self {
        Self {
            info_hash: String::new(),
            status: TorrentStatus::Pending,
            total_size: 0,
            downloaded: 0,
            uploaded: 0,
            download_rate: 0,
            upload_rate: 0,
            progress: 0.0,
            num_peers: 0,
            num_seeds: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub ip: String,
    pub port: u16,
    pub client: Option<String>,
    pub progress: f32,
    pub is_seed: bool,
}

#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub listen_port: u16,
    pub download_rate_limit: Option<u64>,
    pub upload_rate_limit: Option<u64>,
    pub max_connections: u32,
    pub max_uploads: u32,
    pub active_downloads: u32,
    pub active_seeds: u32,
    pub alert_mask: u64,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            listen_port: 6881,
            download_rate_limit: None,
            upload_rate_limit: None,
            max_connections: 100,
            max_uploads: 4,
            active_downloads: 4,
            active_seeds: 4,
            alert_mask: 0x7FFFFFFF,
        }
    }
}

#[derive(Debug, Clone)]
pub enum SessionEvent {
    TorrentAdded {
        info_hash: String,
        name: String,
    },
    TorrentRemoved {
        info_hash: String,
    },
    TorrentPaused {
        info_hash: String,
    },
    TorrentResumed {
        info_hash: String,
    },
    TorrentFinished {
        info_hash: String,
    },
    ProgressUpdate {
        progress: TorrentProgress,
    },
    PeerConnected {
        info_hash: String,
        peer: PeerInfo,
    },
    PeerDisconnected {
        info_hash: String,
        peer_ip: String,
    },
    Error {
        info_hash: Option<String>,
        message: String,
    },
}

pub type EventCallback = Box<dyn Fn(SessionEvent) + Send + Sync>;

pub struct SessionManager {
    session: Arc<Session>,
    config: SessionConfig,
    torrents: Arc<RwLock<HashMap<String, TorrentEntry>>>,
    event_tx: broadcast::Sender<SessionEvent>,
    shutdown_tx: broadcast::Sender<()>,
}

#[derive(Debug)]
struct TorrentEntry {
    info_hash: String,
    name: String,
    status: TorrentStatus,
    save_path: String,
    torrent_data: Vec<u8>,
}

impl SessionManager {
    pub fn new(config: SessionConfig) -> Result<Self> {
        Self::with_session(Arc::new(Session::new()?), config)
    }

    pub fn with_session(session: Arc<Session>, config: SessionConfig) -> Result<Self> {
        session.set_alert_mask(config.alert_mask);
        
        let (event_tx, _) = broadcast::channel(256);
        let (shutdown_tx, _) = broadcast::channel(1);
        
        Ok(Self {
            session,
            config,
            torrents: Arc::new(RwLock::new(HashMap::new())),
            event_tx,
            shutdown_tx,
        })
    }

    pub async fn add_torrent(
        &self,
        torrent_data: Vec<u8>,
        save_path: &str,
        paused: bool,
    ) -> Result<String> {
        let info = crate::torrent::parse_torrent(&torrent_data)?;
        let info_hash = info.info_hash.clone();
        let name = info.name.clone();
        
        self.session.add_torrent_paused(&torrent_data, save_path)?;
        
        if !paused {
            if !self.session.find_torrent(&info_hash) {
                bail!("Failed to add torrent to session");
            }
            self.session.resume_torrent(&info_hash)?;
        }
        
        let entry = TorrentEntry {
            info_hash: info_hash.clone(),
            name: name.clone(),
            status: if paused { TorrentStatus::Paused } else { TorrentStatus::Downloading },
            save_path: save_path.to_string(),
            torrent_data,
        };
        
        {
            let mut torrents = self.torrents.write().await;
            torrents.insert(info_hash.clone(), entry);
        }
        
        let _ = self.event_tx.send(SessionEvent::TorrentAdded {
            info_hash: info_hash.clone(),
            name,
        });
        
        info!(info_hash = %info_hash, "Torrent added successfully");
        Ok(info_hash)
    }

    pub async fn add_torrent_with_resume(
        &self,
        torrent_data: Vec<u8>,
        save_path: &str,
        resume_data: Option<&[u8]>,
    ) -> Result<String> {
        let info = crate::torrent::parse_torrent(&torrent_data)?;
        let info_hash = info.info_hash.clone();
        let name = info.name.clone();
        
        self.session.add_torrent_with_resume(&torrent_data, save_path, resume_data)?;
        
        let entry = TorrentEntry {
            info_hash: info_hash.clone(),
            name: name.clone(),
            status: TorrentStatus::Pending,
            save_path: save_path.to_string(),
            torrent_data,
        };
        
        {
            let mut torrents = self.torrents.write().await;
            torrents.insert(info_hash.clone(), entry);
        }
        
        let _ = self.event_tx.send(SessionEvent::TorrentAdded {
            info_hash: info_hash.clone(),
            name,
        });
        
        info!(info_hash = %info_hash, "Torrent added with resume data");
        Ok(info_hash)
    }

    pub async fn remove_torrent(&self, info_hash: &str) -> Result<()> {
        {
            let torrents = self.torrents.read().await;
            if !torrents.contains_key(info_hash) {
                bail!("Torrent not found: {}", info_hash);
            }
        }
        
        {
            let mut torrents = self.torrents.write().await;
            if let Some(mut entry) = torrents.remove(info_hash) {
                entry.status = TorrentStatus::Removed;
            }
        }
        
        let _ = self.event_tx.send(SessionEvent::TorrentRemoved {
            info_hash: info_hash.to_string(),
        });
        
        info!(info_hash = %info_hash, "Torrent removed");
        Ok(())
    }

    pub async fn pause_torrent(&self, info_hash: &str) -> Result<()> {
        self.session.pause_torrent(info_hash)?;
        
        {
            let mut torrents = self.torrents.write().await;
            if let Some(entry) = torrents.get_mut(info_hash) {
                entry.status = TorrentStatus::Paused;
            }
        }
        
        let _ = self.event_tx.send(SessionEvent::TorrentPaused {
            info_hash: info_hash.to_string(),
        });
        
        debug!(info_hash = %info_hash, "Torrent paused");
        Ok(())
    }

    pub async fn resume_torrent(&self, info_hash: &str) -> Result<()> {
        self.session.resume_torrent(info_hash)?;
        
        {
            let mut torrents = self.torrents.write().await;
            if let Some(entry) = torrents.get_mut(info_hash) {
                entry.status = TorrentStatus::Downloading;
            }
        }
        
        let _ = self.event_tx.send(SessionEvent::TorrentResumed {
            info_hash: info_hash.to_string(),
        });
        
        debug!(info_hash = %info_hash, "Torrent resumed");
        Ok(())
    }

    pub async fn get_torrent_status(&self, info_hash: &str) -> Option<TorrentStatus> {
        let torrents = self.torrents.read().await;
        torrents.get(info_hash).map(|e| e.status)
    }

    pub async fn get_torrent_progress(&self, info_hash: &str) -> Option<TorrentProgress> {
        let torrents = self.torrents.read().await;
        let entry = torrents.get(info_hash)?;
        
        let is_seeding = self.session.is_seeding(info_hash);
        
        Some(TorrentProgress {
            info_hash: info_hash.to_string(),
            status: if is_seeding { TorrentStatus::Seeding } else { entry.status },
            total_size: 0,
            downloaded: 0,
            uploaded: 0,
            download_rate: 0,
            upload_rate: 0,
            progress: if is_seeding { 1.0 } else { 0.0 },
            num_peers: 0,
            num_seeds: 0,
        })
    }

    pub async fn list_torrents(&self) -> Vec<String> {
        let torrents = self.torrents.read().await;
        torrents.keys().cloned().collect()
    }

    pub async fn list_torrents_with_status(&self) -> Vec<(String, TorrentStatus)> {
        let torrents = self.torrents.read().await;
        torrents.iter().map(|(k, v)| (k.clone(), v.status)).collect()
    }

    pub fn is_seeding(&self, info_hash: &str) -> bool {
        self.session.is_seeding(info_hash)
    }

    pub fn find_torrent(&self, info_hash: &str) -> bool {
        self.session.find_torrent(info_hash)
    }

    pub fn save_resume_data(&self, info_hash: &str) -> Result<()> {
        self.session.save_resume_data(info_hash)
    }

    pub fn set_piece_deadline(&self, info_hash: &str, piece_index: u32, deadline: Duration) -> Result<()> {
        self.session.set_piece_deadline(info_hash, piece_index, deadline.as_millis() as i32)
    }

    pub fn read_piece(&self, info_hash: &str, piece_index: u32) -> Result<Vec<u8>> {
        self.session.read_piece(info_hash, piece_index)
    }

    pub fn pop_alerts(&self) -> Vec<Alert> {
        let alert_list = self.session.pop_alerts();
        alert_list.iter().collect()
    }

    pub fn wait_for_alert(&self, timeout: Duration) -> bool {
        self.session.wait_for_alert(timeout)
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<SessionEvent> {
        self.event_tx.subscribe()
    }

    pub async fn process_alerts(&self) {
        let alerts = self.pop_alerts();
        
        for alert in alerts {
            match alert.alert_type {
                AlertType::TorrentFinished => {
                    if let Some(ref info_hash) = alert.info_hash {
                        {
                            let mut torrents = self.torrents.write().await;
                            if let Some(entry) = torrents.get_mut(info_hash) {
                                entry.status = TorrentStatus::Seeding;
                            }
                        }
                        
                        let _ = self.event_tx.send(SessionEvent::TorrentFinished {
                            info_hash: info_hash.clone(),
                        });
                        
                        info!(info_hash = %info_hash, "Torrent download finished");
                    }
                }
                AlertType::PieceFinished => {
                    if let Some(ref info_hash) = alert.info_hash {
                        debug!(
                            info_hash = %info_hash,
                            piece = alert.piece_index,
                            "Piece finished downloading"
                        );
                    }
                }
                AlertType::SaveResumeData => {
                    if let Some(ref info_hash) = alert.info_hash {
                        debug!(info_hash = %info_hash, "Resume data saved");
                    }
                }
                AlertType::AddTorrent => {
                    if let Some(ref info_hash) = alert.info_hash {
                        debug!(info_hash = %info_hash, "Torrent added to session");
                    }
                }
                AlertType::MetadataReceived => {
                    if let Some(ref info_hash) = alert.info_hash {
                        debug!(info_hash = %info_hash, "Metadata received");
                    }
                }
                AlertType::PieceRead => {
                    if let Some(ref info_hash) = alert.info_hash {
                        debug!(
                            info_hash = %info_hash,
                            piece = alert.piece_index,
                            "Piece read completed"
                        );
                    }
                }
                AlertType::Unknown => {
                    debug!(message = %alert.message, "Unknown alert");
                }
            }
        }
    }

    pub async fn run_alert_loop(&self) {
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Alert loop shutting down");
                    break;
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    self.process_alerts().await;
                }
            }
        }
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
        info!("Session manager shutdown initiated");
    }

    pub fn get_config(&self) -> &SessionConfig {
        &self.config
    }

    pub async fn update_config(&mut self, config: SessionConfig) {
        self.config = config;
        self.session.set_alert_mask(self.config.alert_mask);
    }

    pub async fn get_torrent_count(&self) -> usize {
        let torrents = self.torrents.read().await;
        torrents.len()
    }

    pub async fn get_active_downloads(&self) -> usize {
        let torrents = self.torrents.read().await;
        torrents.values()
            .filter(|e| e.status == TorrentStatus::Downloading)
            .count()
    }

    pub async fn get_seeding_count(&self) -> usize {
        let torrents = self.torrents.read().await;
        torrents.values()
            .filter(|e| e.status == TorrentStatus::Seeding)
            .count()
    }

    pub async fn get_torrent_info(&self, info_hash: &str) -> Option<TorrentInfo> {
        let torrents = self.torrents.read().await;
        let entry = torrents.get(info_hash)?;
        crate::torrent::parse_torrent(&entry.torrent_data).ok()
    }
}

impl Drop for SessionManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn test_torrent_path() -> std::path::PathBuf {
        let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        manifest_dir.join("../test_data/test.torrent")
    }

    #[tokio::test]
    async fn test_session_manager_creation() {
        let config = SessionConfig::default();
        let manager = SessionManager::new(config);
        assert!(manager.is_ok(), "Failed to create session manager: {:?}", manager.err());
    }

    #[tokio::test]
    async fn test_add_torrent() {
        let temp_dir = TempDir::new().unwrap();
        let manager = SessionManager::new(SessionConfig::default()).unwrap();
        
        let torrent_data = fs::read(test_torrent_path()).expect("Failed to read test torrent");
        let result = manager.add_torrent(torrent_data, temp_dir.path().to_str().unwrap(), true).await;
        
        assert!(result.is_ok(), "Failed to add torrent: {:?}", result.err());
        let info_hash = result.unwrap();
        assert!(!info_hash.is_empty());
        
        let status = manager.get_torrent_status(&info_hash).await;
        assert_eq!(status, Some(TorrentStatus::Paused));
    }

    #[tokio::test]
    async fn test_pause_and_resume() {
        let temp_dir = TempDir::new().unwrap();
        let manager = SessionManager::new(SessionConfig::default()).unwrap();
        
        let torrent_data = fs::read(test_torrent_path()).expect("Failed to read test torrent");
        let info_hash = manager.add_torrent(torrent_data, temp_dir.path().to_str().unwrap(), false).await.unwrap();
        
        let pause_result = manager.pause_torrent(&info_hash).await;
        assert!(pause_result.is_ok(), "Failed to pause: {:?}", pause_result.err());
        
        let status = manager.get_torrent_status(&info_hash).await;
        assert_eq!(status, Some(TorrentStatus::Paused));
        
        let resume_result = manager.resume_torrent(&info_hash).await;
        assert!(resume_result.is_ok(), "Failed to resume: {:?}", resume_result.err());
        
        let status = manager.get_torrent_status(&info_hash).await;
        assert_eq!(status, Some(TorrentStatus::Downloading));
    }

    #[tokio::test]
    async fn test_remove_torrent() {
        let temp_dir = TempDir::new().unwrap();
        let manager = SessionManager::new(SessionConfig::default()).unwrap();
        
        let torrent_data = fs::read(test_torrent_path()).expect("Failed to read test torrent");
        let info_hash = manager.add_torrent(torrent_data, temp_dir.path().to_str().unwrap(), true).await.unwrap();
        
        let remove_result = manager.remove_torrent(&info_hash).await;
        assert!(remove_result.is_ok(), "Failed to remove: {:?}", remove_result.err());
        
        let status = manager.get_torrent_status(&info_hash).await;
        assert_eq!(status, None);
    }

    #[tokio::test]
    async fn test_list_torrents() {
        let temp_dir = TempDir::new().unwrap();
        let manager = SessionManager::new(SessionConfig::default()).unwrap();
        
        let torrent_data = fs::read(test_torrent_path()).expect("Failed to read test torrent");
        let info_hash = manager.add_torrent(torrent_data, temp_dir.path().to_str().unwrap(), true).await.unwrap();
        
        let torrents = manager.list_torrents().await;
        assert_eq!(torrents.len(), 1);
        assert!(torrents.contains(&info_hash));
    }

    #[tokio::test]
    async fn test_event_subscription() {
        let temp_dir = TempDir::new().unwrap();
        let manager = SessionManager::new(SessionConfig::default()).unwrap();
        
        let mut event_rx = manager.subscribe_events();
        
        let torrent_data = fs::read(test_torrent_path()).expect("Failed to read test torrent");
        let _ = manager.add_torrent(torrent_data, temp_dir.path().to_str().unwrap(), true).await;
        
        let event = event_rx.recv().await;
        assert!(event.is_ok());
        
        match event.unwrap() {
            SessionEvent::TorrentAdded { info_hash, name } => {
                assert!(!info_hash.is_empty());
                assert!(!name.is_empty());
            }
            _ => panic!("Expected TorrentAdded event"),
        }
    }

    #[tokio::test]
    async fn test_torrent_count() {
        let temp_dir = TempDir::new().unwrap();
        let manager = SessionManager::new(SessionConfig::default()).unwrap();
        
        assert_eq!(manager.get_torrent_count().await, 0);
        
        let torrent_data = fs::read(test_torrent_path()).expect("Failed to read test torrent");
        let _ = manager.add_torrent(torrent_data, temp_dir.path().to_str().unwrap(), true).await;
        
        assert_eq!(manager.get_torrent_count().await, 1);
    }

    #[tokio::test]
    async fn test_config_management() {
        let mut config = SessionConfig::default();
        config.listen_port = 9999;
        config.max_connections = 200;
        
        let manager = SessionManager::new(config.clone()).unwrap();
        let retrieved_config = manager.get_config();
        
        assert_eq!(retrieved_config.listen_port, 9999);
        assert_eq!(retrieved_config.max_connections, 200);
    }

    #[tokio::test]
    async fn test_find_torrent() {
        let temp_dir = TempDir::new().unwrap();
        let manager = SessionManager::new(SessionConfig::default()).unwrap();
        
        let torrent_data = fs::read(test_torrent_path()).expect("Failed to read test torrent");
        let info_hash = manager.add_torrent(torrent_data, temp_dir.path().to_str().unwrap(), true).await.unwrap();
        
        assert!(manager.find_torrent(&info_hash));
        assert!(!manager.find_torrent("nonexistent_hash"));
    }
}
