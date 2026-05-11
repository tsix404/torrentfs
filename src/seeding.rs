use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::error::{TorrentError, TorrentResult};
use crate::download::{Session, TorrentHandle, TorrentState};

pub struct SeedingManager {
    session: Arc<Mutex<Session>>,
    handles: Arc<Mutex<HashMap<String, TorrentHandle>>>,
    seeding_info: Arc<Mutex<HashMap<String, SeedingInfo>>>,
    cache_dir: PathBuf,
}

#[derive(Debug, Clone)]
pub struct SeedingInfo {
    pub info_hash: String,
    pub name: String,
    pub total_size: u64,
    pub uploaded: u64,
    pub state: SeedingState,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SeedingState {
    Checking,
    Seeding,
    Queued,
    Paused,
    Error,
}

impl SeedingManager {
    pub fn new(cache_dir: &Path) -> TorrentResult<Self> {
        let session = Session::new(None)?;
        
        Ok(Self {
            session: Arc::new(Mutex::new(session)),
            handles: Arc::new(Mutex::new(HashMap::new())),
            seeding_info: Arc::new(Mutex::new(HashMap::new())),
            cache_dir: cache_dir.to_path_buf(),
        })
    }
    
    pub fn add_seed(&self, info: &crate::TorrentInfo) -> TorrentResult<()> {
        let info_hash = hex::encode(info.info_hash()?);
        
        {
            let handles = self.handles.lock()
                .map_err(|_| TorrentError::Unknown { code: -1, message: "Handles lock poisoned".to_string() })?;
            
            if handles.contains_key(&info_hash) {
                return Ok(());
            }
        }
        
        let cache_path = self.cache_dir.join(&info_hash);
        if !cache_path.exists() {
            std::fs::create_dir_all(&cache_path)
                .map_err(|e| TorrentError::IoError(e.to_string()))?;
        }
        
        let handle = {
            let mut session = self.session.lock()
                .map_err(|_| TorrentError::Unknown { code: -1, message: "Session lock poisoned".to_string() })?;
            
            session.add_torrent(info, &cache_path)?
        };
        
        let name = info.name();
        let total_size = info.total_size();
        
        {
            let mut handles = self.handles.lock()
                .map_err(|_| TorrentError::Unknown { code: -1, message: "Handles lock poisoned".to_string() })?;
            
            handles.insert(info_hash.clone(), handle);
        }
        
        {
            let mut info_map = self.seeding_info.lock()
                .map_err(|_| TorrentError::Unknown { code: -1, message: "Seeding info lock poisoned".to_string() })?;
            
            info_map.insert(info_hash.clone(), SeedingInfo {
                info_hash: info_hash.clone(),
                name,
                total_size,
                uploaded: 0,
                state: SeedingState::Checking,
            });
        }
        
        Ok(())
    }
    
    pub fn remove_seed(&self, info_hash: &str) -> TorrentResult<()> {
        let handle = {
            let mut handles = self.handles.lock()
                .map_err(|_| TorrentError::Unknown { code: -1, message: "Handles lock poisoned".to_string() })?;
            handles.remove(info_hash)
        };
        
        if let Some(handle) = handle {
            let mut session = self.session.lock()
                .map_err(|_| TorrentError::Unknown { code: -1, message: "Session lock poisoned".to_string() })?;
            session.remove_torrent(handle, false);
        }
        
        {
            let mut info_map = self.seeding_info.lock()
                .map_err(|_| TorrentError::Unknown { code: -1, message: "Seeding info lock poisoned".to_string() })?;
            info_map.remove(info_hash);
        }
        
        Ok(())
    }
    
    pub fn update_seeding_status(&self) -> TorrentResult<Vec<SeedingInfo>> {
        let handles = self.handles.lock()
            .map_err(|_| TorrentError::Unknown { code: -1, message: "Handles lock poisoned".to_string() })?;
        
        let mut info_map = self.seeding_info.lock()
            .map_err(|_| TorrentError::Unknown { code: -1, message: "Seeding info lock poisoned".to_string() })?;
        
        for (info_hash, handle) in handles.iter() {
            if let Ok(status) = handle.status() {
                if let Some(info) = info_map.get_mut(info_hash) {
                    info.uploaded = status.total_done;
                    
                    info.state = match status.state {
                        TorrentState::Seeding => SeedingState::Seeding,
                        TorrentState::Finished => SeedingState::Seeding,
                        TorrentState::CheckingFiles | TorrentState::CheckingResumeData | TorrentState::QueuedForChecking => SeedingState::Checking,
                        TorrentState::Downloading | TorrentState::DownloadingMetadata => SeedingState::Checking,
                        TorrentState::Allocating => SeedingState::Checking,
                        TorrentState::Unknown => SeedingState::Error,
                    };
                }
            }
        }
        
        Ok(info_map.values().cloned().collect())
    }
    
    pub fn get_seeding_info(&self, info_hash: &str) -> Option<SeedingInfo> {
        if let Err(e) = self.update_seeding_status() {
            eprintln!("[WARN] Failed to update seeding status: {}", e);
        }
        let info_map = self.seeding_info.lock().ok()?;
        info_map.get(info_hash).cloned()
    }
    
    pub fn has_handle(&self, info_hash: &str) -> bool {
        self.handles.lock().ok().map(|h| h.contains_key(info_hash)).unwrap_or(false)
    }
    
    pub fn is_seeding(&self, info_hash: &str) -> bool {
        if let Err(e) = self.update_seeding_status() {
            eprintln!("[WARN] Failed to update seeding status: {}", e);
        }
        let info_map = match self.seeding_info.lock().ok() {
            Some(m) => m,
            None => return false,
        };
        match info_map.get(info_hash) {
            Some(info) => info.state == SeedingState::Seeding,
            None => false,
        }
    }
    
    pub fn get_all_seeds(&self) -> Vec<SeedingInfo> {
        if let Err(e) = self.update_seeding_status() {
            eprintln!("[WARN] Failed to update seeding status: {}", e);
        }
        let info_map = self.seeding_info.lock().ok();
        info_map.map(|m| m.values().cloned().collect()).unwrap_or_default()
    }
    
    pub fn get_total_uploaded(&self) -> u64 {
        if let Err(e) = self.update_seeding_status() {
            eprintln!("[WARN] Failed to update seeding status: {}", e);
        }
        let info_map = self.seeding_info.lock().ok();
        info_map.map(|m| m.values().map(|s| s.uploaded).sum()).unwrap_or(0)
    }
}

unsafe impl Send for SeedingManager {}
unsafe impl Sync for SeedingManager {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_seeding_manager_new() {
        let temp_dir = TempDir::new().unwrap();
        let manager = SeedingManager::new(temp_dir.path());
        assert!(manager.is_ok());
    }
    
    #[test]
    fn test_has_handle() {
        let temp_dir = TempDir::new().unwrap();
        let manager = SeedingManager::new(temp_dir.path()).unwrap();
        
        assert!(!manager.has_handle("nonexistent"));
    }
    
    #[test]
    fn test_is_seeding() {
        let temp_dir = TempDir::new().unwrap();
        let manager = SeedingManager::new(temp_dir.path()).unwrap();
        
        assert!(!manager.is_seeding("nonexistent"));
    }
    
    #[test]
    fn test_get_all_seeds() {
        let temp_dir = TempDir::new().unwrap();
        let manager = SeedingManager::new(temp_dir.path()).unwrap();
        
        let seeds = manager.get_all_seeds();
        assert!(seeds.is_empty());
    }
}
