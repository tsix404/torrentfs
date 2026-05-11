use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::error::{TorrentError, TorrentResult};
use crate::download::{Session, TorrentHandle, TorrentState};

pub struct SeedingManager {
    session: Arc<Mutex<Session>>,
    active_seeds: Arc<Mutex<HashMap<String, SeedingInfo>>>,
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
            active_seeds: Arc::new(Mutex::new(HashMap::new())),
            cache_dir: cache_dir.to_path_buf(),
        })
    }
    
    pub fn add_seed(&self, info: &crate::TorrentInfo) -> TorrentResult<()> {
        let info_hash = hex::encode(info.info_hash()?);
        
        {
            let seeds = self.active_seeds.lock()
                .map_err(|_| TorrentError::Unknown { code: -1, message: "Lock poisoned".to_string() })?;
            
            if seeds.contains_key(&info_hash) {
                return Ok(());
            }
        }
        
        let cache_path = self.cache_dir.join(&info_hash);
        if !cache_path.exists() {
            std::fs::create_dir_all(&cache_path)
                .map_err(|e| TorrentError::IoError(e.to_string()))?;
        }
        
        let mut session = self.session.lock()
            .map_err(|_| TorrentError::Unknown { code: -1, message: "Session lock poisoned".to_string() })?;
        
        let handle = session.add_torrent(info, &cache_path)?;
        
        let name = info.name();
        let total_size = info.total_size();
        
        {
            let mut seeds = self.active_seeds.lock()
                .map_err(|_| TorrentError::Unknown { code: -1, message: "Lock poisoned".to_string() })?;
            
            seeds.insert(info_hash.clone(), SeedingInfo {
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
        let mut seeds = self.active_seeds.lock()
            .map_err(|_| TorrentError::Unknown { code: -1, message: "Lock poisoned".to_string() })?;
        
        if seeds.remove(info_hash).is_some() {
            drop(seeds);
        }
        
        Ok(())
    }
    
    pub fn update_seeding_status(&self) -> TorrentResult<Vec<SeedingInfo>> {
        let mut result = Vec::new();
        
        let seeds = self.active_seeds.lock()
            .map_err(|_| TorrentError::Unknown { code: -1, message: "Lock poisoned".to_string() })?;
        
        for (info_hash, info) in seeds.iter() {
            result.push(info.clone());
        }
        
        Ok(result)
    }
    
    pub fn get_seeding_info(&self, info_hash: &str) -> Option<SeedingInfo> {
        let seeds = self.active_seeds.lock().ok()?;
        seeds.get(info_hash).cloned()
    }
    
    pub fn is_seeding(&self, info_hash: &str) -> bool {
        let seeds = self.active_seeds.lock().unwrap();
        seeds.contains_key(info_hash)
    }
    
    pub fn get_all_seeds(&self) -> Vec<SeedingInfo> {
        let seeds = self.active_seeds.lock().unwrap();
        seeds.values().cloned().collect()
    }
    
    pub fn get_total_uploaded(&self) -> u64 {
        let seeds = self.active_seeds.lock().unwrap();
        seeds.values().map(|s| s.uploaded).sum()
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
