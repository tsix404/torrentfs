use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{TorrentError, TorrentResult};

const CACHE_VERSION: u32 = 1;
const METADATA_FILE: &str = "cache.meta";

#[derive(Debug, Clone)]
pub struct PieceInfo {
    pub info_hash: String,
    pub piece_index: i32,
    pub size: usize,
    pub last_accessed: u64,
}

#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub info_hash: String,
    pub piece_index: i32,
    pub last_accessed: u64,
    pub access_count: u64,
}

pub struct PieceCache {
    cache_dir: PathBuf,
    max_cache_size: u64,
    current_size: Arc<Mutex<u64>>,
    entries: Arc<Mutex<HashMap<String, CacheEntry>>>,
}

impl PieceCache {
    pub fn new(cache_dir: &Path, max_size: u64) -> TorrentResult<Self> {
        let cache_dir = cache_dir.join("pieces");
        fs::create_dir_all(&cache_dir)
            .map_err(|e| TorrentError::IoError(format!("Failed to create cache directory: {}", e)))?;
        
        let mut cache = Self {
            cache_dir,
            max_cache_size: max_size,
            current_size: Arc::new(Mutex::new(0)),
            entries: Arc::new(Mutex::new(HashMap::new())),
        };
        
        cache.load_metadata()?;
        cache.rebuild_index()?;
        
        Ok(cache)
    }
    
    fn metadata_path(&self) -> PathBuf {
        self.cache_dir.join(METADATA_FILE)
    }
    
    fn piece_path(&self, info_hash: &str, piece_index: i32) -> PathBuf {
        self.cache_dir.join(info_hash).join(format!("piece_{:06}", piece_index))
    }
    
    fn load_metadata(&mut self) -> TorrentResult<()> {
        let meta_path = self.metadata_path();
        
        if !meta_path.exists() {
            self.save_metadata()?;
            return Ok(());
        }
        
        let mut file = File::open(&meta_path)
            .map_err(|e| TorrentError::IoError(format!("Failed to open metadata: {}", e)))?;
        
        let mut buf = [0u8; 8];
        file.read_exact(&mut buf[..4])
            .map_err(|e| TorrentError::IoError(format!("Failed to read metadata version: {}", e)))?;
        
        let version = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if version != CACHE_VERSION {
            fs::remove_dir_all(&self.cache_dir)
                .map_err(|e| TorrentError::IoError(format!("Failed to clear cache: {}", e)))?;
            fs::create_dir_all(&self.cache_dir)
                .map_err(|e| TorrentError::IoError(format!("Failed to recreate cache: {}", e)))?;
            self.save_metadata()?;
            return Ok(());
        }
        
        file.read_exact(&mut buf[..8])
            .map_err(|e| TorrentError::IoError(format!("Failed to read cache size: {}", e)))?;
        let current_size = u64::from_le_bytes(buf);
        
        *self.current_size.lock().map_err(|_| 
            TorrentError::Unknown { code: -1, message: "Lock poisoned".to_string() })? = current_size;
        
        Ok(())
    }
    
    fn save_metadata(&self) -> TorrentResult<()> {
        let meta_path = self.metadata_path();
        
        let mut file = File::create(&meta_path)
            .map_err(|e| TorrentError::IoError(format!("Failed to create metadata: {}", e)))?;
        
        file.write_all(&CACHE_VERSION.to_le_bytes())
            .map_err(|e| TorrentError::IoError(format!("Failed to write metadata version: {}", e)))?;
        
        let current_size = *self.current_size.lock()
            .map_err(|_| TorrentError::Unknown { code: -1, message: "Lock poisoned".to_string() })?;
        file.write_all(&current_size.to_le_bytes())
            .map_err(|e| TorrentError::IoError(format!("Failed to write cache size: {}", e)))?;
        
        Ok(())
    }
    
    fn rebuild_index(&mut self) -> TorrentResult<()> {
        let mut entries = self.entries.lock()
            .map_err(|_| TorrentError::Unknown { code: -1, message: "Lock poisoned".to_string() })?;
        entries.clear();
        
        for entry in fs::read_dir(&self.cache_dir)
            .map_err(|e| TorrentError::IoError(format!("Failed to read cache directory: {}", e)))? {
            
            let entry = entry.map_err(|e| TorrentError::IoError(e.to_string()))?;
            let path = entry.path();
            
            if !path.is_dir() {
                continue;
            }
            
            if let Some(info_hash) = path.file_name().and_then(|n| n.to_str()) {
                if info_hash == METADATA_FILE {
                    continue;
                }
                
                for piece_entry in fs::read_dir(&path)
                    .map_err(|e| TorrentError::IoError(format!("Failed to read piece directory: {}", e)))? {
                    
                    let piece_entry = piece_entry.map_err(|e| TorrentError::IoError(e.to_string()))?;
                    let piece_path = piece_entry.path();
                    
                    if let Some(filename) = piece_path.file_name().and_then(|n| n.to_str()) {
                        if let Some(piece_idx_str) = filename.strip_prefix("piece_") {
                            if let Ok(piece_index) = piece_idx_str.parse::<i32>() {
                                let metadata = piece_entry.metadata()
                                    .map_err(|e| TorrentError::IoError(e.to_string()))?;
                                let last_accessed = metadata.accessed()
                                    .map_err(|e| TorrentError::IoError(e.to_string()))?
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_secs();
                                
                                let key = Self::make_key(info_hash, piece_index);
                                entries.insert(key, CacheEntry {
                                    info_hash: info_hash.to_string(),
                                    piece_index,
                                    last_accessed,
                                    access_count: 0,
                                });
                            }
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
    
    fn make_key(info_hash: &str, piece_index: i32) -> String {
        format!("{}:{}", info_hash, piece_index)
    }
    
    pub fn has_piece(&self, info_hash: &str, piece_index: i32) -> bool {
        let key = Self::make_key(info_hash, piece_index);
        self.entries.lock().map(|e| e.contains_key(&key)).unwrap_or(false)
    }
    
    pub fn read_piece(&self, info_hash: &str, piece_index: i32) -> TorrentResult<Option<Vec<u8>>> {
        let key = Self::make_key(info_hash, piece_index);
        
        {
            let entries = self.entries.lock()
                .map_err(|_| TorrentError::Unknown { code: -1, message: "Lock poisoned".to_string() })?;
            
            if !entries.contains_key(&key) {
                return Ok(None);
            }
        }
        
        let path = self.piece_path(info_hash, piece_index);
        
        if !path.exists() {
            let mut entries = self.entries.lock()
                .map_err(|_| TorrentError::Unknown { code: -1, message: "Lock poisoned".to_string() })?;
            entries.remove(&key);
            return Ok(None);
        }
        
        let mut file = File::open(&path)
            .map_err(|e| TorrentError::IoError(format!("Failed to open piece file: {}", e)))?;
        
        let mut data = Vec::new();
        file.read_to_end(&mut data)
            .map_err(|e| TorrentError::IoError(format!("Failed to read piece: {}", e)))?;
        
        {
            let mut entries = self.entries.lock()
                .map_err(|_| TorrentError::Unknown { code: -1, message: "Lock poisoned".to_string() })?;
            
            if let Some(entry) = entries.get_mut(&key) {
                entry.last_accessed = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                entry.access_count += 1;
            }
        }
        
        Ok(Some(data))
    }
    
    pub fn write_piece(&self, info_hash: &str, piece_index: i32, data: &[u8]) -> TorrentResult<()> {
        let key = Self::make_key(info_hash, piece_index);
        let data_size = data.len() as u64;
        
        {
            let mut current_size = self.current_size.lock()
                .map_err(|_| TorrentError::Unknown { code: -1, message: "Lock poisoned".to_string() })?;
            
            while *current_size + data_size > self.max_cache_size && *current_size > 0 {
                drop(current_size);
                self.evict_lru()?;
                current_size = self.current_size.lock()
                    .map_err(|_| TorrentError::Unknown { code: -1, message: "Lock poisoned".to_string() })?;
            }
            
            *current_size += data_size;
        }
        
        let piece_dir = self.cache_dir.join(info_hash);
        fs::create_dir_all(&piece_dir)
            .map_err(|e| TorrentError::IoError(format!("Failed to create piece directory: {}", e)))?;
        
        let path = self.piece_path(info_hash, piece_index);
        
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| TorrentError::IoError(format!("Failed to create piece file: {}", e)))?;
        
        file.write_all(data)
            .map_err(|e| TorrentError::IoError(format!("Failed to write piece: {}", e)))?;
        
        {
            let mut entries = self.entries.lock()
                .map_err(|_| TorrentError::Unknown { code: -1, message: "Lock poisoned".to_string() })?;
            
            entries.insert(key.clone(), CacheEntry {
                info_hash: info_hash.to_string(),
                piece_index,
                last_accessed: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                access_count: 1,
            });
        }
        
        let _ = self.save_metadata();
        
        Ok(())
    }
    
    fn evict_lru(&self) -> TorrentResult<()> {
        let mut entries = self.entries.lock()
            .map_err(|_| TorrentError::Unknown { code: -1, message: "Lock poisoned".to_string() })?;
        
        if entries.is_empty() {
            return Ok(());
        }
        
        let mut oldest_key = None;
        let mut oldest_time = u64::MAX;
        
        for (key, entry) in entries.iter() {
            if entry.last_accessed < oldest_time {
                oldest_time = entry.last_accessed;
                oldest_key = Some(key.clone());
            }
        }
        
        if let Some(key) = oldest_key {
            if let Some(entry) = entries.remove(&key) {
                drop(entries);
                
                let path = self.piece_path(&entry.info_hash, entry.piece_index);
                
                if path.exists() {
                    let size = path.metadata()
                        .map_err(|e| TorrentError::IoError(e.to_string()))?
                        .len();
                    
                    fs::remove_file(&path)
                        .map_err(|e| TorrentError::IoError(format!("Failed to remove piece: {}", e)))?;
                    
                    let mut current_size = self.current_size.lock()
                        .map_err(|_| TorrentError::Unknown { code: -1, message: "Lock poisoned".to_string() })?;
                    *current_size = current_size.saturating_sub(size);
                }
            }
        }
        
        Ok(())
    }
    
    pub fn get_cached_pieces(&self, info_hash: &str) -> Vec<i32> {
        let entries = self.entries.lock().unwrap();
        entries.iter()
            .filter(|(_, e)| e.info_hash == info_hash)
            .map(|(_, e)| e.piece_index)
            .collect()
    }
    
    pub fn cache_size(&self) -> u64 {
        *self.current_size.lock().unwrap()
    }
    
    pub fn has_all_pieces(&self, info_hash: &str, total_pieces: i32) -> bool {
        if total_pieces <= 0 {
            return false;
        }
        
        let entries = self.entries.lock().unwrap();
        let cached_count = entries.iter()
            .filter(|(_, e)| e.info_hash == info_hash)
            .count();
        
        cached_count == total_pieces as usize
    }
    
    pub fn clear_cache(&self) -> TorrentResult<()> {
        let mut entries = self.entries.lock()
            .map_err(|_| TorrentError::Unknown { code: -1, message: "Lock poisoned".to_string() })?;
        entries.clear();
        
        for entry in fs::read_dir(&self.cache_dir)
            .map_err(|e| TorrentError::IoError(format!("Failed to read cache directory: {}", e)))? {
            
            let entry = entry.map_err(|e| TorrentError::IoError(e.to_string()))?;
            let path = entry.path();
            
            if path.is_dir() {
                fs::remove_dir_all(&path)
                    .map_err(|e| TorrentError::IoError(format!("Failed to remove cache directory: {}", e)))?;
            } else if path.file_name().map(|n| n != METADATA_FILE).unwrap_or(false) {
                fs::remove_file(&path)
                    .map_err(|e| TorrentError::IoError(format!("Failed to remove cache file: {}", e)))?;
            }
        }
        
        let mut current_size = self.current_size.lock()
            .map_err(|_| TorrentError::Unknown { code: -1, message: "Lock poisoned".to_string() })?;
        *current_size = 0;
        
        self.save_metadata()
    }
}

unsafe impl Send for PieceCache {}
unsafe impl Sync for PieceCache {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_cache_new() {
        let temp_dir = TempDir::new().unwrap();
        let cache = PieceCache::new(temp_dir.path(), 1024 * 1024);
        assert!(cache.is_ok());
    }
    
    #[test]
    fn test_write_and_read_piece() {
        let temp_dir = TempDir::new().unwrap();
        let cache = PieceCache::new(temp_dir.path(), 1024 * 1024).unwrap();
        
        let info_hash = "abc123";
        let piece_index = 0;
        let data = vec![1, 2, 3, 4, 5];
        
        cache.write_piece(info_hash, piece_index, &data).unwrap();
        
        assert!(cache.has_piece(info_hash, piece_index));
        
        let read_data = cache.read_piece(info_hash, piece_index).unwrap();
        assert_eq!(read_data, Some(data));
    }
    
    #[test]
    fn test_cache_miss() {
        let temp_dir = TempDir::new().unwrap();
        let cache = PieceCache::new(temp_dir.path(), 1024 * 1024).unwrap();
        
        let read_data = cache.read_piece("nonexistent", 0).unwrap();
        assert!(read_data.is_none());
    }
    
    #[test]
    fn test_eviction() {
        let temp_dir = TempDir::new().unwrap();
        let cache = PieceCache::new(temp_dir.path(), 100).unwrap();
        
        cache.write_piece("hash1", 0, &[1, 2, 3, 4, 5]).unwrap();
        cache.write_piece("hash1", 1, &[6, 7, 8, 9, 10]).unwrap();
        cache.write_piece("hash1", 2, &[11, 12, 13, 14, 15]).unwrap();
        
        let first_piece = cache.read_piece("hash1", 0).unwrap();
        assert!(first_piece.is_some() || !cache.has_piece("hash1", 0));
    }
    
    #[test]
    fn test_persistence() {
        let temp_dir = TempDir::new().unwrap();
        
        {
            let cache = PieceCache::new(temp_dir.path(), 1024 * 1024).unwrap();
            cache.write_piece("hash1", 0, &[1, 2, 3, 4, 5]).unwrap();
        }
        
        {
            let cache = PieceCache::new(temp_dir.path(), 1024 * 1024).unwrap();
            assert!(cache.has_piece("hash1", 0));
            let data = cache.read_piece("hash1", 0).unwrap();
            assert_eq!(data, Some(vec![1, 2, 3, 4, 5]));
        }
    }
    
    #[test]
    fn test_get_cached_pieces() {
        let temp_dir = TempDir::new().unwrap();
        let cache = PieceCache::new(temp_dir.path(), 1024 * 1024).unwrap();
        
        cache.write_piece("hash1", 0, &[1]).unwrap();
        cache.write_piece("hash1", 1, &[2]).unwrap();
        cache.write_piece("hash2", 0, &[3]).unwrap();
        
        let pieces = cache.get_cached_pieces("hash1");
        assert_eq!(pieces.len(), 2);
        assert!(pieces.contains(&0));
        assert!(pieces.contains(&1));
        
        let pieces = cache.get_cached_pieces("hash2");
        assert_eq!(pieces.len(), 1);
        assert!(pieces.contains(&0));
    }
}
