use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{TorrentError, TorrentResult};

const CACHE_METADATA_FILE: &str = "cache_metadata.txt";
const PIECE_METADATA_SUFFIX: &str = ".meta";

#[derive(Debug, Clone)]
pub struct PieceMetadata {
    pub last_accessed: u64,
    pub size: u64,
}

#[derive(Debug)]
pub struct CacheManager {
    cache_dir: PathBuf,
    metadata: HashMap<String, PieceMetadata>,
    max_cache_size: u64,
    current_size: u64,
}

fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl CacheManager {
    pub fn new(cache_dir: &Path, max_cache_size: u64) -> TorrentResult<Self> {
        let cache_dir = cache_dir.to_path_buf();
        if !cache_dir.exists() {
            fs::create_dir_all(&cache_dir)
                .map_err(|e| TorrentError::IoError(format!("Failed to create cache directory: {}", e)))?;
        }
        
        let mut manager = CacheManager {
            cache_dir,
            metadata: HashMap::new(),
            max_cache_size,
            current_size: 0,
        };
        
        manager.rebuild_index()?;
        
        Ok(manager)
    }
    
    pub fn rebuild_index(&mut self) -> TorrentResult<()> {
        let metadata_path = self.cache_dir.join(CACHE_METADATA_FILE);
        
        if metadata_path.exists() {
            self.load_metadata_file(&metadata_path)?;
        }
        
        self.scan_cache_directory()?;
        
        self.save_metadata_file()?;
        
        Ok(())
    }
    
    fn load_metadata_file(&mut self, path: &Path) -> TorrentResult<()> {
        let file = File::open(path)
            .map_err(|e| TorrentError::IoError(format!("Failed to open metadata file: {}", e)))?;
        let reader = BufReader::new(file);
        
        for line in reader.lines() {
            let line = line
                .map_err(|e| TorrentError::IoError(format!("Failed to read metadata line: {}", e)))?;
            
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() >= 3 {
                let piece_key = parts[0].to_string();
                if let Ok(last_accessed) = parts[1].parse::<u64>() {
                    if let Ok(size) = parts[2].parse::<u64>() {
                        self.metadata.insert(piece_key, PieceMetadata { last_accessed, size });
                    }
                }
            }
        }
        
        Ok(())
    }
    
    fn scan_cache_directory(&mut self) -> TorrentResult<()> {
        let entries = fs::read_dir(&self.cache_dir)
            .map_err(|e| TorrentError::IoError(format!("Failed to read cache directory: {}", e)))?;
        
        self.current_size = 0;
        
        for entry in entries {
            let entry = entry
                .map_err(|e| TorrentError::IoError(format!("Failed to read directory entry: {}", e)))?;
            let path = entry.path();
            
            if path.is_file() {
                let filename = path.file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("");
                
                if filename == CACHE_METADATA_FILE || filename.ends_with(PIECE_METADATA_SUFFIX) {
                    continue;
                }
                
                let piece_key = filename.to_string();
                
                let metadata = fs::metadata(&path)
                    .map_err(|e| TorrentError::IoError(format!("Failed to get file metadata: {}", e)))?;
                
                let size = metadata.len();
                
                let last_accessed = self.metadata
                    .get(&piece_key)
                    .map(|m| m.last_accessed)
                    .unwrap_or_else(current_timestamp_ms);
                
                self.metadata.insert(piece_key.clone(), PieceMetadata { last_accessed, size });
                self.current_size += size;
            }
        }
        
        Ok(())
    }
    
    fn save_metadata_file(&self) -> TorrentResult<()> {
        let metadata_path = self.cache_dir.join(CACHE_METADATA_FILE);
        let file = File::create(&metadata_path)
            .map_err(|e| TorrentError::IoError(format!("Failed to create metadata file: {}", e)))?;
        let mut writer = BufWriter::new(file);
        
        for (piece_key, meta) in &self.metadata {
            writeln!(writer, "{}\t{}\t{}", piece_key, meta.last_accessed, meta.size)
                .map_err(|e| TorrentError::IoError(format!("Failed to write metadata: {}", e)))?;
        }
        
        writer.flush()
            .map_err(|e| TorrentError::IoError(format!("Failed to flush metadata file: {}", e)))?;
        
        Ok(())
    }
    
    pub fn record_access(&mut self, piece_key: &str) -> TorrentResult<()> {
        let now = current_timestamp_ms();
        
        if let Some(meta) = self.metadata.get_mut(piece_key) {
            meta.last_accessed = now;
        } else {
            return Err(TorrentError::IoError(format!("Piece not found in cache: {}", piece_key)));
        }
        
        self.save_metadata_file()
    }
    
    pub fn add_piece(&mut self, piece_key: &str, size: u64) -> TorrentResult<()> {
        let now = current_timestamp_ms();
        
        self.metadata.insert(piece_key.to_string(), PieceMetadata {
            last_accessed: now,
            size,
        });
        
        self.current_size += size;
        
        if self.current_size > self.max_cache_size {
            self.evict_lru()?;
        }
        
        self.save_metadata_file()
    }
    
    pub fn evict_lru(&mut self) -> TorrentResult<()> {
        while self.current_size > self.max_cache_size && !self.metadata.is_empty() {
            let oldest = self.metadata
                .iter()
                .min_by_key(|(_, meta)| meta.last_accessed)
                .map(|(k, _)| k.clone());
            
            if let Some(piece_key) = oldest {
                self.remove_piece(&piece_key)?;
            } else {
                break;
            }
        }
        
        Ok(())
    }
    
    fn remove_piece(&mut self, piece_key: &str) -> TorrentResult<()> {
        let piece_path = self.cache_dir.join(piece_key);
        
        if piece_path.exists() {
            let size = fs::metadata(&piece_path)
                .map(|m| m.len())
                .unwrap_or(0);
            
            fs::remove_file(&piece_path)
                .map_err(|e| TorrentError::IoError(format!("Failed to remove piece file: {}", e)))?;
            
            self.current_size = self.current_size.saturating_sub(size);
        }
        
        let meta_path = self.cache_dir.join(format!("{}{}", piece_key, PIECE_METADATA_SUFFIX));
        if meta_path.exists() {
            let _ = fs::remove_file(&meta_path);
        }
        
        self.metadata.remove(piece_key);
        
        self.save_metadata_file()
    }
    
    pub fn piece_path(&self, piece_key: &str) -> PathBuf {
        self.cache_dir.join(piece_key)
    }
    
    pub fn has_piece(&self, piece_key: &str) -> bool {
        self.metadata.contains_key(piece_key)
    }
    
    pub fn current_size(&self) -> u64 {
        self.current_size
    }
    
    pub fn piece_count(&self) -> usize {
        self.metadata.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_cache_manager_basic() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
        
        assert_eq!(cache.piece_count(), 0);
        
        let test_key = "test_piece_001";
        let piece_path = cache.piece_path(test_key);
        std::fs::write(&piece_path, vec![0u8; 100])?;
        cache.add_piece(test_key, 100)?;
        
        assert!(cache.has_piece(test_key));
        assert_eq!(cache.piece_count(), 1);
        
        Ok(())
    }
    
    #[test]
    fn test_lru_eviction() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 250)?;
        
        let piece_1_path = cache.piece_path("piece_1");
        std::fs::write(&piece_1_path, vec![0u8; 100])?;
        cache.add_piece("piece_1", 100)?;
        
        std::thread::sleep(std::time::Duration::from_millis(5));
        
        let piece_2_path = cache.piece_path("piece_2");
        std::fs::write(&piece_2_path, vec![0u8; 100])?;
        cache.add_piece("piece_2", 100)?;
        
        assert!(cache.has_piece("piece_1"));
        assert!(cache.has_piece("piece_2"));
        assert_eq!(cache.current_size(), 200);
        
        std::thread::sleep(std::time::Duration::from_millis(5));
        
        let piece_3_path = cache.piece_path("piece_3");
        std::fs::write(&piece_3_path, vec![0u8; 100])?;
        cache.add_piece("piece_3", 100)?;
        
        assert!(!cache.has_piece("piece_1"), "piece_1 should be evicted (oldest)");
        assert!(cache.has_piece("piece_2"), "piece_2 should remain");
        assert!(cache.has_piece("piece_3"), "piece_3 should remain");
        assert_eq!(cache.current_size(), 200);
        
        Ok(())
    }
    
    #[test]
    fn test_persistence_across_restart() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        
        {
            let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
            let piece_path = cache.piece_path("persist_piece");
            std::fs::write(&piece_path, vec![0u8; 50])?;
            cache.add_piece("persist_piece", 50)?;
        }
        
        let cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
        
        assert!(cache.has_piece("persist_piece"));
        
        Ok(())
    }
    
    #[test]
    fn test_record_access_updates_lru() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 250)?;
        
        let piece_1_path = cache.piece_path("piece_1");
        std::fs::write(&piece_1_path, vec![0u8; 100])?;
        cache.add_piece("piece_1", 100)?;
        
        std::thread::sleep(std::time::Duration::from_millis(5));
        
        let piece_2_path = cache.piece_path("piece_2");
        std::fs::write(&piece_2_path, vec![0u8; 100])?;
        cache.add_piece("piece_2", 100)?;
        
        std::thread::sleep(std::time::Duration::from_millis(5));
        cache.record_access("piece_1")?;
        
        std::thread::sleep(std::time::Duration::from_millis(5));
        
        let piece_3_path = cache.piece_path("piece_3");
        std::fs::write(&piece_3_path, vec![0u8; 100])?;
        cache.add_piece("piece_3", 100)?;
        
        assert!(cache.has_piece("piece_1"), "piece_1 should remain (accessed recently)");
        assert!(!cache.has_piece("piece_2"), "piece_2 should be evicted (oldest after piece_1 access)");
        assert!(cache.has_piece("piece_3"), "piece_3 should remain");
        
        Ok(())
    }
}
