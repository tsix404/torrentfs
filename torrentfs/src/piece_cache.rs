use std::path::{Path, PathBuf};

use crate::error::Result;

pub struct PieceCache {
    cache_dir: PathBuf,
}

impl PieceCache {
    pub fn new() -> Result<Self> {
        let cache_dir = Self::get_cache_dir()?;
        if !cache_dir.exists() {
            std::fs::create_dir_all(&cache_dir)?;
        }
        Ok(Self { cache_dir })
    }

    pub fn with_state_dir(state_dir: &Path) -> Result<Self> {
        let cache_dir = state_dir.join("cache").join("pieces");
        if !cache_dir.exists() {
            std::fs::create_dir_all(&cache_dir)?;
        }
        Ok(Self { cache_dir })
    }

    pub fn with_cache_dir(cache_dir: PathBuf) -> Result<Self> {
        if !cache_dir.exists() {
            std::fs::create_dir_all(&cache_dir)?;
        }
        Ok(Self { cache_dir })
    }

    fn get_cache_dir() -> Result<PathBuf> {
        let home_dir = dirs::home_dir()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "Could not determine home directory"))?;
        Ok(home_dir
            .join(".local")
            .join("share")
            .join("torrentfs")
            .join("cache")
            .join("pieces"))
    }

    fn validate_info_hash(info_hash: &str) -> Result<()> {
        if info_hash.len() == 40 && info_hash.chars().all(|c| c.is_ascii_hexdigit()) {
            Ok(())
        } else {
            Err(crate::error::TorrentFsError::InvalidInfoHash(info_hash.to_string()).into())
        }
    }

    fn piece_path(&self, info_hash: &str, piece_idx: u32) -> Result<PathBuf> {
        Self::validate_info_hash(info_hash)?;
        Ok(self.cache_dir.join(info_hash).join(format!("{}.piece", piece_idx)))
    }

    pub fn has_piece(&self, info_hash: &str, piece_idx: u32) -> bool {
        self.piece_path(info_hash, piece_idx).map(|p| p.exists()).unwrap_or(false)
    }

    pub fn read_piece(&self, info_hash: &str, piece_idx: u32) -> Result<Vec<u8>> {
        let path = self.piece_path(info_hash, piece_idx)?;
        std::fs::read(&path).map_err(Into::into)
    }

    pub fn write_piece(&self, info_hash: &str, piece_idx: u32, data: &[u8]) -> Result<()> {
        Self::validate_info_hash(info_hash)?;
        let piece_dir = self.cache_dir.join(info_hash);
        if !piece_dir.exists() {
            std::fs::create_dir_all(&piece_dir)?;
        }

        let final_path = self.piece_path(info_hash, piece_idx)?;
        let temp_path = final_path.with_extension("tmp");

        std::fs::write(&temp_path, data)?;
        std::fs::rename(&temp_path, &final_path)?;

        Ok(())
    }

    pub fn scan_cached_pieces(&self) -> Result<Vec<(String, Vec<u32>)>> {
        let mut result = Vec::new();
        
        if !self.cache_dir.exists() {
            return Ok(result);
        }
        
        let entries = std::fs::read_dir(&self.cache_dir)?;
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_dir() {
                if let Some(info_hash) = path.file_name().and_then(|n| n.to_str()) {
                    if info_hash.len() == 40 && info_hash.chars().all(|c| c.is_ascii_hexdigit()) {
                        let mut pieces = Vec::new();
                        
                        if let Ok(piece_entries) = std::fs::read_dir(&path) {
                            for piece_entry in piece_entries {
                                if let Ok(piece_entry) = piece_entry {
                                    let piece_path = piece_entry.path();
                                    if let Some(piece_name) = piece_path.file_name().and_then(|n| n.to_str()) {
                                        if let Some(idx_str) = piece_name.strip_suffix(".piece") {
                                            if let Ok(piece_idx) = idx_str.parse::<u32>() {
                                                pieces.push(piece_idx);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        
                        if !pieces.is_empty() {
                            pieces.sort();
                            result.push((info_hash.to_string(), pieces));
                        }
                    }
                }
            }
        }
        
        tracing::info!(
            torrents = result.len(),
            total_pieces = result.iter().map(|(_, p)| p.len()).sum::<usize>(),
            "Scanned cache directory"
        );
        
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (TempDir, PieceCache) {
        let temp_dir = TempDir::new().unwrap();
        let cache = PieceCache::with_cache_dir(temp_dir.path().join("cache")).unwrap();
        (temp_dir, cache)
    }

    #[test]
    fn test_write_and_read_piece() {
        let (_temp_dir, cache) = setup();
        let info_hash = "abc123def456abc123def456abc123def456abc1";
        let piece_idx = 0u32;
        let data = b"test piece data";

        cache.write_piece(info_hash, piece_idx, data).unwrap();
        let read_data = cache.read_piece(info_hash, piece_idx).unwrap();

        assert_eq!(read_data, data);
    }

    #[test]
    fn test_has_piece() {
        let (_temp_dir, cache) = setup();
        let info_hash = "abc123def456abc123def456abc123def456abc1";
        let piece_idx = 5u32;

        assert!(!cache.has_piece(info_hash, piece_idx));

        cache.write_piece(info_hash, piece_idx, b"data").unwrap();
        assert!(cache.has_piece(info_hash, piece_idx));
    }

    #[test]
    fn test_read_nonexistent_piece() {
        let (_temp_dir, cache) = setup();
        let info_hash = "abc123def456abc123def456abc123def456abc1";
        let piece_idx = 999u32;

        let result = cache.read_piece(info_hash, piece_idx);
        assert!(result.is_err());
    }

    #[test]
    fn test_multiple_pieces() {
        let (_temp_dir, cache) = setup();
        let info_hash = "abc123def456abc123def456abc123def456abc1";

        for i in 0..5u32 {
            let data = format!("piece data {}", i);
            cache.write_piece(info_hash, i, data.as_bytes()).unwrap();
        }

        for i in 0..5u32 {
            assert!(cache.has_piece(info_hash, i));
            let read_data = cache.read_piece(info_hash, i).unwrap();
            assert_eq!(read_data, format!("piece data {}", i).as_bytes());
        }
    }

    #[test]
    fn test_different_torrents() {
        let (_temp_dir, cache) = setup();
        let hash1 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let hash2 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

        cache.write_piece(hash1, 0, b"torrent1 data").unwrap();
        cache.write_piece(hash2, 0, b"torrent2 data").unwrap();

        assert_eq!(cache.read_piece(hash1, 0).unwrap(), b"torrent1 data");
        assert_eq!(cache.read_piece(hash2, 0).unwrap(), b"torrent2 data");
    }

    #[test]
    fn test_overwrite_piece() {
        let (_temp_dir, cache) = setup();
        let info_hash = "abc123def456abc123def456abc123def456abc1";
        let piece_idx = 0u32;

        cache.write_piece(info_hash, piece_idx, b"original").unwrap();
        cache.write_piece(info_hash, piece_idx, b"updated").unwrap();

        let data = cache.read_piece(info_hash, piece_idx).unwrap();
        assert_eq!(data, b"updated");
    }

    #[test]
    fn test_invalid_info_hash_traversal_attack() {
        let (_temp_dir, cache) = setup();
        let malicious_hash = "../etc";
        let piece_idx = 0u32;

        let result = cache.write_piece(malicious_hash, piece_idx, b"malicious");
        assert!(result.is_err());
        assert!(!cache.has_piece(malicious_hash, piece_idx));

        let result = cache.read_piece(malicious_hash, piece_idx);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_info_hash_wrong_length() {
        let (_temp_dir, cache) = setup();
        let invalid_hash = "abc123";
        let piece_idx = 0u32;

        let result = cache.write_piece(invalid_hash, piece_idx, b"data");
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_info_hash_invalid_chars() {
        let (_temp_dir, cache) = setup();
        let invalid_hash = "gggggggggggggggggggggggggggggggggggggggg";
        let piece_idx = 0u32;

        let result = cache.write_piece(invalid_hash, piece_idx, b"data");
        assert!(result.is_err());
    }

    #[test]
    fn test_valid_info_hash_accepted() {
        let (_temp_dir, cache) = setup();
        let valid_hash = "0123456789abcdef0123456789abcdef01234567";
        let piece_idx = 0u32;

        cache.write_piece(valid_hash, piece_idx, b"data").unwrap();
        assert!(cache.has_piece(valid_hash, piece_idx));
        let data = cache.read_piece(valid_hash, piece_idx).unwrap();
        assert_eq!(data, b"data");
    }
}
