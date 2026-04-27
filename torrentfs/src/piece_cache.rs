use std::path::PathBuf;

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

    fn piece_path(&self, info_hash: &str, piece_idx: u32) -> PathBuf {
        self.cache_dir.join(info_hash).join(format!("{}.piece", piece_idx))
    }

    pub fn has_piece(&self, info_hash: &str, piece_idx: u32) -> bool {
        self.piece_path(info_hash, piece_idx).exists()
    }

    pub fn read_piece(&self, info_hash: &str, piece_idx: u32) -> Result<Vec<u8>> {
        let path = self.piece_path(info_hash, piece_idx);
        std::fs::read(&path).map_err(Into::into)
    }

    pub fn write_piece(&self, info_hash: &str, piece_idx: u32, data: &[u8]) -> Result<()> {
        let piece_dir = self.cache_dir.join(info_hash);
        if !piece_dir.exists() {
            std::fs::create_dir_all(&piece_dir)?;
        }

        let final_path = self.piece_path(info_hash, piece_idx);
        let temp_path = final_path.with_extension("tmp");

        std::fs::write(&temp_path, data)?;
        std::fs::rename(&temp_path, &final_path)?;

        Ok(())
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
}
