use anyhow::{Context, Result};
use std::sync::Arc;

use crate::database::Database;
use crate::repo::TorrentRepo;

pub struct MetadataManager {
    repo: TorrentRepo,
}

#[derive(Debug, Clone)]
pub struct ParsedTorrent {
    pub torrent_name: String,
    pub info_hash: Vec<u8>,
    pub total_size: i64,
    pub file_count: i64,
    pub files: Vec<FileEntry>,
}

#[derive(Debug, Clone)]
pub struct FileEntry {
    pub path: String,
    pub size: i64,
}

impl MetadataManager {
    pub fn new(db: Arc<Database>) -> Result<Self> {
        let repo = TorrentRepo::new(db.pool().clone());
        Ok(Self { repo })
    }

    pub async fn process_torrent_data(&self, data: &[u8]) -> Result<ParsedTorrent> {
        let info = torrentfs_libtorrent::parse_torrent(data)
            .context("Failed to parse torrent data")?;

        let info_hash = hex::decode(&info.info_hash)
            .context("Failed to decode info hash hex")?;

        let files: Vec<FileEntry> = info.files.iter().map(|f| FileEntry {
            path: f.path.clone(),
            size: f.size as i64,
        }).collect();

        Ok(ParsedTorrent {
            torrent_name: info.name,
            info_hash,
            total_size: info.total_size as i64,
            file_count: info.file_count as i64,
            files,
        })
    }

    pub async fn persist_to_db(&self, parsed: &ParsedTorrent) -> Result<()> {
        let repo_files: Vec<crate::repo::FileEntry> = parsed.files.iter().map(|f| {
            crate::repo::FileEntry {
                id: 0,
                torrent_id: 0,
                path: f.path.clone(),
                size: f.size,
            }
        }).collect();

        self.repo.insert_if_not_exists(
            &parsed.info_hash,
            &parsed.torrent_name,
            parsed.total_size,
            parsed.file_count,
            repo_files,
        ).await?;

        tracing::info!(
            "Persisted torrent '{}' ({} files, {} bytes) to DB",
            parsed.torrent_name, parsed.file_count, parsed.total_size
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::sync::Arc;

    fn test_torrent_dir() -> std::path::PathBuf {
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        manifest_dir.join("../")
    }

    fn first_torrent_file() -> Option<std::path::PathBuf> {
        let dir = test_torrent_dir();
        std::fs::read_dir(&dir).ok()?.filter_map(|e| {
            let e = e.ok()?;
            if e.file_name().to_string_lossy().ends_with(".torrent") {
                Some(e.path())
            } else {
                None
            }
        }).next()
    }

    #[tokio::test]
    async fn test_parse_valid_torrent() {
        let test_file = first_torrent_file().expect("No .torrent file found in repo root");
        let data = std::fs::read(&test_file).expect("Failed to read test torrent file");

        let db = Database::new().await.unwrap();
        db.migrate().await.unwrap();
        let manager = MetadataManager::new(Arc::new(db)).unwrap();

        let parsed = manager.process_torrent_data(&data).await.unwrap();
        assert!(!parsed.torrent_name.is_empty());
        assert_eq!(parsed.info_hash.len(), 20);
        assert!(parsed.total_size > 0);
        assert!(parsed.file_count > 0);
    }

    #[tokio::test]
    async fn test_process_and_persist() {
        let test_file = first_torrent_file().expect("No .torrent file found in repo root");
        let data = std::fs::read(&test_file).expect("Failed to read test torrent file");

        let db = Database::new().await.unwrap();
        db.migrate().await.unwrap();
        let manager = MetadataManager::new(Arc::new(db)).unwrap();

        let parsed = manager.process_torrent_data(&data).await.unwrap();
        manager.persist_to_db(&parsed).await.unwrap();
    }
}
