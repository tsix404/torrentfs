use anyhow::{Context, Result};
use std::path::Path;
use std::sync::Arc;

use crate::database::Database;
use crate::repo::TorrentRepo;

pub struct MetadataManager {
    pub(crate) repo: TorrentRepo,
}

#[derive(Debug, Clone)]
pub struct ParsedTorrent {
    pub torrent_name: String,
    pub info_hash: Vec<u8>,
    pub total_size: i64,
    pub piece_size: i64,
    pub file_count: i64,
    pub files: Vec<FileEntry>,
    pub source_path: String,
}

#[derive(Debug, Clone)]
pub struct FileEntry {
    pub path: String,
    pub size: i64,
    pub first_piece: i64,
    pub last_piece: i64,
}

impl MetadataManager {
    pub fn new(db: Arc<Database>) -> Result<Self> {
        let repo = TorrentRepo::new(db.pool().clone());
        Ok(Self { repo })
    }

    pub async fn process_torrent_data(&self, data: &[u8]) -> Result<ParsedTorrent> {
        self.process_torrent_data_with_path(data, None).await
    }

    pub async fn process_torrent_data_with_path(&self, data: &[u8], torrent_path: Option<&Path>) -> Result<ParsedTorrent> {
        let info = torrentfs_libtorrent::parse_torrent(data)
            .context("Failed to parse torrent data")?;

        let info_hash = hex::decode(&info.info_hash)
            .context("Failed to decode info hash hex")?;

        let files: Vec<FileEntry> = info.files.iter().map(|f| FileEntry {
            path: f.path.clone(),
            size: f.size as i64,
            first_piece: f.first_piece as i64,
            last_piece: f.last_piece as i64,
        }).collect();

        let source_path = torrent_path
            .map(|p| extract_source_path(p))
            .unwrap_or_default();

        Ok(ParsedTorrent {
            torrent_name: info.name,
            info_hash,
            total_size: info.total_size as i64,
            piece_size: info.piece_size as i64,
            file_count: info.file_count as i64,
            files,
            source_path,
        })
    }

    pub async fn persist_to_db(&self, parsed: &ParsedTorrent) -> Result<()> {
        let repo_files: Vec<crate::repo::FileEntry> = parsed.files.iter().map(|f| {
            crate::repo::FileEntry {
                id: 0,
                torrent_id: 0,
                path: f.path.clone(),
                size: f.size,
                first_piece: f.first_piece,
                last_piece: f.last_piece,
            }
        }).collect();

        self.repo.insert_if_not_exists(
            &parsed.info_hash,
            &parsed.torrent_name,
            parsed.total_size,
            parsed.piece_size,
            parsed.file_count,
            repo_files,
            &parsed.source_path,
        ).await?;

        tracing::info!(
            "Persisted torrent '{}' ({} files, {} bytes) to DB",
            parsed.torrent_name, parsed.file_count, parsed.total_size
        );

        Ok(())
    }

    pub async fn list_torrents(&self) -> anyhow::Result<Vec<crate::repo::Torrent>> {
        self.repo.list_all().await.map_err(|e| e.into())
    }

    pub async fn get_torrent_files(&self, torrent_name: &str) -> anyhow::Result<Vec<crate::repo::FileEntry>> {
        let torrent = self.repo
            .find_by_name(torrent_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Torrent '{}' not found", torrent_name))?;
        
        self.repo.get_files(torrent.id).await.map_err(|e| e.into())
    }
}

fn extract_source_path(torrent_path: &Path) -> String {
    let path_str = torrent_path.to_string_lossy();
    
    if let Some(stripped) = path_str.strip_prefix("metadata/") {
        if let Some(parent) = Path::new(stripped).parent() {
            if parent != Path::new("") {
                return format!("{}/", parent.to_string_lossy());
            }
        }
    }
    
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqliteConnectOptions;
    use sqlx::SqlitePool;
    use std::path::Path;
    use std::str::FromStr;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn test_torrent_dir() -> std::path::PathBuf {
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        manifest_dir.join("../test_data")
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

    async fn setup_temp_db() -> (TempDir, Database) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let options = SqliteConnectOptions::from_str(&db_path.to_string_lossy())
            .unwrap()
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(options).await.unwrap();
        let db = Database::with_pool(pool);
        db.migrate().await.unwrap();
        (temp_dir, db)
    }

    #[tokio::test]
    async fn test_parse_valid_torrent() {
        let test_file = first_torrent_file().expect("No .torrent file found in repo root");
        let data = std::fs::read(&test_file).expect("Failed to read test torrent file");

        let (_temp_dir, db) = setup_temp_db().await;
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

        let (_temp_dir, db) = setup_temp_db().await;
        let manager = MetadataManager::new(Arc::new(db)).unwrap();

        let parsed = manager.process_torrent_data(&data).await.unwrap();
        manager.persist_to_db(&parsed).await.unwrap();
    }

    #[tokio::test]
    async fn test_persist_includes_piece_range() {
        let test_file = first_torrent_file().expect("No .torrent file found in repo root");
        let data = std::fs::read(&test_file).expect("Failed to read test torrent file");

        let (_temp_dir, db) = setup_temp_db().await;
        let manager = MetadataManager::new(Arc::new(db)).unwrap();

        let parsed = manager.process_torrent_data(&data).await.unwrap();
        manager.persist_to_db(&parsed).await.unwrap();

        // Re-read from DB and verify piece ranges
        let torrent = manager.repo.find_by_info_hash(&parsed.info_hash).await.unwrap().unwrap();
        let db_files = manager.repo.get_files(torrent.id).await.unwrap();

        assert_eq!(db_files.len() as i64, parsed.file_count);

        for (i, db_file) in db_files.iter().enumerate() {
            let parsed_file = &parsed.files[i];
            assert_eq!(db_file.path, parsed_file.path);
            assert_eq!(db_file.size, parsed_file.size);
            assert_eq!(db_file.first_piece, parsed_file.first_piece);
            assert_eq!(db_file.last_piece, parsed_file.last_piece);
            assert!(db_file.first_piece >= 0);
            assert!(db_file.last_piece >= db_file.first_piece,
                "File {}: last_piece {} < first_piece {}", i, db_file.last_piece, db_file.first_piece);
        }
    }

    #[test]
    fn test_extract_source_path() {
        assert_eq!(extract_source_path(Path::new("metadata/xxx.torrent")), "");
        assert_eq!(extract_source_path(Path::new("metadata/a/b/xxx.torrent")), "a/b/");
        assert_eq!(extract_source_path(Path::new("metadata/a/xxx.torrent")), "a/");
        assert_eq!(extract_source_path(Path::new("other/xxx.torrent")), "");
        assert_eq!(extract_source_path(Path::new("xxx.torrent")), "");
    }

    #[tokio::test]
    async fn test_process_with_source_path() {
        let test_file = first_torrent_file().expect("No .torrent file found in repo root");
        let data = std::fs::read(&test_file).expect("Failed to read test torrent file");

        let (_temp_dir, db) = setup_temp_db().await;
        let manager = MetadataManager::new(Arc::new(db)).unwrap();

        let parsed = manager.process_torrent_data_with_path(&data, Some(Path::new("metadata/sub/dir/test.torrent"))).await.unwrap();
        assert_eq!(parsed.source_path, "sub/dir/");
    }
}
