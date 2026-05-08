use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;

use crate::error::Result;

#[derive(Debug, Clone, PartialEq)]
pub enum InsertResult {
    Inserted(Torrent),
    AlreadyExists(Torrent),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Torrent {
    pub id: i64,
    pub info_hash: Vec<u8>,
    pub name: String,
    pub total_size: i64,
    pub file_count: i64,
    pub status: String,
    pub source_path: String,
    pub torrent_data: Option<Vec<u8>>,
    pub resume_data: Option<Vec<u8>>,
    pub added_at: String,
}

#[derive(Debug, Clone)]
pub struct TorrentWithData {
    pub torrent: Torrent,
    pub torrent_data: Vec<u8>,
    pub resume_data: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct FileEntry {
    pub id: i64,
    pub torrent_id: i64,
    pub path: String,
    pub size: i64,
    pub first_piece: i64,
    pub last_piece: i64,
    pub offset: i64,
}

#[derive(Debug, Clone)]
pub struct TorrentRepo {
    pool: SqlitePool,
}

impl TorrentRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn insert(
        &self,
        info_hash: &[u8],
        name: &str,
        total_size: i64,
        file_count: i64,
        source_path: &str,
        torrent_data: Option<&[u8]>,
    ) -> Result<Torrent> {
        let result = sqlx::query_as::<_, (i64, Vec<u8>, String, i64, i64, String, String, Option<Vec<u8>>, Option<Vec<u8>>, String)>(
            "INSERT INTO torrents (info_hash, name, total_size, file_count, source_path, torrent_data)
             VALUES (?, ?, ?, ?, ?, ?)
             RETURNING id, info_hash, name, total_size, file_count, status, source_path, torrent_data, resume_data, added_at",
        )
        .bind(info_hash)
        .bind(name)
        .bind(total_size)
        .bind(file_count)
        .bind(source_path)
        .bind(torrent_data)
        .fetch_one(&self.pool)
        .await?;

        Ok(Torrent {
            id: result.0,
            info_hash: result.1,
            name: result.2,
            total_size: result.3,
            file_count: result.4,
            status: result.5,
            source_path: result.6,
            torrent_data: result.7,
            resume_data: result.8,
            added_at: result.9,
        })
    }

    pub async fn find_by_info_hash(&self, hash: &[u8]) -> Result<Option<Torrent>> {
        let row = sqlx::query_as::<_, (i64, Vec<u8>, String, i64, i64, String, String, Option<Vec<u8>>, Option<Vec<u8>>, String)>(
            "SELECT id, info_hash, name, total_size, file_count, status, source_path, torrent_data, resume_data, added_at
             FROM torrents WHERE info_hash = ?",
        )
        .bind(hash)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| Torrent {
            id: r.0,
            info_hash: r.1,
            name: r.2,
            total_size: r.3,
            file_count: r.4,
            status: r.5,
            source_path: r.6,
            torrent_data: r.7,
            resume_data: r.8,
            added_at: r.9,
        }))
    }

    pub async fn find_by_info_hash_and_source_path(&self, hash: &[u8], source_path: &str) -> Result<Option<Torrent>> {
        let row = sqlx::query_as::<_, (i64, Vec<u8>, String, i64, i64, String, String, Option<Vec<u8>>, Option<Vec<u8>>, String)>(
            "SELECT id, info_hash, name, total_size, file_count, status, source_path, torrent_data, resume_data, added_at
             FROM torrents WHERE info_hash = ? AND source_path = ?",
        )
        .bind(hash)
        .bind(source_path)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| Torrent {
            id: r.0,
            info_hash: r.1,
            name: r.2,
            total_size: r.3,
            file_count: r.4,
            status: r.5,
            source_path: r.6,
            torrent_data: r.7,
            resume_data: r.8,
            added_at: r.9,
        }))
    }

    pub async fn list_all(&self) -> Result<Vec<Torrent>> {
        let rows = sqlx::query_as::<_, (i64, Vec<u8>, String, i64, i64, String, String, Option<Vec<u8>>, Option<Vec<u8>>, String)>(
            "SELECT id, info_hash, name, total_size, file_count, status, source_path, torrent_data, resume_data, added_at
             FROM torrents ORDER BY id",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| Torrent {
                id: r.0,
                info_hash: r.1,
                name: r.2,
                total_size: r.3,
                file_count: r.4,
                status: r.5,
                source_path: r.6,
                torrent_data: r.7,
                resume_data: r.8,
                added_at: r.9,
            })
            .collect())
    }

    pub async fn find_by_name(&self, name: &str) -> Result<Option<Torrent>> {
        let row = sqlx::query_as::<_, (i64, Vec<u8>, String, i64, i64, String, String, Option<Vec<u8>>, Option<Vec<u8>>, String)>(
            "SELECT id, info_hash, name, total_size, file_count, status, source_path, torrent_data, resume_data, added_at
             FROM torrents WHERE name = ?",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| Torrent {
            id: r.0,
            info_hash: r.1,
            name: r.2,
            total_size: r.3,
            file_count: r.4,
            status: r.5,
            source_path: r.6,
            torrent_data: r.7,
            resume_data: r.8,
            added_at: r.9,
        }))
    }

    pub async fn insert_files(&self, torrent_id: i64, files: Vec<FileEntry>) -> Result<()> {
        for file in files {
            sqlx::query(
                "INSERT INTO torrent_files (torrent_id, path, size, first_piece, last_piece, offset) VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(torrent_id)
            .bind(&file.path)
            .bind(file.size)
            .bind(file.first_piece)
            .bind(file.last_piece)
            .bind(file.offset)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    pub async fn insert_if_not_exists(
        &self,
        info_hash: &[u8],
        name: &str,
        total_size: i64,
        file_count: i64,
        source_path: &str,
        torrent_data: Option<&[u8]>,
        files: Vec<FileEntry>,
    ) -> Result<InsertResult> {
        if let Some(existing) = self.find_by_info_hash_and_source_path(info_hash, source_path).await? {
            return Ok(InsertResult::AlreadyExists(existing));
        }
        let torrent = self.insert(info_hash, name, total_size, file_count, source_path, torrent_data).await?;
        self.insert_files(torrent.id, files).await?;
        Ok(InsertResult::Inserted(torrent))
    }

    pub async fn get_files(&self, torrent_id: i64) -> Result<Vec<FileEntry>> {
        let rows = sqlx::query_as::<_, (i64, i64, String, i64, i64, i64, i64)>(
            "SELECT id, torrent_id, path, size, first_piece, last_piece, offset FROM torrent_files WHERE torrent_id = ? ORDER BY id",
        )
        .bind(torrent_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| FileEntry {
                id: r.0,
                torrent_id: r.1,
                path: r.2,
                size: r.3,
                first_piece: r.4,
                last_piece: r.5,
                offset: r.6,
            })
            .collect())
    }

    pub async fn update_resume_data(&self, info_hash: &[u8], resume_data: &[u8]) -> Result<()> {
        sqlx::query(
            "UPDATE torrents SET resume_data = ? WHERE info_hash = ?",
        )
        .bind(resume_data)
        .bind(info_hash)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_status(&self, info_hash: &[u8], status: &str) -> Result<()> {
        sqlx::query(
            "UPDATE torrents SET status = ? WHERE info_hash = ?",
        )
        .bind(status)
        .bind(info_hash)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_all_with_data(&self) -> Result<Vec<TorrentWithData>> {
        let rows = sqlx::query_as::<_, (i64, Vec<u8>, String, i64, i64, String, String, Option<Vec<u8>>, Option<Vec<u8>>, String)>(
            "SELECT id, info_hash, name, total_size, file_count, status, source_path, torrent_data, resume_data, added_at
             FROM torrents WHERE torrent_data IS NOT NULL ORDER BY id",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .filter_map(|r| {
                r.7.as_ref().map(|td| TorrentWithData {
                    torrent: Torrent {
                        id: r.0,
                        info_hash: r.1.clone(),
                        name: r.2.clone(),
                        total_size: r.3,
                        file_count: r.4,
                        status: r.5.clone(),
                        source_path: r.6.clone(),
                        torrent_data: Some(td.clone()),
                        resume_data: r.8.clone(),
                        added_at: r.9.clone(),
                    },
                    torrent_data: td.clone(),
                    resume_data: r.8.clone(),
                })
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqliteConnectOptions;
    use std::str::FromStr;
    use tempfile::TempDir;

    async fn setup_test_db() -> (TempDir, SqlitePool) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let options = SqliteConnectOptions::from_str(&db_path.to_string_lossy())
            .unwrap()
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(options).await.unwrap();

        sqlx::query(
            "CREATE TABLE torrents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                info_hash BLOB NOT NULL,
                name TEXT NOT NULL,
                total_size INTEGER NOT NULL,
                file_count INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                source_path TEXT NOT NULL DEFAULT '',
                torrent_data BLOB,
                resume_data BLOB,
                added_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(info_hash, source_path)
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "CREATE TABLE torrent_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                torrent_id INTEGER NOT NULL,
                path TEXT NOT NULL,
                size INTEGER NOT NULL,
                first_piece INTEGER NOT NULL DEFAULT 0,
                last_piece INTEGER NOT NULL DEFAULT 0,
                offset INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY (torrent_id) REFERENCES torrents(id) ON DELETE CASCADE,
                UNIQUE(torrent_id, path)
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        (temp_dir, pool)
    }

    #[tokio::test]
    async fn test_insert_and_find() {
        let (_temp_dir, pool) = setup_test_db().await;
        let repo = TorrentRepo::new(pool);

        let info_hash = vec![0u8; 20];
        let torrent = repo
            .insert(&info_hash, "test.torrent", 1024, 3, "", None::<&[u8]>)
            .await
            .unwrap();

        assert_eq!(torrent.name, "test.torrent");
        assert_eq!(torrent.total_size, 1024);
        assert_eq!(torrent.file_count, 3);
        assert_eq!(torrent.status, "pending");
        assert_eq!(torrent.info_hash, info_hash);

        let found = repo.find_by_info_hash(&info_hash).await.unwrap();
        assert!(found.is_some());
        let found = found.unwrap();
        assert_eq!(found.id, torrent.id);
        assert_eq!(found.name, "test.torrent");
    }

    #[tokio::test]
    async fn test_find_by_info_hash_not_found() {
        let (_temp_dir, pool) = setup_test_db().await;
        let repo = TorrentRepo::new(pool);

        let result = repo.find_by_info_hash(&[0u8; 20]).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_list_all() {
        let (_temp_dir, pool) = setup_test_db().await;
        let repo = TorrentRepo::new(pool);

        repo.insert(&vec![1u8; 20], "torrent1", 100, 1, "", None::<&[u8]>)
            .await
            .unwrap();
        repo.insert(&vec![2u8; 20], "torrent2", 200, 2, "", None::<&[u8]>)
            .await
            .unwrap();

        let all = repo.list_all().await.unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].name, "torrent1");
        assert_eq!(all[1].name, "torrent2");
    }

    #[tokio::test]
    async fn test_list_all_empty() {
        let (_temp_dir, pool) = setup_test_db().await;
        let repo = TorrentRepo::new(pool);

        let all = repo.list_all().await.unwrap();
        assert!(all.is_empty());
    }

    #[tokio::test]
    async fn test_insert_and_get_files() {
        let (_temp_dir, pool) = setup_test_db().await;
        let repo = TorrentRepo::new(pool);

        let torrent = repo
            .insert(&vec![1u8; 20], "test.torrent", 2048, 2, "", None::<&[u8]>)
            .await
            .unwrap();

        let files = vec![
            FileEntry {
                id: 0,
                torrent_id: torrent.id,
                path: "dir/file1.txt".to_string(),
                size: 1024,
                first_piece: 0,
                last_piece: 0,
                offset: 0,
            },
            FileEntry {
                id: 0,
                torrent_id: torrent.id,
                path: "dir/file2.txt".to_string(),
                size: 1024,
                first_piece: 0,
                last_piece: 0,
                offset: 1024,
            },
        ];

        repo.insert_files(torrent.id, files).await.unwrap();

        let retrieved = repo.get_files(torrent.id).await.unwrap();
        assert_eq!(retrieved.len(), 2);
        assert_eq!(retrieved[0].path, "dir/file1.txt");
        assert_eq!(retrieved[0].size, 1024);
        assert_eq!(retrieved[0].first_piece, 0);
        assert_eq!(retrieved[0].last_piece, 0);
        assert_eq!(retrieved[0].offset, 0);
        assert_eq!(retrieved[1].path, "dir/file2.txt");
        assert_eq!(retrieved[1].torrent_id, torrent.id);
        assert_eq!(retrieved[1].first_piece, 0);
        assert_eq!(retrieved[1].last_piece, 0);
        assert_eq!(retrieved[1].offset, 1024);
    }

    #[tokio::test]
    async fn test_get_files_empty() {
        let (_temp_dir, pool) = setup_test_db().await;
        let repo = TorrentRepo::new(pool);

        let torrent = repo
            .insert(&vec![1u8; 20], "empty.torrent", 0, 0, "", None::<&[u8]>)
            .await
            .unwrap();

        let files = repo.get_files(torrent.id).await.unwrap();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn test_insert_duplicate_info_hash_same_source_path() {
        let (_temp_dir, pool) = setup_test_db().await;
        let repo = TorrentRepo::new(pool);

        let hash = vec![0xAA; 20];
        repo.insert(&hash, "first", 100, 1, "movies", None::<&[u8]>).await.unwrap();

        let result = repo.insert(&hash, "duplicate", 200, 2, "movies", None::<&[u8]>).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_insert_same_info_hash_different_source_path() {
        let (_temp_dir, pool) = setup_test_db().await;
        let repo = TorrentRepo::new(pool);

        let hash = vec![0xAA; 20];
        let t1 = repo.insert(&hash, "first", 100, 1, "movies", None::<&[u8]>).await.unwrap();
        let t2 = repo.insert(&hash, "second", 200, 2, "backup", None::<&[u8]>).await.unwrap();

        assert_ne!(t1.id, t2.id);
        assert_eq!(t1.source_path, "movies");
        assert_eq!(t2.source_path, "backup");

        let all = repo.list_all().await.unwrap();
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn test_find_by_info_hash_and_source_path() {
        let (_temp_dir, pool) = setup_test_db().await;
        let repo = TorrentRepo::new(pool);

        let hash = vec![0xAA; 20];
        repo.insert(&hash, "first", 100, 1, "movies", None::<&[u8]>).await.unwrap();
        repo.insert(&hash, "second", 200, 2, "backup", None::<&[u8]>).await.unwrap();

        let found_movies = repo.find_by_info_hash_and_source_path(&hash, "movies").await.unwrap();
        assert!(found_movies.is_some());
        assert_eq!(found_movies.unwrap().source_path, "movies");

        let found_backup = repo.find_by_info_hash_and_source_path(&hash, "backup").await.unwrap();
        assert!(found_backup.is_some());
        assert_eq!(found_backup.unwrap().source_path, "backup");

        let found_other = repo.find_by_info_hash_and_source_path(&hash, "other").await.unwrap();
        assert!(found_other.is_none());
    }

    #[tokio::test]
    async fn test_insert_files_duplicate_path() {
        let (_temp_dir, pool) = setup_test_db().await;
        let repo = TorrentRepo::new(pool);

        let torrent = repo
            .insert(&vec![1u8; 20], "test.torrent", 100, 1, "", None::<&[u8]>)
            .await
            .unwrap();

        let files = vec![
            FileEntry {
                id: 0,
                torrent_id: torrent.id,
                path: "same.txt".to_string(),
                size: 50,
                first_piece: 0,
                last_piece: 0,
                offset: 0,
            },
            FileEntry {
                id: 0,
                torrent_id: torrent.id,
                path: "same.txt".to_string(),
                size: 50,
                first_piece: 0,
                last_piece: 0,
                offset: 50,
            },
        ];

        let result = repo.insert_files(torrent.id, files).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_insert_and_get_files_with_piece_range() {
        let (_temp_dir, pool) = setup_test_db().await;
        let repo = TorrentRepo::new(pool);

        let torrent = repo
            .insert(&vec![1u8; 20], "test.torrent", 4096, 3, "", None::<&[u8]>)
            .await
            .unwrap();

        let files = vec![
            FileEntry {
                id: 0,
                torrent_id: torrent.id,
                path: "a.txt".to_string(),
                size: 1024,
                first_piece: 0,
                last_piece: 1,
                offset: 0,
            },
            FileEntry {
                id: 0,
                torrent_id: torrent.id,
                path: "b.txt".to_string(),
                size: 2048,
                first_piece: 1,
                last_piece: 3,
                offset: 1024,
            },
            FileEntry {
                id: 0,
                torrent_id: torrent.id,
                path: "c.txt".to_string(),
                size: 0,
                first_piece: 4,
                last_piece: 4,
                offset: 3072,
            },
        ];

        repo.insert_files(torrent.id, files).await.unwrap();

        let retrieved = repo.get_files(torrent.id).await.unwrap();
        assert_eq!(retrieved.len(), 3);
        assert_eq!(retrieved[0].first_piece, 0);
        assert_eq!(retrieved[0].last_piece, 1);
        assert_eq!(retrieved[1].first_piece, 1);
        assert_eq!(retrieved[1].last_piece, 3);
        assert_eq!(retrieved[2].first_piece, 4);
        assert_eq!(retrieved[2].last_piece, 4);
    }

    #[tokio::test]
    async fn test_find_by_name() {
        let (_temp_dir, pool) = setup_test_db().await;
        let repo = TorrentRepo::new(pool);

        let info_hash1 = vec![1u8; 20];
        let info_hash2 = vec![2u8; 20];
        
        repo.insert(&info_hash1, "torrent1", 100, 1, "", None::<&[u8]>)
            .await
            .unwrap();
        repo.insert(&info_hash2, "torrent2", 200, 2, "", None::<&[u8]>)
            .await
            .unwrap();

        let found = repo.find_by_name("torrent1").await.unwrap();
        assert!(found.is_some());
        let found = found.unwrap();
        assert_eq!(found.name, "torrent1");
        assert_eq!(found.total_size, 100);
        assert_eq!(found.file_count, 1);

        let found = repo.find_by_name("torrent2").await.unwrap();
        assert!(found.is_some());
        let found = found.unwrap();
        assert_eq!(found.name, "torrent2");
        assert_eq!(found.total_size, 200);
        assert_eq!(found.file_count, 2);

        let not_found = repo.find_by_name("nonexistent").await.unwrap();
        assert!(not_found.is_none());
    }

    #[test]
    fn test_torrent_serialize_deserialize() {
        let torrent = Torrent {
            id: 1,
            info_hash: vec![0xAA; 20],
            name: "test.torrent".to_string(),
            total_size: 1024,
            file_count: 3,
            status: "pending".to_string(),
            source_path: "/downloads".to_string(),
            torrent_data: Some(vec![0xDE, 0xAD]),
            resume_data: None,
            added_at: "2026-01-01T00:00:00Z".to_string(),
        };

        let json = serde_json::to_string(&torrent).unwrap();
        let deserialized: Torrent = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized, torrent);
    }

    #[test]
    fn test_torrent_serialize_deserialize_minimal() {
        let torrent = Torrent {
            id: 0,
            info_hash: vec![],
            name: String::new(),
            total_size: 0,
            file_count: 0,
            status: String::new(),
            source_path: String::new(),
            torrent_data: None,
            resume_data: None,
            added_at: String::new(),
        };

        let json = serde_json::to_string(&torrent).unwrap();
        let deserialized: Torrent = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized, torrent);
    }

    #[tokio::test]
    async fn test_update_status() {
        let (_temp_dir, pool) = setup_test_db().await;
        let repo = TorrentRepo::new(pool);

        let info_hash = vec![1u8; 20];
        let torrent = repo
            .insert(&info_hash, "test.torrent", 1024, 3, "", None::<&[u8]>)
            .await
            .unwrap();

        assert_eq!(torrent.status, "pending");

        repo.update_status(&info_hash, "downloading").await.unwrap();

        let updated = repo.find_by_info_hash(&info_hash).await.unwrap().unwrap();
        assert_eq!(updated.status, "downloading");

        repo.update_status(&info_hash, "completed").await.unwrap();

        let updated = repo.find_by_info_hash(&info_hash).await.unwrap().unwrap();
        assert_eq!(updated.status, "completed");
    }

    #[tokio::test]
    async fn test_update_status_not_found() {
        let (_temp_dir, pool) = setup_test_db().await;
        let repo = TorrentRepo::new(pool);

        let info_hash = vec![99u8; 20];
        repo.update_status(&info_hash, "active").await.unwrap();

        let found = repo.find_by_info_hash(&info_hash).await.unwrap();
        assert!(found.is_none());
    }

    #[test]
    fn test_torrent_serialize_deserialize() {
        let torrent = Torrent {
            id: 1,
            info_hash: vec![0xAA; 20],
            name: "test.torrent".to_string(),
            total_size: 1024,
            file_count: 3,
            status: "pending".to_string(),
            source_path: "/downloads".to_string(),
            torrent_data: Some(vec![0xDE, 0xAD]),
            resume_data: None,
            added_at: "2026-01-01T00:00:00Z".to_string(),
        };

        let json = serde_json::to_string(&torrent).unwrap();
        let deserialized: Torrent = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized, torrent);
    }

    #[test]
    fn test_torrent_serialize_deserialize_minimal() {
        let torrent = Torrent {
            id: 0,
            info_hash: vec![],
            name: String::new(),
            total_size: 0,
            file_count: 0,
            status: String::new(),
            source_path: String::new(),
            torrent_data: None,
            resume_data: None,
            added_at: String::new(),
        };

        let json = serde_json::to_string(&torrent).unwrap();
        let deserialized: Torrent = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized, torrent);
    }
}
