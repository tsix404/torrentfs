use sqlx::SqlitePool;

use crate::error::Result;

#[derive(Debug, Clone, PartialEq)]
pub enum InsertResult {
    Inserted(Torrent),
    AlreadyExists(Torrent),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Torrent {
    pub id: i64,
    pub info_hash: Vec<u8>,
    pub name: String,
    pub total_size: i64,
    pub file_count: i64,
    pub status: String,
    pub metadata_path: String,
    pub added_at: String,
}

#[derive(Debug, Clone)]
pub struct FileEntry {
    pub id: i64,
    pub torrent_id: i64,
    pub path: String,
    pub size: i64,
    pub first_piece: i64,
    pub last_piece: i64,
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
        metadata_path: &str,
    ) -> Result<Torrent> {
        let result = sqlx::query_as::<_, (i64, Vec<u8>, String, i64, i64, String, String, String)>(
            "INSERT INTO torrents (info_hash, name, total_size, file_count, metadata_path)
             VALUES (?, ?, ?, ?, ?)
             RETURNING id, info_hash, name, total_size, file_count, status, metadata_path, added_at",
        )
        .bind(info_hash)
        .bind(name)
        .bind(total_size)
        .bind(file_count)
        .bind(metadata_path)
        .fetch_one(&self.pool)
        .await?;

        Ok(Torrent {
            id: result.0,
            info_hash: result.1,
            name: result.2,
            total_size: result.3,
            file_count: result.4,
            status: result.5,
            metadata_path: result.6,
            added_at: result.7,
        })
    }

    pub async fn find_by_info_hash(&self, hash: &[u8]) -> Result<Option<Torrent>> {
        let row = sqlx::query_as::<_, (i64, Vec<u8>, String, i64, i64, String, String, String)>(
            "SELECT id, info_hash, name, total_size, file_count, status, metadata_path, added_at
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
            metadata_path: r.6,
            added_at: r.7,
        }))
    }

    pub async fn list_all(&self) -> Result<Vec<Torrent>> {
        let rows = sqlx::query_as::<_, (i64, Vec<u8>, String, i64, i64, String, String, String)>(
            "SELECT id, info_hash, name, total_size, file_count, status, metadata_path, added_at
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
                metadata_path: r.6,
                added_at: r.7,
            })
            .collect())
    }

    pub async fn find_by_name(&self, name: &str) -> Result<Option<Torrent>> {
        let row = sqlx::query_as::<_, (i64, Vec<u8>, String, i64, i64, String, String, String)>(
            "SELECT id, info_hash, name, total_size, file_count, status, metadata_path, added_at
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
            metadata_path: r.6,
            added_at: r.7,
        }))
    }

    pub async fn insert_files(&self, torrent_id: i64, files: Vec<FileEntry>) -> Result<()> {
        for file in files {
            sqlx::query(
                "INSERT INTO torrent_files (torrent_id, path, size, first_piece, last_piece) VALUES (?, ?, ?, ?, ?)",
            )
            .bind(torrent_id)
            .bind(&file.path)
            .bind(file.size)
            .bind(file.first_piece)
            .bind(file.last_piece)
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
        metadata_path: &str,
        files: Vec<FileEntry>,
    ) -> Result<InsertResult> {
        if let Some(existing) = self.find_by_info_hash(info_hash).await? {
            return Ok(InsertResult::AlreadyExists(existing));
        }
        let torrent = self.insert(info_hash, name, total_size, file_count, metadata_path).await?;
        self.insert_files(torrent.id, files).await?;
        Ok(InsertResult::Inserted(torrent))
    }

    pub async fn get_files(&self, torrent_id: i64) -> Result<Vec<FileEntry>> {
        let rows = sqlx::query_as::<_, (i64, i64, String, i64, i64, i64)>(
            "SELECT id, torrent_id, path, size, first_piece, last_piece FROM torrent_files WHERE torrent_id = ? ORDER BY id",
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
                info_hash BLOB NOT NULL UNIQUE,
                name TEXT NOT NULL,
                total_size INTEGER NOT NULL,
                file_count INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                metadata_path TEXT NOT NULL DEFAULT '',
                added_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
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
            .insert(&info_hash, "test.torrent", 1024, 3, "")
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

        repo.insert(&vec![1u8; 20], "torrent1", 100, 1, "")
            .await
            .unwrap();
        repo.insert(&vec![2u8; 20], "torrent2", 200, 2, "")
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
            .insert(&vec![1u8; 20], "test.torrent", 2048, 2, "")
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
            },
            FileEntry {
                id: 0,
                torrent_id: torrent.id,
                path: "dir/file2.txt".to_string(),
                size: 1024,
                first_piece: 0,
                last_piece: 0,
            },
        ];

        repo.insert_files(torrent.id, files).await.unwrap();

        let retrieved = repo.get_files(torrent.id).await.unwrap();
        assert_eq!(retrieved.len(), 2);
        assert_eq!(retrieved[0].path, "dir/file1.txt");
        assert_eq!(retrieved[0].size, 1024);
        assert_eq!(retrieved[0].first_piece, 0);
        assert_eq!(retrieved[0].last_piece, 0);
        assert_eq!(retrieved[1].path, "dir/file2.txt");
        assert_eq!(retrieved[1].torrent_id, torrent.id);
        assert_eq!(retrieved[1].first_piece, 0);
        assert_eq!(retrieved[1].last_piece, 0);
    }

    #[tokio::test]
    async fn test_get_files_empty() {
        let (_temp_dir, pool) = setup_test_db().await;
        let repo = TorrentRepo::new(pool);

        let torrent = repo
            .insert(&vec![1u8; 20], "empty.torrent", 0, 0, "")
            .await
            .unwrap();

        let files = repo.get_files(torrent.id).await.unwrap();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn test_insert_duplicate_info_hash() {
        let (_temp_dir, pool) = setup_test_db().await;
        let repo = TorrentRepo::new(pool);

        let hash = vec![0xAA; 20];
        repo.insert(&hash, "first", 100, 1, "").await.unwrap();

        let result = repo.insert(&hash, "duplicate", 200, 2, "").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_insert_files_duplicate_path() {
        let (_temp_dir, pool) = setup_test_db().await;
        let repo = TorrentRepo::new(pool);

        let torrent = repo
            .insert(&vec![1u8; 20], "test.torrent", 100, 1, "")
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
            },
            FileEntry {
                id: 0,
                torrent_id: torrent.id,
                path: "same.txt".to_string(),
                size: 50,
                first_piece: 0,
                last_piece: 0,
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
            .insert(&vec![1u8; 20], "test.torrent", 4096, 3, "")
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
            },
            FileEntry {
                id: 0,
                torrent_id: torrent.id,
                path: "b.txt".to_string(),
                size: 2048,
                first_piece: 1,
                last_piece: 3,
            },
            FileEntry {
                id: 0,
                torrent_id: torrent.id,
                path: "c.txt".to_string(),
                size: 0,
                first_piece: 4,
                last_piece: 4,
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
        
        repo.insert(&info_hash1, "torrent1", 100, 1, "")
            .await
            .unwrap();
        repo.insert(&info_hash2, "torrent2", 200, 2, "")
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
}
