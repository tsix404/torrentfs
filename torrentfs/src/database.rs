//! Database module for TorrentFS SQLite persistence.

use anyhow::{Context, Result};
use sqlx::{sqlite::SqliteConnectOptions, sqlite::SqlitePoolOptions, SqlitePool, sqlite::SqliteJournalMode};
use std::path::PathBuf;
use std::str::FromStr;

/// Database connection pool and configuration.
pub struct Database {
    pool: SqlitePool,
}

impl Database {
    /// Initialize the database connection pool.
    /// Creates the state directory if it doesn't exist.
    pub async fn new() -> Result<Self> {
        let state_dir = get_state_dir()?;
        let db_path = state_dir.join("metadata.db");
        
        // Create state directory if it doesn't exist
        if !state_dir.exists() {
            std::fs::create_dir_all(&state_dir)
                .with_context(|| format!("Failed to create state directory: {:?}", state_dir))?;
        }
        
        let connect_options = SqliteConnectOptions::from_str(&db_path.to_string_lossy())?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal);
        
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(connect_options).await
            .with_context(|| format!("Failed to connect to database: {:?}", db_path))?;
        
        Ok(Self { pool })
    }
    
    /// Create a Database from an existing connection pool (for testing).
    pub fn with_pool(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Get a reference to the connection pool.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
    
    /// Run database migrations.
    pub async fn migrate(&self) -> Result<()> {
        // Create migrations table if it doesn't exist
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS _sqlx_migrations (
                version BIGINT PRIMARY KEY,
                description TEXT NOT NULL,
                installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                success BOOLEAN NOT NULL,
                checksum BLOB NOT NULL,
                execution_time BIGINT NOT NULL
            )"
        )
        .execute(self.pool())
        .await
        .context("Failed to create migrations table")?;
        
        // Check if migration v1 has already been applied
        let v1_applied: Option<i64> = sqlx::query_scalar(
            "SELECT version FROM _sqlx_migrations WHERE version = 1 AND success = true"
        )
        .fetch_optional(self.pool())
        .await
        .context("Failed to check migration v1 status")?;
        
        let v2_applied: Option<i64> = sqlx::query_scalar(
            "SELECT version FROM _sqlx_migrations WHERE version = 2 AND success = true"
        )
        .fetch_optional(self.pool())
        .await
        .context("Failed to check migration v2 status")?;
        
        if v1_applied.is_none() {
            // Apply initial migration
            println!("Applying initial migration...");
            
            // Create torrents table
            sqlx::query(
                "CREATE TABLE IF NOT EXISTS torrents (
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
                )"
            )
            .execute(self.pool())
            .await
            .context("Failed to create torrents table")?;
            
            // Create torrent_files table
            sqlx::query(
                "CREATE TABLE IF NOT EXISTS torrent_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    torrent_id INTEGER NOT NULL,
                    path TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    first_piece INTEGER NOT NULL DEFAULT 0,
                    last_piece INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY (torrent_id) REFERENCES torrents(id) ON DELETE CASCADE,
                    UNIQUE(torrent_id, path)
                )"
            )
            .execute(self.pool())
            .await
            .context("Failed to create torrent_files table")?;
            
            // Create indexes
            sqlx::query("CREATE INDEX IF NOT EXISTS idx_torrents_info_hash ON torrents(info_hash)")
                .execute(self.pool())
                .await
                .context("Failed to create index on torrents.info_hash")?;
            
            sqlx::query("CREATE INDEX IF NOT EXISTS idx_torrents_info_hash_source_path ON torrents(info_hash, source_path)")
                .execute(self.pool())
                .await
                .context("Failed to create composite index on torrents(info_hash, source_path)")?;
            
            sqlx::query("CREATE INDEX IF NOT EXISTS idx_torrents_status ON torrents(status)")
                .execute(self.pool())
                .await
                .context("Failed to create index on torrents.status")?;
            
            sqlx::query("CREATE INDEX IF NOT EXISTS idx_torrent_files_torrent_id ON torrent_files(torrent_id)")
                .execute(self.pool())
                .await
                .context("Failed to create index on torrent_files.torrent_id")?;
            
            sqlx::query("CREATE INDEX IF NOT EXISTS idx_torrent_files_path ON torrent_files(path)")
                .execute(self.pool())
                .await
                .context("Failed to create index on torrent_files.path")?;
            
            // Record migration as successful
            sqlx::query(
                "INSERT OR IGNORE INTO _sqlx_migrations (version, description, success, checksum, execution_time)
                 VALUES (1, 'initial', true, ?, 0)"
            )
            .bind(vec![0u8; 32]) // Dummy checksum
            .execute(self.pool())
            .await
            .context("Failed to record migration")?;
            
            println!("Initial migration applied successfully.");
        } else {
            println!("Migration v1 already applied.");
        }
        
        if v2_applied.is_none() {
            println!("Applying migration v2: adding torrent_data and resume_data columns...");
            
            let add_torrent_data = sqlx::query(
                "ALTER TABLE torrents ADD COLUMN torrent_data BLOB"
            )
            .execute(self.pool())
            .await;
            
            match &add_torrent_data {
                Ok(_) => println!("Added torrent_data column"),
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("duplicate column name") {
                        println!("torrent_data column already exists, skipping");
                    } else {
                        return Err(add_torrent_data.unwrap_err())
                            .context("Failed to add torrent_data column");
                    }
                }
            }
            
            let add_resume_data = sqlx::query(
                "ALTER TABLE torrents ADD COLUMN resume_data BLOB"
            )
            .execute(self.pool())
            .await;
            
            match &add_resume_data {
                Ok(_) => println!("Added resume_data column"),
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("duplicate column name") {
                        println!("resume_data column already exists, skipping");
                    } else {
                        return Err(add_resume_data.unwrap_err())
                            .context("Failed to add resume_data column");
                    }
                }
            }
            
            sqlx::query(
                "INSERT OR IGNORE INTO _sqlx_migrations (version, description, success, checksum, execution_time)
                 VALUES (2, 'add_torrent_data_resume_data', true, ?, 0)"
            )
            .bind(vec![0u8; 32])
            .execute(self.pool())
            .await
            .context("Failed to record migration v2")?;
            
            println!("Migration v2 applied successfully.");
        } else {
            println!("Migration v2 already applied.");
        }
        
        let v3_applied: Option<i64> = sqlx::query_scalar(
            "SELECT version FROM _sqlx_migrations WHERE version = 3 AND success = true"
        )
        .fetch_optional(self.pool())
        .await
        .context("Failed to check migration v3 status")?;
        
        if v3_applied.is_none() {
            println!("Applying migration v3: adding idx_torrents_source_path index...");
            
            sqlx::query("CREATE INDEX IF NOT EXISTS idx_torrents_source_path ON torrents(source_path)")
                .execute(self.pool())
                .await
                .context("Failed to create index on torrents.source_path")?;
            
            sqlx::query(
                "INSERT OR IGNORE INTO _sqlx_migrations (version, description, success, checksum, execution_time)
                 VALUES (3, 'add_source_path_index', true, ?, 0)"
            )
            .bind(vec![0u8; 32])
            .execute(self.pool())
            .await
            .context("Failed to record migration v3")?;
            
            println!("Migration v3 applied successfully.");
        } else {
            println!("Migration v3 already applied.");
        }
        
        let v4_applied: Option<i64> = sqlx::query_scalar(
            "SELECT version FROM _sqlx_migrations WHERE version = 4 AND success = true"
        )
        .fetch_optional(self.pool())
        .await
        .context("Failed to check migration v4 status")?;
        
        if v4_applied.is_none() {
            println!("Applying migration v4: adding offset column to torrent_files...");
            
            let add_offset = sqlx::query(
                "ALTER TABLE torrent_files ADD COLUMN offset INTEGER NOT NULL DEFAULT 0"
            )
            .execute(self.pool())
            .await;
            
            match &add_offset {
                Ok(_) => println!("Added offset column to torrent_files"),
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("duplicate column name") {
                        println!("offset column already exists, skipping");
                    } else {
                        return Err(add_offset.unwrap_err())
                            .context("Failed to add offset column");
                    }
                }
            }
            
            sqlx::query(
                "INSERT OR IGNORE INTO _sqlx_migrations (version, description, success, checksum, execution_time)
                 VALUES (4, 'add_file_offset_column', true, ?, 0)"
            )
            .bind(vec![0u8; 32])
            .execute(self.pool())
            .await
            .context("Failed to record migration v4")?;
            
            println!("Migration v4 applied successfully.");
        } else {
            println!("Migration v4 already applied.");
        }
        
        let v5_applied: Option<i64> = sqlx::query_scalar(
            "SELECT version FROM _sqlx_migrations WHERE version = 5 AND success = true"
        )
        .fetch_optional(self.pool())
        .await
        .context("Failed to check migration v5 status")?;
        
        if v5_applied.is_none() {
            println!("Applying migration v5: changing unique constraint to (info_hash, source_path)...");
            
            // Cleanup any leftover tables from failed migrations
            let _ = sqlx::query("DROP TABLE IF EXISTS torrents_new")
                .execute(self.pool())
                .await;
            
            // Check if we need to migrate (check if old constraint exists)
            let constraint_check: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM pragma_index_info('sqlite_autoindex_torrents_1')"
            )
            .fetch_one(self.pool())
            .await
            .unwrap_or(0);
            
            // If constraint_check is 1, it means single-column unique index exists
            // We need to recreate the table with composite constraint
            if constraint_check == 1 {
                println!("Found old single-column unique constraint, migrating...");
                
                // Create new table with composite unique constraint
                sqlx::query(
                    "CREATE TABLE torrents_new (
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
                    )"
                )
                .execute(self.pool())
                .await
                .context("Failed to create torrents_new table")?;
                
                // Copy data from old table
                sqlx::query(
                    "INSERT INTO torrents_new 
                     SELECT id, info_hash, name, total_size, file_count, status, source_path, 
                            torrent_data, resume_data, added_at 
                     FROM torrents"
                )
                .execute(self.pool())
                .await
                .context("Failed to copy data to torrents_new")?;
                
                // Drop old table
                sqlx::query("DROP TABLE torrents")
                    .execute(self.pool())
                    .await
                    .context("Failed to drop old torrents table")?;
                
                // Rename new table
                sqlx::query("ALTER TABLE torrents_new RENAME TO torrents")
                    .execute(self.pool())
                    .await
                    .context("Failed to rename torrents_new to torrents")?;
                
                // Recreate indexes
                sqlx::query("CREATE INDEX IF NOT EXISTS idx_torrents_info_hash ON torrents(info_hash)")
                    .execute(self.pool())
                    .await
                    .context("Failed to recreate idx_torrents_info_hash")?;
                
                sqlx::query("CREATE INDEX IF NOT EXISTS idx_torrents_info_hash_source_path ON torrents(info_hash, source_path)")
                    .execute(self.pool())
                    .await
                    .context("Failed to create idx_torrents_info_hash_source_path")?;
                
                sqlx::query("CREATE INDEX IF NOT EXISTS idx_torrents_status ON torrents(status)")
                    .execute(self.pool())
                    .await
                    .context("Failed to recreate idx_torrents_status")?;
                
                sqlx::query("CREATE INDEX IF NOT EXISTS idx_torrents_source_path ON torrents(source_path)")
                    .execute(self.pool())
                    .await
                    .context("Failed to recreate idx_torrents_source_path")?;
                
                println!("Migration v5: table recreated with composite unique constraint");
            } else {
                // Already has correct schema or no data, just ensure index exists
                sqlx::query("CREATE INDEX IF NOT EXISTS idx_torrents_info_hash_source_path ON torrents(info_hash, source_path)")
                    .execute(self.pool())
                    .await
                    .context("Failed to create idx_torrents_info_hash_source_path")?;
                
                println!("Migration v5: composite index created (schema already correct)");
            }
            
            sqlx::query(
                "INSERT OR IGNORE INTO _sqlx_migrations (version, description, success, checksum, execution_time)
                 VALUES (5, 'change_unique_constraint_to_composite', true, ?, 0)"
            )
            .bind(vec![0u8; 32])
            .execute(self.pool())
            .await
            .context("Failed to record migration v5")?;
            
            println!("Migration v5 applied successfully.");
        } else {
            println!("Migration v5 already applied.");
        }
        
        Ok(())
    }
}

/// Get the TorrentFS state directory.
/// Defaults to `~/.local/share/torrentfs/db/`.
fn get_state_dir() -> Result<PathBuf> {
    let home_dir = dirs::home_dir()
        .context("Could not determine home directory")?;
    
    let state_dir = home_dir
        .join(".local")
        .join("share")
        .join("torrentfs")
        .join("db");
    
    Ok(state_dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[tokio::test]
    async fn test_database_connection() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        
        // Override state directory for test
        let connect_options = SqliteConnectOptions::from_str(&db_path.to_string_lossy())
            .unwrap()
            .create_if_missing(true);
        
        let pool = SqlitePool::connect_with(connect_options).await.unwrap();
        let db = Database { pool };
        
        // Test that we can run a simple query
        let result: i64 = sqlx::query_scalar("SELECT 1")
            .fetch_one(db.pool())
            .await
            .unwrap();
        
        assert_eq!(result, 1);
    }
    
    #[tokio::test]
    async fn test_migration_creates_source_path_index() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        
        let connect_options = SqliteConnectOptions::from_str(&db_path.to_string_lossy())
            .unwrap()
            .create_if_missing(true);
        
        let pool = SqlitePool::connect_with(connect_options).await.unwrap();
        let db = Database::with_pool(pool);
        
        db.migrate().await.unwrap();
        
        let index_exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_torrents_source_path'"
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        
        assert_eq!(index_exists, 1, "idx_torrents_source_path index should exist after migration");
    }
}