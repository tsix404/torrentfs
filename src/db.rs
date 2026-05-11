use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("database error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("torrent with source_path already exists: {0}")]
    SourcePathExists(String),
    #[error("migration error: {0}")]
    Migration(String),
}

#[derive(Debug, Clone)]
pub struct Torrent {
    pub id: i64,
    pub source_path: String,
    pub name: String,
    pub total_size: i64,
    pub info_hash: String,
    pub duplicate_count: i64,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub struct TorrentFile {
    pub id: i64,
    pub torrent_id: i64,
    pub directory_id: Option<i64>,
    pub name: String,
    pub size: i64,
    pub piece_start: Option<i64>,
    pub piece_end: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct TorrentDirectory {
    pub id: i64,
    pub torrent_id: i64,
    pub parent_id: Option<i64>,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InsertTorrentResult {
    Inserted(i64),
    Duplicate(i64),
}

pub struct FileEntry {
    pub path: String,
    pub size: i64,
}

pub struct Database {
    conn: Connection,
}

impl Database {
    pub fn open(path: &Path) -> Result<Self, DbError> {
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        let mut db = Self { conn };
        db.run_migrations()?;
        Ok(db)
    }

    pub fn open_in_memory() -> Result<Self, DbError> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("PRAGMA foreign_keys=ON;")?;
        let mut db = Self { conn };
        db.run_migrations()?;
        Ok(db)
    }

    fn run_migrations(&mut self) -> Result<(), DbError> {
        let tx = self.conn.transaction()?;
        let user_version: i64 = tx
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .optional()?
            .unwrap_or(0);

        if user_version < 1 {
            Self::migrate_v1(&tx)?;
        }

        tx.pragma_update(None, "user_version", 1)?;
        tx.commit()?;
        Ok(())
    }

    fn migrate_v1(conn: &Connection) -> Result<(), DbError> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS torrents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_path TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                total_size INTEGER NOT NULL,
                info_hash TEXT UNIQUE NOT NULL,
                duplicate_count INTEGER NOT NULL DEFAULT 0,
                created_at DATETIME NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_torrents_source_path ON torrents(source_path);
            CREATE INDEX IF NOT EXISTS idx_torrents_info_hash ON torrents(info_hash);

            CREATE TABLE IF NOT EXISTS torrent_directories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                torrent_id INTEGER NOT NULL,
                parent_id INTEGER,
                name TEXT NOT NULL,
                FOREIGN KEY (torrent_id) REFERENCES torrents(id) ON DELETE CASCADE,
                FOREIGN KEY (parent_id) REFERENCES torrent_directories(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_torrent_dirs_torrent_id ON torrent_directories(torrent_id);
            CREATE INDEX IF NOT EXISTS idx_torrent_dirs_parent_id ON torrent_directories(parent_id);

            CREATE TABLE IF NOT EXISTS directory_closure (
                ancestor_id INTEGER NOT NULL,
                descendant_id INTEGER NOT NULL,
                depth INTEGER NOT NULL,
                PRIMARY KEY (ancestor_id, descendant_id),
                FOREIGN KEY (ancestor_id) REFERENCES torrent_directories(id) ON DELETE CASCADE,
                FOREIGN KEY (descendant_id) REFERENCES torrent_directories(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_closure_descendant ON directory_closure(descendant_id);

            CREATE TABLE IF NOT EXISTS torrent_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                torrent_id INTEGER NOT NULL,
                directory_id INTEGER,
                name TEXT NOT NULL,
                size INTEGER NOT NULL,
                piece_start INTEGER,
                piece_end INTEGER,
                FOREIGN KEY (torrent_id) REFERENCES torrents(id) ON DELETE CASCADE,
                FOREIGN KEY (directory_id) REFERENCES torrent_directories(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_torrent_files_torrent_id ON torrent_files(torrent_id);
            CREATE INDEX IF NOT EXISTS idx_torrent_files_directory_id ON torrent_files(directory_id);",
        )?;
        Ok(())
    }

    pub fn insert_torrent(
        &mut self,
        source_path: &str,
        name: &str,
        total_size: i64,
        info_hash: &str,
    ) -> Result<InsertTorrentResult, DbError> {
        let existing: Option<i64> = self
            .conn
            .query_row(
                "SELECT id FROM torrents WHERE info_hash = ?",
                params![info_hash],
                |row| row.get(0),
            )
            .optional()?
            .flatten();

        if let Some(id) = existing {
            self.conn.execute(
                "UPDATE torrents SET duplicate_count = duplicate_count + 1 WHERE id = ?",
                params![id],
            )?;
            return Ok(InsertTorrentResult::Duplicate(id));
        }

        let existing_path: Option<i64> = self
            .conn
            .query_row(
                "SELECT id FROM torrents WHERE source_path = ?",
                params![source_path],
                |row| row.get(0),
            )
            .optional()?
            .flatten();

        if existing_path.is_some() {
            return Err(DbError::SourcePathExists(source_path.to_string()));
        }

        self.conn.execute(
            "INSERT INTO torrents (source_path, name, total_size, info_hash) VALUES (?, ?, ?, ?)",
            params![source_path, name, total_size, info_hash],
        )?;

        let id = self.conn.last_insert_rowid();
        Ok(InsertTorrentResult::Inserted(id))
    }

    pub fn insert_files(
        &mut self,
        torrent_id: i64,
        files: &[FileEntry],
    ) -> Result<(), DbError> {
        let tx = self.conn.transaction()?;

        let mut dir_cache: std::collections::HashMap<String, i64> = std::collections::HashMap::new();

        for file_entry in files {
            let path_parts: Vec<&str> = file_entry.path.split('/').collect();
            if path_parts.is_empty() {
                continue;
            }

            let mut current_parent_id: Option<i64> = None;

            for (i, part) in path_parts.iter().enumerate() {
                let is_file = i == path_parts.len() - 1;
                let current_path = path_parts[..=i].join("/");

                if is_file {
                    tx.execute(
                        "INSERT INTO torrent_files (torrent_id, directory_id, name, size) VALUES (?, ?, ?, ?)",
                        params![torrent_id, current_parent_id, part, file_entry.size],
                    )?;
                } else {
                    if let Some(&cached_id) = dir_cache.get(&current_path) {
                        current_parent_id = Some(cached_id);
                        continue;
                    }

                    let existing_id: Option<i64> = tx
                        .query_row(
                            "SELECT id FROM torrent_directories WHERE torrent_id = ? AND parent_id IS ? AND name = ?",
                            params![torrent_id, current_parent_id, part],
                            |row| row.get(0),
                        )
                        .optional()?
                        .flatten();

                    if let Some(id) = existing_id {
                        dir_cache.insert(current_path.clone(), id);
                        current_parent_id = Some(id);
                        continue;
                    }

                    tx.execute(
                        "INSERT INTO torrent_directories (torrent_id, parent_id, name) VALUES (?, ?, ?)",
                        params![torrent_id, current_parent_id, part],
                    )?;
                    let dir_id = tx.last_insert_rowid();

                    tx.execute(
                        "INSERT INTO directory_closure (ancestor_id, descendant_id, depth) VALUES (?, ?, 0)",
                        params![dir_id, dir_id],
                    )?;

                    if let Some(parent_id) = current_parent_id {
                        tx.execute(
                            "INSERT INTO directory_closure (ancestor_id, descendant_id, depth)
                             SELECT ancestor_id, ?, depth + 1 FROM directory_closure WHERE descendant_id = ?",
                            params![dir_id, parent_id],
                        )?;
                    }

                    dir_cache.insert(current_path.clone(), dir_id);
                    current_parent_id = Some(dir_id);
                }
            }
        }

        tx.commit()?;
        Ok(())
    }

    pub fn get_torrent_by_source_path(&self, source_path: &str) -> Result<Option<Torrent>, DbError> {
        let result = self
            .conn
            .query_row(
                "SELECT id, source_path, name, total_size, info_hash, duplicate_count, created_at
                 FROM torrents WHERE source_path = ?",
                params![source_path],
                |row| {
                    Ok(Torrent {
                        id: row.get(0)?,
                        source_path: row.get(1)?,
                        name: row.get(2)?,
                        total_size: row.get(3)?,
                        info_hash: row.get(4)?,
                        duplicate_count: row.get(5)?,
                        created_at: row.get(6)?,
                    })
                },
            )
            .optional()?;

        Ok(result)
    }

    pub fn get_torrent_by_info_hash(&self, info_hash: &str) -> Result<Option<Torrent>, DbError> {
        let result = self
            .conn
            .query_row(
                "SELECT id, source_path, name, total_size, info_hash, duplicate_count, created_at
                 FROM torrents WHERE info_hash = ?",
                params![info_hash],
                |row| {
                    Ok(Torrent {
                        id: row.get(0)?,
                        source_path: row.get(1)?,
                        name: row.get(2)?,
                        total_size: row.get(3)?,
                        info_hash: row.get(4)?,
                        duplicate_count: row.get(5)?,
                        created_at: row.get(6)?,
                    })
                },
            )
            .optional()?;

        Ok(result)
    }

    pub fn get_files_by_torrent_id(&self, torrent_id: i64) -> Result<Vec<TorrentFile>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, torrent_id, directory_id, name, size, piece_start, piece_end
             FROM torrent_files WHERE torrent_id = ? ORDER BY id",
        )?;

        let files = stmt
            .query_map(params![torrent_id], |row| {
                Ok(TorrentFile {
                    id: row.get(0)?,
                    torrent_id: row.get(1)?,
                    directory_id: row.get(2)?,
                    name: row.get(3)?,
                    size: row.get(4)?,
                    piece_start: row.get(5)?,
                    piece_end: row.get(6)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(files)
    }

    pub fn get_subdirectory_ids(&self, parent_id: i64) -> Result<Vec<i64>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id FROM torrent_directories WHERE parent_id = ?",
        )?;

        let ids = stmt
            .query_map(params![parent_id], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ids)
    }

    pub fn get_files_in_directory(&self, directory_id: i64) -> Result<Vec<TorrentFile>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, torrent_id, directory_id, name, size, piece_start, piece_end
             FROM torrent_files WHERE directory_id = ?",
        )?;

        let files = stmt
            .query_map(params![directory_id], |row| {
                Ok(TorrentFile {
                    id: row.get(0)?,
                    torrent_id: row.get(1)?,
                    directory_id: row.get(2)?,
                    name: row.get(3)?,
                    size: row.get(4)?,
                    piece_start: row.get(5)?,
                    piece_end: row.get(6)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(files)
    }

    pub fn get_root_files(&self, torrent_id: i64) -> Result<Vec<TorrentFile>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, torrent_id, directory_id, name, size, piece_start, piece_end
             FROM torrent_files WHERE torrent_id = ? AND directory_id IS NULL",
        )?;

        let files = stmt
            .query_map(params![torrent_id], |row| {
                Ok(TorrentFile {
                    id: row.get(0)?,
                    torrent_id: row.get(1)?,
                    directory_id: row.get(2)?,
                    name: row.get(3)?,
                    size: row.get(4)?,
                    piece_start: row.get(5)?,
                    piece_end: row.get(6)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(files)
    }

    pub fn get_torrent_directory(&self, torrent_id: i64, parent_id: Option<i64>, name: &str) -> Result<Option<TorrentDirectory>, DbError> {
        let result = self.conn
            .query_row(
                "SELECT id, torrent_id, parent_id, name FROM torrent_directories WHERE torrent_id = ? AND parent_id IS ? AND name = ?",
                params![torrent_id, parent_id, name],
                |row| {
                    Ok(TorrentDirectory {
                        id: row.get(0)?,
                        torrent_id: row.get(1)?,
                        parent_id: row.get(2)?,
                        name: row.get(3)?,
                    })
                },
            )
            .optional()?;

        Ok(result)
    }

    pub fn get_torrent_directories_by_parent(&self, parent_id: Option<i64>, torrent_id: i64) -> Result<Vec<TorrentDirectory>, DbError> {
        let mut stmt = if parent_id.is_none() {
            self.conn.prepare(
                "SELECT id, torrent_id, parent_id, name FROM torrent_directories WHERE torrent_id = ? AND parent_id IS NULL",
            )?
        } else {
            self.conn.prepare(
                "SELECT id, torrent_id, parent_id, name FROM torrent_directories WHERE torrent_id = ? AND parent_id = ?",
            )?
        };

        let dirs = if parent_id.is_none() {
            stmt.query_map(params![torrent_id], |row| {
                Ok(TorrentDirectory {
                    id: row.get(0)?,
                    torrent_id: row.get(1)?,
                    parent_id: row.get(2)?,
                    name: row.get(3)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
        } else {
            stmt.query_map(params![torrent_id, parent_id], |row| {
                Ok(TorrentDirectory {
                    id: row.get(0)?,
                    torrent_id: row.get(1)?,
                    parent_id: row.get(2)?,
                    name: row.get(3)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
        };

        Ok(dirs)
    }

    pub fn get_all_files_under_directory(&self, directory_id: i64) -> Result<Vec<TorrentFile>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT f.id, f.torrent_id, f.directory_id, f.name, f.size, f.piece_start, f.piece_end
             FROM torrent_files f
             JOIN directory_closure c ON f.directory_id = c.descendant_id
             WHERE c.ancestor_id = ?",
        )?;

        let files = stmt
            .query_map(params![directory_id], |row| {
                Ok(TorrentFile {
                    id: row.get(0)?,
                    torrent_id: row.get(1)?,
                    directory_id: row.get(2)?,
                    name: row.get(3)?,
                    size: row.get(4)?,
                    piece_start: row.get(5)?,
                    piece_end: row.get(6)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(files)
    }

    pub fn delete_torrent(&mut self, torrent_id: i64) -> Result<(), DbError> {
        self.conn.execute(
            "DELETE FROM torrents WHERE id = ?",
            params![torrent_id],
        )?;
        Ok(())
    }

    pub fn get_all_torrents(&self) -> Result<Vec<Torrent>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, source_path, name, total_size, info_hash, duplicate_count, created_at
             FROM torrents ORDER BY id",
        )?;

        let torrents = stmt
            .query_map([], |row| {
                Ok(Torrent {
                    id: row.get(0)?,
                    source_path: row.get(1)?,
                    name: row.get(2)?,
                    total_size: row.get(3)?,
                    info_hash: row.get(4)?,
                    duplicate_count: row.get(5)?,
                    created_at: row.get(6)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(torrents)
    }

    pub fn get_torrents_by_source_path(&self, source_path: &str) -> Result<Vec<Torrent>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, source_path, name, total_size, info_hash, duplicate_count, created_at
             FROM torrents WHERE source_path = ? ORDER BY id",
        )?;

        let torrents = stmt
            .query_map(params![source_path], |row| {
                Ok(Torrent {
                    id: row.get(0)?,
                    source_path: row.get(1)?,
                    name: row.get(2)?,
                    total_size: row.get(3)?,
                    info_hash: row.get(4)?,
                    duplicate_count: row.get(5)?,
                    created_at: row.get(6)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(torrents)
    }

    pub fn get_source_path_prefixes(&self, prefix: &str) -> Result<Vec<String>, DbError> {
        let paths: Vec<String> = if prefix.is_empty() {
            let mut stmt = self.conn.prepare(
                "SELECT DISTINCT source_path FROM torrents WHERE source_path != '' ORDER BY source_path",
            )?;
            let rows = stmt.query_map([], |row| row.get(0))?;
            rows.collect::<Result<Vec<_>, _>>()?
        } else {
            let pattern = format!("{}%", prefix);
            let mut stmt = self.conn.prepare(
                "SELECT DISTINCT source_path FROM torrents WHERE source_path LIKE ? ORDER BY source_path",
            )?;
            let rows = stmt.query_map(params![pattern], |row| row.get(0))?;
            rows.collect::<Result<Vec<_>, _>>()?
        };

        let mut result: Vec<String> = paths
            .iter()
            .filter_map(|p| {
                let stripped: &str = if prefix.is_empty() {
                    p.as_str()
                } else {
                    p.strip_prefix(prefix)?.trim_start_matches('/')
                };
                
                if stripped.is_empty() {
                    return None;
                }
                
                let first_component = stripped.split('/').next().unwrap_or("");
                if first_component.is_empty() {
                    None
                } else {
                    Some(first_component.to_string())
                }
            })
            .collect();

        result.sort();
        result.dedup();
        Ok(result)
    }

    pub fn get_file_by_path(&self, torrent_id: i64, path: &str) -> Result<Option<TorrentFile>, DbError> {
        let parts: Vec<&str> = path.split('/').collect();
        if parts.is_empty() {
            return Ok(None);
        }

        let file_name = parts.last().unwrap();
        let dir_path_parts: Vec<&str> = parts[..parts.len()-1].to_vec();
        
        if dir_path_parts.is_empty() {
            let result = self.conn
                .query_row(
                    "SELECT id, torrent_id, directory_id, name, size, piece_start, piece_end
                     FROM torrent_files WHERE torrent_id = ? AND directory_id IS NULL AND name = ?",
                    params![torrent_id, file_name],
                    |row| {
                        Ok(TorrentFile {
                            id: row.get(0)?,
                            torrent_id: row.get(1)?,
                            directory_id: row.get(2)?,
                            name: row.get(3)?,
                            size: row.get(4)?,
                            piece_start: row.get(5)?,
                            piece_end: row.get(6)?,
                        })
                    },
                )
                .optional()?;

            return Ok(result);
        }

        let dir_id = self.resolve_directory_path(torrent_id, &dir_path_parts)?;
        
        match dir_id {
            Some(did) => {
                let result = self.conn
                    .query_row(
                        "SELECT id, torrent_id, directory_id, name, size, piece_start, piece_end
                         FROM torrent_files WHERE torrent_id = ? AND directory_id = ? AND name = ?",
                        params![torrent_id, did, file_name],
                        |row| {
                            Ok(TorrentFile {
                                id: row.get(0)?,
                                torrent_id: row.get(1)?,
                                directory_id: row.get(2)?,
                                name: row.get(3)?,
                                size: row.get(4)?,
                                piece_start: row.get(5)?,
                                piece_end: row.get(6)?,
                            })
                        },
                    )
                    .optional()?;
                Ok(result)
            }
            None => Ok(None),
        }
    }

    fn resolve_directory_path(&self, torrent_id: i64, parts: &[&str]) -> Result<Option<i64>, DbError> {
        let mut current_parent: Option<i64> = None;

        for part in parts {
            let existing_id: Option<i64> = self.conn
                .query_row(
                    "SELECT id FROM torrent_directories WHERE torrent_id = ? AND parent_id IS ? AND name = ?",
                    params![torrent_id, current_parent, part],
                    |row| row.get(0),
                )
                .optional()?
                .flatten();

            match existing_id {
                Some(id) => current_parent = Some(id),
                None => return Ok(None),
            }
        }

        Ok(current_parent)
    }

pub fn get_torrent_id_by_name_and_source_path(&self, name: &str, source_path: &str) -> Result<Option<i64>, DbError> {
        let result = self.conn
            .query_row(
                "SELECT id FROM torrents WHERE name = ? AND source_path = ?",
                params![name, source_path],
                |row| row.get(0),
            )
            .optional()?
            .flatten();

        Ok(result)
    }

    pub fn get_torrent_by_id(&self, id: i64) -> Result<Option<Torrent>, DbError> {
        let result = self
            .conn
            .query_row(
                "SELECT id, source_path, name, total_size, info_hash, duplicate_count, created_at
                 FROM torrents WHERE id = ?",
                params![id],
                |row| {
                    Ok(Torrent {
                        id: row.get(0)?,
                        source_path: row.get(1)?,
                        name: row.get(2)?,
                        total_size: row.get(3)?,
                        info_hash: row.get(4)?,
                        duplicate_count: row.get(5)?,
                        created_at: row.get(6)?,
                    })
                },
            )
            .optional()?;

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_open_in_memory() {
        let db = Database::open_in_memory();
        assert!(db.is_ok());
    }

    #[test]
    fn test_insert_and_get_torrent() {
        let mut db = Database::open_in_memory().unwrap();
        
        let result = db.insert_torrent("test/path", "Test Torrent", 1024, "abc123").unwrap();
        assert_eq!(result, InsertTorrentResult::Inserted(1));
        
        let torrent = db.get_torrent_by_source_path("test/path").unwrap().unwrap();
        assert_eq!(torrent.name, "Test Torrent");
        assert_eq!(torrent.total_size, 1024);
    }

    #[test]
    fn test_duplicate_info_hash() {
        let mut db = Database::open_in_memory().unwrap();
        
        db.insert_torrent("path1", "Torrent 1", 1024, "hash1").unwrap();
        let result = db.insert_torrent("path2", "Torrent 2", 2048, "hash1").unwrap();
        assert_eq!(result, InsertTorrentResult::Duplicate(1));
        
        let torrent = db.get_torrent_by_source_path("path1").unwrap().unwrap();
        assert_eq!(torrent.duplicate_count, 1);
    }

    #[test]
    fn test_duplicate_source_path_error() {
        let mut db = Database::open_in_memory().unwrap();
        
        db.insert_torrent("path1", "Torrent 1", 1024, "hash1").unwrap();
        let result = db.insert_torrent("path1", "Torrent 2", 2048, "hash2");
        assert!(matches!(result, Err(DbError::SourcePathExists(_))));
    }

    #[test]
    fn test_insert_files() {
        let mut db = Database::open_in_memory().unwrap();
        
        let torrent_id = match db.insert_torrent("path1", "Test", 1024, "hash1").unwrap() {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        };

        let files = vec![
            FileEntry { path: "dir1/file1.txt".to_string(), size: 100 },
            FileEntry { path: "dir1/file2.txt".to_string(), size: 200 },
            FileEntry { path: "dir2/file3.txt".to_string(), size: 300 },
        ];

        db.insert_files(torrent_id, &files).unwrap();

        let all_files = db.get_files_by_torrent_id(torrent_id).unwrap();
        assert_eq!(all_files.len(), 3);
    }

    #[test]
    fn test_get_subdirectory_ids() {
        let mut db = Database::open_in_memory().unwrap();
        
        let torrent_id = match db.insert_torrent("path1", "Test", 1024, "hash1").unwrap() {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        };

        let files = vec![
            FileEntry { path: "dir1/file1.txt".to_string(), size: 100 },
            FileEntry { path: "dir2/file2.txt".to_string(), size: 200 },
        ];

        db.insert_files(torrent_id, &files).unwrap();

        let root_dirs = db.get_torrent_directories_by_parent(None, torrent_id).unwrap();
        assert_eq!(root_dirs.len(), 2);
    }

    #[test]
    fn test_delete_torrent_cascade() {
        let mut db = Database::open_in_memory().unwrap();
        
        let torrent_id = match db.insert_torrent("path1", "Test", 1024, "hash1").unwrap() {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        };

        let files = vec![FileEntry { path: "file.txt".to_string(), size: 100 }];
        db.insert_files(torrent_id, &files).unwrap();

        db.delete_torrent(torrent_id).unwrap();

        let torrent = db.get_torrent_by_source_path("path1").unwrap();
        assert!(torrent.is_none());

        let files = db.get_files_by_torrent_id(torrent_id).unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn test_get_files_in_directory() {
        let mut db = Database::open_in_memory().unwrap();
        
        let torrent_id = match db.insert_torrent("path1", "Test", 1024, "hash1").unwrap() {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        };

        let files = vec![
            FileEntry { path: "dir1/file1.txt".to_string(), size: 100 },
            FileEntry { path: "dir1/file2.txt".to_string(), size: 200 },
            FileEntry { path: "file3.txt".to_string(), size: 300 },
        ];

        db.insert_files(torrent_id, &files).unwrap();

        let dirs = db.get_torrent_directories_by_parent(None, torrent_id).unwrap();
        let dir1 = dirs.iter().find(|d| d.name == "dir1").unwrap();

        let dir_files = db.get_files_in_directory(dir1.id).unwrap();
        assert_eq!(dir_files.len(), 2);
    }

    #[test]
    fn test_get_all_files_under_directory() {
        let mut db = Database::open_in_memory().unwrap();
        
        let torrent_id = match db.insert_torrent("path1", "Test", 1024, "hash1").unwrap() {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        };

        let files = vec![
            FileEntry { path: "dir1/subdir/file1.txt".to_string(), size: 100 },
            FileEntry { path: "dir1/file2.txt".to_string(), size: 200 },
        ];

        db.insert_files(torrent_id, &files).unwrap();

        let dirs = db.get_torrent_directories_by_parent(None, torrent_id).unwrap();
        let dir1 = dirs.iter().find(|d| d.name == "dir1").unwrap();

        let all_files = db.get_all_files_under_directory(dir1.id).unwrap();
        assert_eq!(all_files.len(), 2);
    }

    #[test]
    fn test_persistence() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        {
            let mut db = Database::open(path).unwrap();
            db.insert_torrent("path1", "Test", 1024, "hash1").unwrap();
        }

        {
            let db = Database::open(path).unwrap();
            let torrent = db.get_torrent_by_source_path("path1").unwrap().unwrap();
            assert_eq!(torrent.name, "Test");
        }
    }

    #[test]
    fn test_get_torrent_by_info_hash() {
        let mut db = Database::open_in_memory().unwrap();
        
        db.insert_torrent("path1", "Test", 1024, "abc123").unwrap();
        
        let torrent = db.get_torrent_by_info_hash("abc123").unwrap().unwrap();
        assert_eq!(torrent.source_path, "path1");
    }

    #[test]
    fn test_get_all_torrents() {
        let mut db = Database::open_in_memory().unwrap();
        
        db.insert_torrent("path1", "Torrent 1", 1024, "hash1").unwrap();
        db.insert_torrent("path2", "Torrent 2", 2048, "hash2").unwrap();
        
        let torrents = db.get_all_torrents().unwrap();
        assert_eq!(torrents.len(), 2);
    }

    #[test]
    fn test_nested_directory_structure() {
        let mut db = Database::open_in_memory().unwrap();
        
        let torrent_id = match db.insert_torrent("path1", "Test", 1024, "hash1").unwrap() {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        };

        let files = vec![
            FileEntry { path: "a/b/c/file.txt".to_string(), size: 100 },
        ];

        db.insert_files(torrent_id, &files).unwrap();

        let all_files = db.get_files_by_torrent_id(torrent_id).unwrap();
        assert_eq!(all_files.len(), 1);
    }

    #[test]
    fn test_get_torrents_by_source_path() {
        let mut db = Database::open_in_memory().unwrap();
        
        db.insert_torrent("path1", "Torrent 1", 1024, "hash1").unwrap();
        db.insert_torrent("path2", "Torrent 2", 2048, "hash2").unwrap();
        db.insert_torrent("other", "Torrent 3", 3072, "hash3").unwrap();
        
        let torrents = db.get_torrents_by_source_path("path1").unwrap();
        assert_eq!(torrents.len(), 1);
        assert_eq!(torrents[0].name, "Torrent 1");
        
        let torrents = db.get_torrents_by_source_path("nonexistent").unwrap();
        assert_eq!(torrents.len(), 0);
    }

    #[test]
    fn test_get_source_path_prefixes() {
        let mut db = Database::open_in_memory().unwrap();
        
        db.insert_torrent("a/b", "Torrent 1", 1024, "hash1").unwrap();
        db.insert_torrent("a/c", "Torrent 2", 2048, "hash2").unwrap();
        db.insert_torrent("d", "Torrent 3", 3072, "hash3").unwrap();
        
        let prefixes = db.get_source_path_prefixes("").unwrap();
        assert!(prefixes.contains(&"a".to_string()));
        assert!(prefixes.contains(&"d".to_string()));
        
        let prefixes = db.get_source_path_prefixes("a").unwrap();
        assert!(prefixes.contains(&"b".to_string()));
        assert!(prefixes.contains(&"c".to_string()));
    }

    #[test]
    fn test_get_root_files() {
        let mut db = Database::open_in_memory().unwrap();
        
        let torrent_id = match db.insert_torrent("path1", "Test", 1024, "hash1").unwrap() {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        };

        let files = vec![
            FileEntry { path: "file1.txt".to_string(), size: 100 },
            FileEntry { path: "file2.txt".to_string(), size: 200 },
            FileEntry { path: "dir/file3.txt".to_string(), size: 300 },
        ];

        db.insert_files(torrent_id, &files).unwrap();

        let root_files = db.get_root_files(torrent_id).unwrap();
        assert_eq!(root_files.len(), 2);
    }

    #[test]
    fn test_get_torrent_directory() {
        let mut db = Database::open_in_memory().unwrap();
        
        let torrent_id = match db.insert_torrent("path1", "Test", 1024, "hash1").unwrap() {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        };

        let files = vec![
            FileEntry { path: "dir1/file.txt".to_string(), size: 100 },
        ];

        db.insert_files(torrent_id, &files).unwrap();

        let dir = db.get_torrent_directory(torrent_id, None, "dir1").unwrap();
        assert!(dir.is_some());
        assert_eq!(dir.unwrap().name, "dir1");
    }
}
