-- Drop index
DROP INDEX IF EXISTS idx_torrents_source_path;

-- SQLite does not support DROP COLUMN, so we need to recreate the table
CREATE TABLE torrents_backup (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    info_hash BLOB NOT NULL UNIQUE,
    name TEXT NOT NULL,
    total_size INTEGER NOT NULL,
    file_count INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    added_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO torrents_backup SELECT id, info_hash, name, total_size, file_count, status, added_at FROM torrents;
DROP TABLE torrents;
ALTER TABLE torrents_backup RENAME TO torrents;

-- Recreate indexes
CREATE INDEX idx_torrents_info_hash ON torrents(info_hash);
CREATE INDEX idx_torrents_status ON torrents(status);
