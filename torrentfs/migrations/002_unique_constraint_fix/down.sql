DROP INDEX idx_torrents_info_hash_source_path;

CREATE TABLE torrents_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    info_hash BLOB NOT NULL UNIQUE,
    name TEXT NOT NULL,
    total_size INTEGER NOT NULL,
    file_count INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    source_path TEXT NOT NULL DEFAULT '',
    torrent_data BLOB,
    resume_data BLOB,
    added_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO torrents_new 
SELECT id, info_hash, name, total_size, file_count, status, source_path, torrent_data, resume_data, added_at
FROM torrents;

DROP TABLE torrents;
ALTER TABLE torrents_new RENAME TO torrents;

CREATE INDEX idx_torrents_info_hash ON torrents(info_hash);
CREATE INDEX idx_torrents_status ON torrents(status);
CREATE INDEX idx_torrents_source_path ON torrents(source_path);
