CREATE TABLE torrents_new (
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
);

INSERT INTO torrents_new 
SELECT * FROM torrents;

DROP TABLE torrents;
ALTER TABLE torrents_new RENAME TO torrents;

CREATE INDEX idx_torrents_info_hash ON torrents(info_hash);
CREATE INDEX idx_torrents_info_hash_source_path ON torrents(info_hash, source_path);
CREATE INDEX idx_torrents_status ON torrents(status);
CREATE INDEX idx_torrents_source_path ON torrents(source_path);
