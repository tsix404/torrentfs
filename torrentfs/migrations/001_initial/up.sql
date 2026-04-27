-- Create torrents table
CREATE TABLE torrents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    info_hash BLOB NOT NULL UNIQUE,
    name TEXT NOT NULL,
    total_size INTEGER NOT NULL,
    piece_size INTEGER NOT NULL DEFAULT 16384,
    file_count INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    added_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create torrent_files table
CREATE TABLE torrent_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    torrent_id INTEGER NOT NULL,
    path TEXT NOT NULL,
    size INTEGER NOT NULL,
    FOREIGN KEY (torrent_id) REFERENCES torrents(id) ON DELETE CASCADE,
    UNIQUE(torrent_id, path)
);

-- Create indexes for faster queries
CREATE INDEX idx_torrents_info_hash ON torrents(info_hash);
CREATE INDEX idx_torrents_status ON torrents(status);
CREATE INDEX idx_torrent_files_torrent_id ON torrent_files(torrent_id);
CREATE INDEX idx_torrent_files_path ON torrent_files(path);