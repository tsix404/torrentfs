-- Drop indexes
DROP INDEX IF EXISTS idx_torrent_files_path;
DROP INDEX IF EXISTS idx_torrent_files_torrent_id;
DROP INDEX IF EXISTS idx_torrents_status;
DROP INDEX IF EXISTS idx_torrents_info_hash;

-- Drop tables
DROP TABLE IF EXISTS torrent_files;
DROP TABLE IF EXISTS torrents;