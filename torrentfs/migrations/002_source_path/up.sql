-- Add source_path column for subdirectory mirroring support
ALTER TABLE torrents ADD COLUMN source_path TEXT NOT NULL DEFAULT '';

-- Create index for source_path queries
CREATE INDEX idx_torrents_source_path ON torrents(source_path);
