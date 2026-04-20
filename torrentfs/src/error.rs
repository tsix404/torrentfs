//! Error types for TorrentFS core.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Invalid torrent: {0}")]
    InvalidTorrent(String),

    #[error("Torrent not found")]
    TorrentNotFound,

    #[error("Internal error: {0}")]
    Internal(String),
}