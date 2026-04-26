use thiserror::Error;

#[derive(Debug, Error)]
pub enum TorrentFsError {
    #[error("Database error: {0}")]
    Db(#[from] sqlx::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Not found: {0}")]
    NotFound(String),
}

pub type Result<T> = std::result::Result<T, TorrentFsError>;
