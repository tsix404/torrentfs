use thiserror::Error;

#[derive(Error, Debug)]
pub enum TorrentfsError {
    #[error("Initialization error: {0}")]
    Initialization(String),

    #[error("Mount error: {0}")]
    Mount(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
