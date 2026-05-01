pub mod alert_loop;
pub mod database;
pub mod download;
pub mod error;
pub mod metadata;
pub mod piece_cache;
pub mod repo;
pub mod runtime;

pub use alert_loop::{AlertLoop, AlertLoopMessage};
pub use database::Database;
pub use download::DownloadCoordinator;
pub use metadata::MetadataManager;
pub use piece_cache::PieceCache;
pub use repo::{TorrentRepo, TorrentWithData};
pub use runtime::TorrentRuntime;

pub async fn init(state_dir: &std::path::Path) -> anyhow::Result<TorrentRuntime> {
    let runtime = TorrentRuntime::new(state_dir).await?;
    tracing::info!("TorrentFS core initialized with alert loop");
    Ok(runtime)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_init_returns_ok() {
        let temp_dir = TempDir::new().unwrap();
        let result = init(temp_dir.path()).await;
        assert!(result.is_ok(), "init() should return Ok: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_init_creates_torrent_runtime() {
        let temp_dir = TempDir::new().unwrap();
        let runtime = init(temp_dir.path()).await.unwrap();
        assert!(runtime.db.pool().acquire().await.is_ok());
    }
}
