pub mod alert_loop;
pub mod database;
pub mod download;
pub mod error;
pub mod metadata;
pub mod piece_cache;
pub mod repo;
pub mod resume_saver;
pub mod runtime;

pub use alert_loop::{AlertLoop, AlertLoopMessage};
pub use database::Database;
pub use download::DownloadCoordinator;
pub use metadata::MetadataManager;
pub use piece_cache::PieceCache;
pub use repo::{TorrentRepo, TorrentWithData, Directory};
pub use resume_saver::{ResumeSaver, ResumeSaverConfig};
pub use runtime::{TorrentRuntime, TorrentRuntimeConfig, sanitize_path_component, build_safe_path};

pub async fn init(state_dir: &std::path::Path) -> anyhow::Result<TorrentRuntime> {
    let runtime = TorrentRuntime::new(state_dir).await?;
    tracing::info!("TorrentFS core initialized with alert loop and resume saver");
    Ok(runtime)
}

pub async fn init_with_config(
    state_dir: &std::path::Path,
    config: TorrentRuntimeConfig,
) -> anyhow::Result<TorrentRuntime> {
    let runtime = TorrentRuntime::with_config(state_dir, config).await?;
    tracing::info!("TorrentFS core initialized with custom config");
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
