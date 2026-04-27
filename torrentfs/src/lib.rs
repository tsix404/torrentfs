pub mod database;
pub mod error;
pub mod metadata;
pub mod piece_cache;
pub mod repo;
pub mod runtime;

pub use database::Database;
pub use metadata::MetadataManager;
pub use piece_cache::PieceCache;
pub use repo::TorrentRepo;
pub use runtime::TorrentRuntime;

pub async fn init() -> anyhow::Result<TorrentRuntime> {
    let runtime = TorrentRuntime::new().await?;
    tracing::info!("TorrentFS core initialized");
    Ok(runtime)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_init_returns_ok() {
        let result = init().await;
        assert!(result.is_ok(), "init() should return Ok: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_init_creates_torrent_runtime() {
        let runtime = init().await.unwrap();
        assert_eq!(runtime.db.pool().acquire().await.is_ok(), true);
    }
}
