pub mod database;
pub mod error;
pub mod metadata;
pub mod repo;
pub mod runtime;

pub use database::Database;
pub use metadata::MetadataManager;
pub use repo::TorrentRepo;
pub use runtime::TorrentRuntime;

pub async fn init() -> anyhow::Result<TorrentRuntime> {
    let runtime = TorrentRuntime::new().await?;
    tracing::info!("TorrentFS core initialized");
    Ok(runtime)
}
