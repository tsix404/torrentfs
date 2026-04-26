//! Core service for TorrentFS.

pub mod database;
pub mod error;
pub mod metadata;
pub mod runtime;

/// Initializes the core service.
pub async fn init() -> anyhow::Result<()> {
    // Initialize database
    let db = database::Database::new().await?;
    db.migrate().await?;
    
    tracing::info!("Database initialized and migrations applied");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_init() {
        assert!(init().await.is_ok());
    }
}