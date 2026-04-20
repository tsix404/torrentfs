//! Core service for TorrentFS.

pub mod error;
pub mod metadata;
pub mod runtime;

/// Initializes the core service.
pub async fn init() -> anyhow::Result<()> {
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