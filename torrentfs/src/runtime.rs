use anyhow::Result;
use std::sync::Arc;

use crate::database::Database;

pub struct TorrentRuntime {
    pub db: Arc<Database>,
}

impl TorrentRuntime {
    pub async fn new() -> Result<Self> {
        let db = Database::new().await?;
        db.migrate().await?;
        Ok(Self { db: Arc::new(db) })
    }
}
