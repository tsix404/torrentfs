//! Metadata management.

use anyhow::Result;

/// Manages torrent metadata.
#[derive(Debug)]
pub struct MetadataManager;

impl MetadataManager {
    /// Creates a new metadata manager.
    pub fn new() -> Result<Self> {
        Ok(Self)
    }
}