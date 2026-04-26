//! Torrent handle wrapper.

use anyhow::Result;

/// Wrapper for a libtorrent torrent handle.
#[derive(Debug)]
pub struct TorrentHandle;

impl TorrentHandle {
    /// Creates a new torrent handle.
    pub fn new() -> Result<Self> {
        Ok(Self)
    }
}