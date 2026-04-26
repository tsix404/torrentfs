//! libtorrent session management.

use anyhow::Result;

/// libtorrent session wrapper.
#[derive(Debug)]
pub struct Session;

impl Session {
    /// Creates a new libtorrent session.
    pub fn new() -> Result<Self> {
        Ok(Self)
    }
}