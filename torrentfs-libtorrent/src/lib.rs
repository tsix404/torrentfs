//! libtorrent integration for TorrentFS.

pub mod session;
pub mod torrent;

pub use session::Session;

/// Main integration point for libtorrent.
#[derive(Debug)]
pub struct LibtorrentIntegration;

impl LibtorrentIntegration {
    /// Creates a new libtorrent integration instance.
    pub fn new() -> Self {
        Self
    }
}