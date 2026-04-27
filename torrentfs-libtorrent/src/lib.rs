//! libtorrent integration for TorrentFS.

pub mod alert;
pub mod session;
pub mod torrent;

pub use alert::{Alert, AlertList, AlertType};
pub use session::Session;
pub use torrent::{parse_torrent, TorrentInfo};

/// Main integration point for libtorrent.
#[derive(Debug)]
pub struct LibtorrentIntegration;

impl LibtorrentIntegration {
    /// Creates a new libtorrent integration instance.
    pub fn new() -> Self {
        Self
    }
}