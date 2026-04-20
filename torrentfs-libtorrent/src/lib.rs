//! libtorrent integration for TorrentFS.

pub mod session;
pub mod torrent;

pub use session::Session;

pub struct LibtorrentIntegration;

impl LibtorrentIntegration {
    pub fn new() -> Self {
        Self
    }
}