//! libtorrent integration for TorrentFS.

pub mod alert;
pub mod error;
pub mod session;
pub mod torrent;
pub mod validator;

pub use alert::{Alert, AlertList, AlertType};
pub use error::{LibtorrentError, LibtorrentErrorCode};
pub use session::Session;
pub use torrent::{parse_torrent, TorrentInfo};
pub use validator::{TorrentValidator, TorrentMetadata, ValidationError};

/// Main integration point for libtorrent.
#[derive(Debug)]
pub struct LibtorrentIntegration;

impl LibtorrentIntegration {
    /// Creates a new libtorrent integration instance.
    pub fn new() -> Self {
        Self
    }
}