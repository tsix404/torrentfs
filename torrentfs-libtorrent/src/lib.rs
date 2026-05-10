//! libtorrent integration for TorrentFS.
//!
//! This crate provides safe Rust bindings to libtorrent-rasterbar for torrent
//! file parsing and BitTorrent session management.
//!
//! # Features
//!
//! - Parse torrent files and extract metadata
//! - Cache parsed results for performance
//! - Session management for torrent downloads
//! - Alert handling for torrent events
//!
//! # Example
//!
//! ```no_run
//! use torrentfs_libtorrent::{parse_torrent, TorrentInfo};
//!
//! // Parse a torrent file
//! let data = std::fs::read("example.torrent").unwrap();
//! let info = parse_torrent(&data).unwrap();
//!
//! // Access metadata
//! println!("Name: {}", info.name);
//! println!("Info hash: {}", info.info_hash);
//! println!("Total size: {} bytes", info.total_size);
//! println!("Files: {:?}", info.files);
//! println!("Trackers: {:?}", info.trackers);
//! ```

pub mod alert;
pub mod error;
pub mod session;
pub mod session_manager;
pub mod torrent;
pub mod validator;

pub use alert::{Alert, AlertList, AlertType};
pub use error::{LibtorrentError, LibtorrentErrorCode};
pub use session::Session;
pub use session_manager::{
    SessionManager, SessionConfig, SessionEvent, TorrentStatus, TorrentProgress, PeerInfo,
};
pub use torrent::{
    parse_torrent, parse_torrent_cached, clear_parse_cache,
    list_files, list_trackers, FileEntry, TrackerEntry, TorrentInfo,
};
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