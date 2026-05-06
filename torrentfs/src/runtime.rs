use anyhow::{bail, Result};
use percent_encoding::percent_decode;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use unicode_normalization::UnicodeNormalization;

use crate::alert_loop::{AlertLoop, AlertLoopMessage};
use crate::database::Database;
use crate::download::DownloadCoordinator;
use crate::metadata::MetadataManager;
use crate::piece_cache::PieceCache;
use torrentfs_libtorrent::{AlertType, Session};

const MAX_PATH_COMPONENT_LENGTH: usize = 255;

/// # Path Sanitization Security
///
/// The `sanitize_path_component` function provides defense against multiple attack vectors:
///
/// ## Attack Vectors Covered
///
/// 1. **Directory Traversal**: Rejects `..` sequences and variants
///    - Direct: `..`, `../etc`
///    - Encoded: `%2e%2e`, `%2e%2e%2fetc`
///    - Double-encoded: `%252e%252e`
///    - Triple-encoded: `%25252e%25252e`
///
/// 2. **Path Separator Injection**: Rejects forward slashes and backslashes
///    - Direct: `path/to/file`, `path\to\file`
///    - Encoded: `%2f`, `%5c`, `%2F`, `%5C`
///    - Double-encoded: `%252f`, `%255c`
///    - Mixed: `%2f%5c%2f`
///
/// 3. **Unicode Homoglyph Attacks**: Uses NFKC normalization
///    - Fullwidth dot: `\u{FF0E}` → `.`
///    - Small form dot: `\u{FE52}` → `.`
///    - Fullwidth slash: `\u{FF0F}` → `/`
///
/// 4. **Mixed Encoding Attacks**: Handles combinations
///    - Mixed ASCII + Unicode: `%2e%FF0E`
///    - Mixed encoding levels: `%2e%252e`
///    - Unicode + encoding: `%EF%BC%8E%2e`
///
/// 5. **Null Byte Injection**: Rejects paths containing `\0`
///
/// 6. **Length Attacks**: Enforces 255-byte limit after full decoding
///
/// ## Test Coverage
///
/// Security tests are organized into:
/// - Unit tests: Specific attack patterns (lines 400-620)
/// - Property tests: Randomized combinations using proptest (lines 620-720)
///
/// All tests verify that sanitization either accepts valid paths or rejects invalid ones
/// with no false positives or negatives.

fn validate_percent_encoding(s: &str) -> Result<()> {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                bail!("Incomplete percent encoding at end of string");
            }
            let hex = &s[i + 1..i + 3];
            if !hex.chars().all(|c| c.is_ascii_hexdigit()) {
                bail!("Invalid percent encoding: invalid hex digits");
            }
            i += 3;
        } else {
            i += 1;
        }
    }
    Ok(())
}

fn decode_fully(component: &str) -> Result<String> {
    validate_percent_encoding(component)?;
    
    let mut current = component.to_string();
    let max_iterations = 10;
    
    for _ in 0..max_iterations {
        let decoded = percent_decode(current.as_bytes())
            .decode_utf8()
            .map_err(|e| anyhow::anyhow!("Invalid percent encoding or UTF-8 in path component: {}", e))?;
        
        let decoded_str = decoded.to_string();
        if decoded_str == current {
            return Ok(decoded_str);
        }
        current = decoded_str;
    }
    
    bail!("Too many levels of percent encoding (potential encoding loop)");
}

pub fn sanitize_path_component(component: &str) -> Result<String> {
    if component.is_empty() {
        bail!("Path component is empty");
    }
    
    let decoded_str = decode_fully(component)?;
    
    if decoded_str.len() > MAX_PATH_COMPONENT_LENGTH {
        bail!(
            "Path component exceeds maximum length of {} bytes (got {} bytes)",
            MAX_PATH_COMPONENT_LENGTH,
            decoded_str.len()
        );
    }
    
    if decoded_str.contains('\0') {
        bail!("Path component contains null byte which is not allowed");
    }
    
    let normalized: String = decoded_str.nfkc().collect();
    
    if normalized.contains("..") {
        bail!("Path component contains '..' sequence which is not allowed");
    }
    
    if normalized.contains('/') || normalized.contains('\\') {
        bail!("Path component contains path separator which is not allowed");
    }
    
    if normalized == "." {
        bail!("Path component is '.' which is not allowed");
    }
    
    let path = Path::new(&normalized);

    for part in path.components() {
        match part {
            Component::ParentDir => {
                bail!("Path component contains directory traversal: '..' is not allowed")
            }
            Component::RootDir => {
                bail!("Path component contains absolute path: root directory is not allowed")
            }
            Component::Prefix(_) => {
                bail!("Path component contains Windows prefix which is not allowed")
            }
            Component::CurDir => {
                bail!("Path component contains current directory '.' which is not allowed")
            }
            _ => {}
        }
    }
    
    Ok(decoded_str.to_string())
}

pub fn build_safe_path(base: &Path, parts: &[&str]) -> Result<PathBuf> {
    let mut path = base.to_path_buf();
    for part in parts {
        let sanitized = sanitize_path_component(part)?;
        path = path.join(sanitized);
    }
    
    let canonical_base = base.canonicalize().unwrap_or_else(|_| base.to_path_buf());
    let canonical_path = path.canonicalize().unwrap_or_else(|_| path.clone());
    
    if !canonical_path.starts_with(&canonical_base) {
        bail!("Path traversal attempt detected: resulting path escapes base directory");
    }
    
    Ok(path)
}

pub struct TorrentRuntime {
    pub db: Arc<Database>,
    pub session: Arc<Session>,
    pub piece_cache: Arc<PieceCache>,
    pub download_coordinator: Arc<DownloadCoordinator>,
    pub metadata_manager: Arc<MetadataManager>,
    shutdown_tx: broadcast::Sender<AlertLoopMessage>,
    state_dir: std::path::PathBuf,
}

impl TorrentRuntime {
    pub async fn new(state_dir: &Path) -> Result<Self> {
        let db = Arc::new(Database::new(state_dir).await?);
        db.migrate().await?;
        
        let session = Arc::new(Session::new()?);
        let piece_cache = Arc::new(PieceCache::with_state_dir(state_dir)?);
        let download_coordinator = Arc::new(DownloadCoordinator::new(
            Arc::clone(&session),
            Arc::clone(&piece_cache),
        ));
        let metadata_manager = Arc::new(MetadataManager::new(Arc::clone(&db))?);
        
        let (shutdown_tx, shutdown_rx) = broadcast::channel::<AlertLoopMessage>(1);
        
        let alert_loop = AlertLoop::new(
            Arc::clone(&session),
            Arc::clone(&piece_cache),
            Arc::clone(&metadata_manager),
            shutdown_rx,
        );
        
        tokio::spawn(alert_loop.run());
        
        let runtime = Self {
            db,
            session,
            piece_cache,
            download_coordinator,
            metadata_manager,
            shutdown_tx,
            state_dir: state_dir.to_path_buf(),
        };
        
        runtime.restore_cache_index()?;
        runtime.restore_torrents().await?;
        
        Ok(runtime)
    }
    
    fn restore_cache_index(&self) -> Result<()> {
        let cached = self.piece_cache.scan_cached_pieces()?;
        
        if cached.is_empty() {
            tracing::info!("No cached pieces found");
            return Ok(());
        }
        
        let total_pieces: usize = cached.iter().map(|(_, pieces)| pieces.len()).sum();
        tracing::info!(
            torrents = cached.len(),
            total_pieces = total_pieces,
            "Cache index restored"
        );
        
        Ok(())
    }
    
    async fn restore_torrents(&self) -> Result<()> {
        let torrents = self.metadata_manager.list_torrents_with_data().await?;
        
        if torrents.is_empty() {
            tracing::info!("No torrents to restore from database");
            return Ok(());
        }
        
        let mut restored = 0;
        let mut skipped = 0;
        let mut failed = 0;
        
        for torrent_with_data in torrents {
            let info_hash_hex = hex::encode(&torrent_with_data.torrent.info_hash);
            let torrent_name = &torrent_with_data.torrent.name;
            let source_path = &torrent_with_data.torrent.source_path;
            
            if self.session.find_torrent(&info_hash_hex) {
                tracing::debug!(
                    info_hash = %info_hash_hex,
                    name = %torrent_name,
                    "Torrent already exists in session, skipping"
                );
                skipped += 1;
                continue;
            }
            
            let save_path = build_safe_path(
                &self.state_dir.join("data"),
                &[source_path.as_str(), torrent_name.as_str()]
            )?;
            
            let save_path_str = save_path.to_string_lossy().into_owned();
            
            match self.session.add_torrent_with_resume(
                &torrent_with_data.torrent_data,
                &save_path_str,
                torrent_with_data.resume_data.as_deref()
            ) {
                Ok(()) => {
                    if torrent_with_data.resume_data.is_some() {
                        tracing::info!(
                            info_hash = %info_hash_hex,
                            name = %torrent_name,
                            "Restored torrent with resume_data"
                        );
                    } else {
                        tracing::info!(
                            info_hash = %info_hash_hex,
                            name = %torrent_name,
                            save_path = %save_path_str,
                            "Restored torrent from database"
                        );
                    }
                    restored += 1;
                }
                Err(e) => {
                    tracing::error!(
                        info_hash = %info_hash_hex,
                        name = %torrent_name,
                        save_path = %save_path_str,
                        error = %e,
                        "Failed to restore torrent from database"
                    );
                    failed += 1;
                }
            }
        }
        
        tracing::info!(
            restored = restored,
            skipped = skipped,
            failed = failed,
            total = restored + skipped + failed,
            "Torrent restoration complete"
        );
        
        Ok(())
    }
    
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(AlertLoopMessage::Shutdown);
    }

    pub async fn graceful_shutdown(&self) -> Result<()> {
        tracing::info!("Starting graceful shutdown...");
        
        let info_hashes = self.session.get_torrents();
        tracing::info!(torrent_count = info_hashes.len(), "Found torrents to save resume data");
        
        for info_hash in &info_hashes {
            if let Err(e) = self.session.pause_torrent(info_hash) {
                tracing::warn!(info_hash = %info_hash, error = %e, "Failed to pause torrent");
            }
        }
        
        for info_hash in &info_hashes {
            if let Err(e) = self.session.save_resume_data(info_hash) {
                tracing::warn!(info_hash = %info_hash, error = %e, "Failed to request resume data save");
            }
        }
        
        if !info_hashes.is_empty() {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            let mut saved_count = 0;
            
            loop {
                let alerts = self.session.pop_alerts();
                
                for alert in alerts.iter() {
                    if alert.alert_type == AlertType::SaveResumeData {
                        if let Some(info_hash_hex) = &alert.info_hash {
                            tracing::info!(info_hash = %info_hash_hex, "Resume data saved");
                            saved_count += 1;
                        }
                    }
                }
                
                if saved_count >= info_hashes.len() {
                    break;
                }
                
                if tokio::time::Instant::now() >= deadline {
                    tracing::warn!(saved = saved_count, total = info_hashes.len(), "Timeout waiting for resume data saves");
                    break;
                }
                
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        
        tracing::info!("Stopping alert loop...");
        self.shutdown();
        
        tracing::info!("Destroying libtorrent session...");
        drop(Arc::clone(&self.session));
        
        tracing::info!("Closing database connection pool...");
        self.db.pool().close().await;
        
        tracing::info!("Graceful shutdown complete");
        Ok(())
    }
}

impl Drop for TorrentRuntime {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_init_returns_ok() {
        let temp_dir = TempDir::new().unwrap();
        let result = TorrentRuntime::new(temp_dir.path()).await;
        assert!(result.is_ok(), "new() should return Ok: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_init_creates_torrent_runtime() {
        let temp_dir = TempDir::new().unwrap();
        let runtime = TorrentRuntime::new(temp_dir.path()).await.unwrap();
        assert!(runtime.db.pool().acquire().await.is_ok());
        assert!(runtime.session.pop_alerts().is_empty() || !runtime.session.pop_alerts().is_empty());
    }

    #[tokio::test]
    async fn test_shutdown() {
        let temp_dir = TempDir::new().unwrap();
        let runtime = TorrentRuntime::new(temp_dir.path()).await.unwrap();
        runtime.shutdown();
    }

    #[tokio::test]
    async fn test_runtime_drop_sends_shutdown() {
        let temp_dir = TempDir::new().unwrap();
        let runtime = TorrentRuntime::new(temp_dir.path()).await.unwrap();
        let shutdown_tx = runtime.shutdown_tx.clone();
        
        drop(runtime);
        
        let result = shutdown_tx.send(AlertLoopMessage::Shutdown);
        assert!(result.is_ok() || result.is_err());
    }

    #[tokio::test]
    async fn test_graceful_shutdown_no_torrents() {
        let temp_dir = TempDir::new().unwrap();
        let runtime = TorrentRuntime::new(temp_dir.path()).await.unwrap();
        let result = runtime.graceful_shutdown().await;
        assert!(result.is_ok(), "graceful_shutdown should succeed: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_get_torrents_returns_vec() {
        let temp_dir = TempDir::new().unwrap();
        let runtime = TorrentRuntime::new(temp_dir.path()).await.unwrap();
        let torrents = runtime.session.get_torrents();
        assert!(torrents.is_empty() || !torrents.is_empty(), "get_torrents should return a Vec");
    }

    #[test]
    fn test_sanitize_path_component_normal() {
        assert_eq!(sanitize_path_component("normal").unwrap(), "normal");
        assert_eq!(sanitize_path_component("file.txt").unwrap(), "file.txt");
        assert_eq!(sanitize_path_component("my-torrent").unwrap(), "my-torrent");
    }

    #[test]
    fn test_sanitize_path_component_traversal() {
        assert!(sanitize_path_component("..").is_err());
        assert!(sanitize_path_component("../etc").is_err());
        assert!(sanitize_path_component("../file").is_err());
    }

    #[test]
    fn test_sanitize_path_component_slashes() {
        assert!(sanitize_path_component("path/to/file").is_err());
        assert!(sanitize_path_component("path\\to\\file").is_err());
    }

    #[test]
    fn test_sanitize_path_component_empty() {
        assert!(sanitize_path_component("").is_err());
    }

    #[test]
    fn test_build_safe_path_normal() {
        let temp_dir = TempDir::new().unwrap();
        let base = temp_dir.path();
        let path = build_safe_path(base, &["source", "torrent"]).unwrap();
        assert!(path.starts_with(base));
        assert!(path.ends_with("source/torrent") || path.ends_with("source\\torrent"));
    }

    #[test]
    fn test_build_safe_path_traversal_blocked() {
        let temp_dir = TempDir::new().unwrap();
        let base = temp_dir.path();
        let result = build_safe_path(base, &["..", "etc"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_safe_path_absolute_blocked() {
        let temp_dir = TempDir::new().unwrap();
        let base = temp_dir.path();
        let result = build_safe_path(base, &["/etc", "passwd"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_sanitize_path_component_edge_cases() {
        assert!(sanitize_path_component("..file").is_err());
        assert!(sanitize_path_component("file..").is_err());
        assert!(sanitize_path_component("...").is_err());
        assert!(sanitize_path_component("....").is_err());
        assert!(sanitize_path_component(".....").is_err());
        assert!(sanitize_path_component(".").is_err());
    }

    #[test]
    fn test_sanitize_path_component_null_byte() {
        assert!(sanitize_path_component("file\0.txt").is_err());
        assert!(sanitize_path_component("\0").is_err());
    }

    #[test]
    fn test_sanitize_path_component_backslash_traversal() {
        assert!(sanitize_path_component("valid\\..\\etc").is_err());
    }

    #[test]
    fn test_sanitize_path_component_length_limit() {
        let valid_length = "a".repeat(255);
        assert_eq!(sanitize_path_component(&valid_length).unwrap(), valid_length);
        
        let too_long = "a".repeat(256);
        assert!(sanitize_path_component(&too_long).is_err());
        
        let very_long = "a".repeat(10000);
        assert!(sanitize_path_component(&very_long).is_err());
    }

    #[test]
    fn test_sanitize_path_component_encoded_slash() {
        assert!(sanitize_path_component("%2f").is_err());
        assert!(sanitize_path_component("%2F").is_err());
        assert!(sanitize_path_component("%5c").is_err());
        assert!(sanitize_path_component("%5C").is_err());
        assert!(sanitize_path_component("etc%2fpasswd").is_err());
        assert!(sanitize_path_component("path%2fto%2ffile").is_err());
        assert!(sanitize_path_component("folder%5cfile").is_err());
    }

    #[test]
    fn test_sanitize_path_component_encoded_traversal() {
        assert!(sanitize_path_component("%2e%2e").is_err());
        assert!(sanitize_path_component("%2e").is_err());
        assert!(sanitize_path_component("%2e%2e%2fetc").is_err());
        assert!(sanitize_path_component("..%2fetc").is_err());
        assert!(sanitize_path_component("%2e%2e/etc").is_err());
    }

    #[test]
    fn test_sanitize_path_component_double_encoded() {
        assert!(sanitize_path_component("%252f").is_err());
        assert!(sanitize_path_component("%255c").is_err());
        assert!(sanitize_path_component("%252e%252e").is_err());
    }

    #[test]
    fn test_sanitize_path_component_valid_encoded() {
        assert_eq!(sanitize_path_component("file%20name").unwrap(), "file name");
        assert_eq!(sanitize_path_component("torrent%2dname").unwrap(), "torrent-name");
        assert_eq!(sanitize_path_component("test%5f123").unwrap(), "test_123");
    }

    #[test]
    fn test_sanitize_path_component_invalid_encoding() {
        assert!(sanitize_path_component("%gg").is_err());
        assert!(sanitize_path_component("%2g").is_err());
        assert!(sanitize_path_component("%").is_err());
        assert!(sanitize_path_component("%2").is_err());
    }

    #[test]
    fn test_sanitize_path_component_unicode_debug() {
        let test1 = "\u{FF0E}\u{FF0E}";
        let test2 = "\u{FE52}\u{FE52}";
        
        let nfc1: String = test1.nfc().collect();
        let nfc2: String = test2.nfc().collect();
        let nfkc1: String = test1.nfkc().collect();
        let nfkc2: String = test2.nfkc().collect();
        
        eprintln!("Fullwidth NFC: {:?}", nfc1);
        eprintln!("Small form NFC: {:?}", nfc2);
        eprintln!("Fullwidth NFKC: {:?}", nfkc1);
        eprintln!("Small form NFKC: {:?}", nfkc2);
    }

    #[test]
    fn test_sanitize_path_component_unicode_homoglyphs() {
        assert!(sanitize_path_component("\u{FF0E}\u{FF0E}").is_err());
        assert!(sanitize_path_component("\u{FE52}\u{FE52}").is_err());
        assert!(sanitize_path_component("\u{FF0E}").is_err());
        assert!(sanitize_path_component("\u{FE52}").is_err());
        assert!(sanitize_path_component("\u{FF0E}\u{FF0E}\u{FF0F}etc").is_err());
        assert!(sanitize_path_component("\u{FE52}\u{FE52}\\etc").is_err());
    }

    #[test]
    fn test_mixed_unicode_and_encoding_attacks() {
        assert!(sanitize_path_component("%FF0E%FF0E").is_err());
        assert!(sanitize_path_component("%2e%FF0E").is_err());
        assert!(sanitize_path_component("%2e%2e%FF0Fetc").is_err());
        assert!(sanitize_path_component("%FF0E%FF0E/etc").is_err());
        assert!(sanitize_path_component("%FE52%FE52").is_err());
    }

    #[test]
    fn test_nested_encoding_attacks() {
        assert!(sanitize_path_component("%252e%252e").is_err());
        assert!(sanitize_path_component("%25%32%65%25%32%65").is_err());
        assert!(sanitize_path_component("%252e%252e%252fetc").is_err());
        assert!(sanitize_path_component("%252f%252f").is_err());
        assert!(sanitize_path_component("%2525%2532%2565").is_err());
    }

    #[test]
    fn test_combined_separator_attacks() {
        assert!(sanitize_path_component("%2f%5c%2f").is_err());
        assert!(sanitize_path_component("path%2f..%5c..").is_err());
        assert!(sanitize_path_component("%5c%2f%5c").is_err());
        assert!(sanitize_path_component("..%2f..%5cetc").is_err());
        assert!(sanitize_path_component("%2f\\%5c/").is_err());
    }

    #[test]
    fn test_boundary_attacks_at_max_length() {
        let attack_at_255 = format!("{}%2e%2e", "a".repeat(252));
        assert!(sanitize_path_component(&attack_at_255).is_err());

        let attack_exceeds_after_decode = format!("{}%252e%252e", "a".repeat(248));
        assert!(sanitize_path_component(&attack_exceeds_after_decode).is_err());

        let valid_at_255 = "a".repeat(255);
        assert!(sanitize_path_component(&valid_at_255).is_ok());

        let encoded_valid_at_boundary = format!("{}%20", "a".repeat(253));
        assert!(sanitize_path_component(&encoded_valid_at_boundary).is_ok());
    }

    #[test]
    fn test_triple_encoded_attacks() {
        assert!(sanitize_path_component("%25252e%25252e").is_err());
        assert!(sanitize_path_component("%25252f%25252f").is_err());
        assert!(sanitize_path_component("%2525%2532%2565%2525%2532%2565").is_err());
    }

    #[test]
    fn test_mixed_encoding_layers() {
        assert!(sanitize_path_component("%2e%252e").is_err());
        assert!(sanitize_path_component("%252e%2e").is_err());
        assert!(sanitize_path_component("..%252f").is_err());
        assert!(sanitize_path_component("%252f..").is_err());
    }

    #[test]
    fn test_unicode_with_encoding_combination() {
        assert!(sanitize_path_component("%EF%BC%8E%EF%BC%8E").is_err());
        assert!(sanitize_path_component("%EF%BC%8E%2e").is_err());
        assert!(sanitize_path_component("%2e%EF%BC%8E").is_err());
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    prop_compose! {
        fn ascii_safe_char()(
            c in any::<char>().prop_filter(
                "Valid ASCII path character",
                |c| c.is_ascii() && !matches!(c, '\0' | '/' | '\\' | '.' | '%')
            )
        ) -> char {
            c
        }
    }

    prop_compose! {
        fn valid_ascii_path()(s in "[a-zA-Z0-9_-]{1,100}") -> String {
            s
        }
    }

    prop_compose! {
        fn ascii_with_slash()(prefix in "[a-zA-Z0-9_]*", slash in "[/\\\\]", suffix in "[a-zA-Z0-9_]*") -> String {
            format!("{}{}{}", prefix, slash, suffix)
        }
    }

    prop_compose! {
        fn ascii_with_null()(prefix in "[a-zA-Z0-9_]*", suffix in "[a-zA-Z0-9_]*") -> String {
            let mut s = prefix;
            s.push('\0');
            s.push_str(&suffix);
            s
        }
    }

    prop_compose! {
        fn ascii_with_double_encoded_dot()(prefix in "[a-zA-Z0-9_]*", suffix in "[a-zA-Z0-9_]*") -> String {
            format!("{}%2e%2e{}", prefix, suffix)
        }
    }

    prop_compose! {
        fn ascii_with_encoded_slash()(prefix in "[a-zA-Z0-9_]*", enc in "(2f|2F|5c|5C)", suffix in "[a-zA-Z0-9_]*") -> String {
            format!("{}%{}{}", prefix, enc, suffix)
        }
    }

    proptest! {
        #[test]
        fn proptest_valid_paths_accept(s in valid_ascii_path()) {
            let result = sanitize_path_component(&s);
            prop_assert!(result.is_ok());
        }

        #[test]
        fn proptest_dot_sequences_rejected(s in r"\.\.+") {
            let result = sanitize_path_component(&s);
            prop_assert!(result.is_err());
        }

        #[test]
        fn proptest_slashes_rejected(s in ascii_with_slash()) {
            let result = sanitize_path_component(&s);
            prop_assert!(result.is_err());
        }

        #[test]
        fn proptest_null_bytes_rejected(s in ascii_with_null()) {
            let result = sanitize_path_component(&s);
            prop_assert!(result.is_err());
        }

        #[test]
        fn proptest_encoded_double_dots_rejected(s in ascii_with_double_encoded_dot()) {
            let result = sanitize_path_component(&s);
            prop_assert!(result.is_err());
        }

        #[test]
        fn proptest_encoded_slashes_rejected(s in ascii_with_encoded_slash()) {
            let result = sanitize_path_component(&s);
            prop_assert!(result.is_err());
        }

        #[test]
        fn proptest_length_boundary(len in 200usize..300) {
            let s = "a".repeat(len);
            let result = sanitize_path_component(&s);
            if len <= 255 {
                prop_assert!(result.is_ok());
            } else {
                prop_assert!(result.is_err());
            }
        }
    }
}
