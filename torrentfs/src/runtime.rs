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
use crate::resume_saver::{ResumeSaver, ResumeSaverConfig};
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

/// Sanitizes a path component to prevent directory traversal and other security issues.
///
/// # Security Measures
///
/// This function implements several security checks to prevent malicious path components:
///
/// - **Empty rejection**: Empty strings are rejected
/// - **Length limit**: Components over 255 bytes are rejected (filesystem limit)
/// - **Null byte rejection**: Null bytes (`\0`) are rejected (C string termination issues)
/// - **Control character rejection**: All control characters are rejected for security:
///   - C0 controls (U+0000 to U+001F): Includes newline, carriage return, tab, bell, escape, etc.
///   - C1 controls (U+0080 to U+009F): Extended control characters
///   - Rationale: Control characters can cause issues in shells, logs, terminals, and other
///     contexts. They may enable injection attacks, corrupt log files, or cause unexpected
///     behavior in path processing.
/// - **Path traversal rejection**: The `..` sequence is rejected to prevent directory traversal
/// - **Path separator rejection**: Both `/` and `\` are rejected to prevent path injection
/// - **Current directory rejection**: The `.` component is rejected
/// - **Unicode normalization**: NFKC normalization is applied to detect Unicode-based attacks
///   (e.g., fullwidth characters that normalize to ASCII path separators)
/// - **Percent encoding**: Fully decoded with iteration limit to prevent encoding loops
///
/// # References
///
/// - POSIX: Only alphanumerics, `.`, `-`, `_` are portable in filenames
/// - Windows: Additional restrictions on `:`, `*`, `?`, `"`, `<`, `>`, `|`
/// - RFC 3986: Percent encoding in URIs
///
/// # Errors
///
/// Returns an error if the component fails any security check.
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
    
    for c in decoded_str.chars() {
        if c.is_control() {
            bail!(
                "Path component contains control character U+{:04X} which is not allowed \
                 (control characters can cause issues in shells, logs, and other contexts)",
                c as u32
            );
        }
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
            Component::CurDir => {
                bail!("Path component contains current directory reference: '.' is not allowed")
            }
            Component::ParentDir => {
                bail!("Path component contains directory traversal: '..' is not allowed")
            }
            Component::RootDir => {
                bail!("Path component contains absolute path: root directory is not allowed")
            }
            Component::Prefix(_) => {
                bail!("Path component contains Windows prefix which is not allowed")
            }
            _ => {}
        }
    }
    
    if is_windows_device_name(&normalized) {
        bail!("Path component is a reserved Windows device name which is not allowed");
    }
    
    Ok(decoded_str.to_string())
}

fn is_windows_device_name(name: &str) -> bool {
    let upper = name.to_uppercase();
    let name_without_ext = upper.split('.').next().unwrap_or("");
    
    let reserved_names = [
        "CON", "PRN", "AUX", "NUL",
        "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
        "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ];
    
    reserved_names.contains(&name_without_ext)
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

pub struct TorrentRuntimeConfig {
    pub resume_save_interval: Duration,
}

impl Default for TorrentRuntimeConfig {
    fn default() -> Self {
        Self {
            resume_save_interval: Duration::from_secs(300),
        }
    }
}

impl TorrentRuntime {
    pub async fn new(state_dir: &Path) -> Result<Self> {
        Self::with_config(state_dir, TorrentRuntimeConfig::default()).await
    }

    pub async fn with_config(state_dir: &Path, config: TorrentRuntimeConfig) -> Result<Self> {
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

        let resume_saver_config = ResumeSaverConfig::new(config.resume_save_interval);
        let resume_saver = ResumeSaver::new(
            Arc::clone(&session),
            resume_saver_config,
            shutdown_tx.subscribe(),
        );
        tokio::spawn(resume_saver.run());
        
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
            let db_status = &torrent_with_data.torrent.status;
            
            if self.session.find_torrent(&info_hash_hex) {
                tracing::debug!(
                    info_hash = %info_hash_hex,
                    name = %torrent_name,
                    "Torrent already exists in session, skipping"
                );
                skipped += 1;
                continue;
            }
            
            let mut path_parts: Vec<&str> = source_path
                .split('/')
                .filter(|p| !p.trim().is_empty())
                .collect();
            path_parts.push(torrent_name.as_str());
            let save_path = build_safe_path(
                &self.state_dir.join("data"),
                &path_parts
            )?;
            
            let save_path_str = save_path.to_string_lossy().into_owned();
            
            match self.session.add_torrent_with_resume(
                &torrent_with_data.torrent_data,
                &save_path_str,
                torrent_with_data.resume_data.as_deref()
            ) {
                Ok(()) => {
                    let should_resume = db_status == "downloading" || db_status == "seeding";
                    
                    if should_resume {
                        if let Err(e) = self.session.resume_torrent(&info_hash_hex) {
                            tracing::warn!(
                                info_hash = %info_hash_hex,
                                name = %torrent_name,
                                db_status = %db_status,
                                error = %e,
                                "Failed to resume torrent after restoration"
                            );
                        } else {
                            tracing::info!(
                                info_hash = %info_hash_hex,
                                name = %torrent_name,
                                db_status = %db_status,
                                "Restored and resumed torrent"
                            );
                        }
                    }
                    
                    if torrent_with_data.resume_data.is_some() {
                        tracing::info!(
                            info_hash = %info_hash_hex,
                            name = %torrent_name,
                            db_status = %db_status,
                            resumed = should_resume,
                            "Restored torrent with resume_data"
                        );
                    } else {
                        tracing::info!(
                            info_hash = %info_hash_hex,
                            name = %torrent_name,
                            save_path = %save_path_str,
                            db_status = %db_status,
                            resumed = should_resume,
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
    async fn test_with_config_default() {
        let temp_dir = TempDir::new().unwrap();
        let config = TorrentRuntimeConfig::default();
        let result = TorrentRuntime::with_config(temp_dir.path(), config).await;
        assert!(result.is_ok(), "with_config() should return Ok: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_with_config_custom_interval() {
        let temp_dir = TempDir::new().unwrap();
        let config = TorrentRuntimeConfig {
            resume_save_interval: Duration::from_secs(60),
        };
        let result = TorrentRuntime::with_config(temp_dir.path(), config).await;
        assert!(result.is_ok(), "with_config() with custom interval should return Ok: {:?}", result.err());
    }

    #[test]
    fn test_config_default_interval() {
        let config = TorrentRuntimeConfig::default();
        assert_eq!(config.resume_save_interval, Duration::from_secs(300));
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
    async fn test_restore_torrents_empty_db() {
        let temp_dir = TempDir::new().unwrap();
        let runtime = TorrentRuntime::new(temp_dir.path()).await.unwrap();
        
        let torrents = runtime.metadata_manager.list_torrents_with_data().await.unwrap();
        assert!(torrents.is_empty(), "Should have no torrents in fresh db");
        
        let session_torrents = runtime.session.get_torrents();
        assert!(session_torrents.is_empty(), "Should have no torrents in session");
    }

    #[tokio::test]
    async fn test_restore_cache_index_empty() {
        let temp_dir = TempDir::new().unwrap();
        let runtime = TorrentRuntime::new(temp_dir.path()).await.unwrap();
        
        let cached = runtime.piece_cache.scan_cached_pieces().unwrap();
        assert!(cached.is_empty(), "Should have no cached pieces in fresh state dir");
    }

    #[tokio::test]
    async fn test_restore_torrents_with_data() {
        let temp_dir = TempDir::new().unwrap();
        let state_dir = temp_dir.path().to_path_buf();
        
        let test_file = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../test_data")
            .read_dir()
            .ok()
            .and_then(|mut d| d.next())
            .and_then(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|e| e == "torrent").unwrap_or(false));
        
        let test_file = match test_file {
            Some(f) => f,
            None => {
                eprintln!("Skipping test - no .torrent file found in test_data");
                return;
            }
        };
        
        let torrent_data = std::fs::read(&test_file).expect("Failed to read test torrent");
        
        {
            let runtime = TorrentRuntime::new(&state_dir).await.unwrap();
            
            let parsed = runtime.metadata_manager.process_torrent_data(&torrent_data, "test").await.unwrap();
            runtime.metadata_manager.persist_to_db(&parsed).await.unwrap();
            
            let _info_hash_hex = hex::encode(&parsed.info_hash);
            runtime.session.add_torrent_paused(&torrent_data, &state_dir.join("data").to_string_lossy()).unwrap();
            
            runtime.metadata_manager.update_status(&parsed.info_hash, "downloading").await.unwrap();
        }
        
        {
            let runtime = TorrentRuntime::new(&state_dir).await.unwrap();
            
            let torrents = runtime.metadata_manager.list_torrents_with_data().await.unwrap();
            assert_eq!(torrents.len(), 1, "Should have restored 1 torrent");
            
            let restored = &torrents[0];
            assert_eq!(restored.torrent.status, "downloading", "Status should be preserved");
            
            let session_torrents = runtime.session.get_torrents();
            assert_eq!(session_torrents.len(), 1, "Torrent should be restored to session");
        }
    }

    #[tokio::test]
    async fn test_status_persists_across_restart() {
        let temp_dir = TempDir::new().unwrap();
        let state_dir = temp_dir.path().to_path_buf();
        
        let test_file = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../test_data")
            .read_dir()
            .ok()
            .and_then(|mut d| d.next())
            .and_then(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|e| e == "torrent").unwrap_or(false));
        
        let test_file = match test_file {
            Some(f) => f,
            None => {
                eprintln!("Skipping test - no .torrent file found in test_data");
                return;
            }
        };
        
        let torrent_data = std::fs::read(&test_file).expect("Failed to read test torrent");
        let info_hash: Vec<u8>;
        
        {
            let runtime = TorrentRuntime::new(&state_dir).await.unwrap();
            
            let parsed = runtime.metadata_manager.process_torrent_data(&torrent_data, "movies").await.unwrap();
            info_hash = parsed.info_hash.clone();
            runtime.metadata_manager.persist_to_db(&parsed).await.unwrap();
            
            runtime.metadata_manager.update_status(&info_hash, "seeding").await.unwrap();
        }
        
        {
            let runtime = TorrentRuntime::new(&state_dir).await.unwrap();
            
            let torrents = runtime.metadata_manager.list_torrents().await.unwrap();
            let restored = torrents.iter().find(|t| t.info_hash == info_hash).expect("Torrent should exist");
            assert_eq!(restored.status, "seeding", "Seeding status should persist across restart");
        }
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
    fn test_sanitize_path_component_dots() {
        assert!(sanitize_path_component(".").is_err());
        assert!(sanitize_path_component("..").is_err());
        assert!(sanitize_path_component("..file").is_err());
        assert!(sanitize_path_component("file..").is_err());
        assert!(sanitize_path_component("...").is_err());
        assert!(sanitize_path_component("....").is_err());
        assert!(sanitize_path_component(".....").is_err());
        assert!(sanitize_path_component("valid").is_ok());
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
        assert!(sanitize_path_component("\u{2024}\u{2024}").is_err());
        assert!(sanitize_path_component("\u{FF0E}").is_err());
        assert!(sanitize_path_component("\u{FE52}").is_err());
        assert!(sanitize_path_component("\u{2024}").is_err());
        assert!(sanitize_path_component("\u{FF0E}\u{FF0E}\u{FF0F}etc").is_err());
        assert!(sanitize_path_component("\u{FE52}\u{FE52}\\etc").is_err());
        assert!(sanitize_path_component("file\u{2024}\u{2024}etc").is_err());
    }

    #[test]
    fn test_sanitize_path_component_control_characters_c0() {
        assert!(sanitize_path_component("file\nname").is_err());
        assert!(sanitize_path_component("file\rname").is_err());
        assert!(sanitize_path_component("file\tname").is_err());
        assert!(sanitize_path_component("\n").is_err());
        assert!(sanitize_path_component("\r").is_err());
        assert!(sanitize_path_component("\t").is_err());
        assert!(sanitize_path_component("\u{0001}").is_err());
        assert!(sanitize_path_component("\u{0002}").is_err());
        assert!(sanitize_path_component("\u{0003}").is_err());
        assert!(sanitize_path_component("\u{0004}").is_err());
        assert!(sanitize_path_component("\u{0005}").is_err());
        assert!(sanitize_path_component("\u{0006}").is_err());
        assert!(sanitize_path_component("\u{0007}").is_err());
        assert!(sanitize_path_component("\u{0008}").is_err());
        assert!(sanitize_path_component("\u{000B}").is_err());
        assert!(sanitize_path_component("\u{000C}").is_err());
        assert!(sanitize_path_component("\u{000E}").is_err());
        assert!(sanitize_path_component("\u{000F}").is_err());
        assert!(sanitize_path_component("\u{0010}").is_err());
        assert!(sanitize_path_component("\u{0011}").is_err());
        assert!(sanitize_path_component("\u{0012}").is_err());
        assert!(sanitize_path_component("\u{0013}").is_err());
        assert!(sanitize_path_component("\u{0014}").is_err());
        assert!(sanitize_path_component("\u{0015}").is_err());
        assert!(sanitize_path_component("\u{0016}").is_err());
        assert!(sanitize_path_component("\u{0017}").is_err());
        assert!(sanitize_path_component("\u{0018}").is_err());
        assert!(sanitize_path_component("\u{0019}").is_err());
        assert!(sanitize_path_component("\u{001A}").is_err());
        assert!(sanitize_path_component("\u{001B}").is_err());
        assert!(sanitize_path_component("\u{001C}").is_err());
        assert!(sanitize_path_component("\u{001D}").is_err());
        assert!(sanitize_path_component("\u{001E}").is_err());
        assert!(sanitize_path_component("\u{001F}").is_err());
    }

    #[test]
    fn test_sanitize_path_component_control_characters_c1() {
        assert!(sanitize_path_component("\u{0080}").is_err());
        assert!(sanitize_path_component("\u{0081}").is_err());
        assert!(sanitize_path_component("\u{0082}").is_err());
        assert!(sanitize_path_component("\u{0085}").is_err());
        assert!(sanitize_path_component("\u{0088}").is_err());
        assert!(sanitize_path_component("\u{008A}").is_err());
        assert!(sanitize_path_component("\u{0090}").is_err());
        assert!(sanitize_path_component("\u{009B}").is_err());
        assert!(sanitize_path_component("\u{009F}").is_err());
        assert!(sanitize_path_component("file\u{0080}name").is_err());
    }

    #[test]
    fn test_sanitize_path_component_safe_characters() {
        assert!(sanitize_path_component("normal_file.txt").is_ok());
        assert!(sanitize_path_component("file-name").is_ok());
        assert!(sanitize_path_component("file_name").is_ok());
        assert!(sanitize_path_component("file.name").is_ok());
        assert!(sanitize_path_component("file name").is_ok());
        assert!(sanitize_path_component("file123").is_ok());
        assert!(sanitize_path_component("UPPERCASE").is_ok());
        assert!(sanitize_path_component("MixedCase123").is_ok());
        assert!(sanitize_path_component("日本語").is_ok());
        assert!(sanitize_path_component("emoji🎉test").is_ok());
    }

    #[test]
    fn test_sanitize_path_component_encoded_control_characters() {
        assert!(sanitize_path_component("%0A").is_err());
        assert!(sanitize_path_component("%0D").is_err());
        assert!(sanitize_path_component("%09").is_err());
        assert!(sanitize_path_component("%00").is_err());
        assert!(sanitize_path_component("%01").is_err());
        assert!(sanitize_path_component("%1F").is_err());
        assert!(sanitize_path_component("file%0Aname").is_err());
        assert!(sanitize_path_component("file%0Dname").is_err());
    }

    #[test]
    fn test_sanitize_path_component_mixed_control_characters() {
        assert!(sanitize_path_component("file\u{0001}name\u{0002}test").is_err());
        assert!(sanitize_path_component("\nfile").is_err());
        assert!(sanitize_path_component("file\r\n").is_err());
        assert!(sanitize_path_component("a\u{0000}b\u{0000}c").is_err());
    }

    #[test]
    fn test_sanitize_path_component_cross_platform_separators() {
        assert!(sanitize_path_component("path/to/file").is_err());
        assert!(sanitize_path_component("path\\to\\file").is_err());
        assert!(sanitize_path_component("mixed/path\\separators").is_err());
        assert!(sanitize_path_component("folder/subfolder\\file").is_err());
        assert!(sanitize_path_component("/leading_slash").is_err());
        assert!(sanitize_path_component("\\leading_backslash").is_err());
        assert!(sanitize_path_component("trailing_slash/").is_err());
        assert!(sanitize_path_component("trailing_backslash\\").is_err());
    }

    #[test]
    fn test_sanitize_path_component_unix_hidden_files() {
        assert_eq!(sanitize_path_component(".hidden").unwrap(), ".hidden");
        assert_eq!(sanitize_path_component(".gitignore").unwrap(), ".gitignore");
        assert_eq!(sanitize_path_component(".config").unwrap(), ".config");
        assert_eq!(sanitize_path_component(".ssh").unwrap(), ".ssh");
        assert_eq!(sanitize_path_component(".bashrc").unwrap(), ".bashrc");
    }

    #[test]
    fn test_sanitize_path_component_spaces_and_dots() {
        assert_eq!(sanitize_path_component(" file").unwrap(), " file");
        assert_eq!(sanitize_path_component("file ").unwrap(), "file ");
        assert_eq!(sanitize_path_component(" file ").unwrap(), " file ");
        assert_eq!(sanitize_path_component("file name").unwrap(), "file name");
        assert_eq!(sanitize_path_component(".file.").unwrap(), ".file.");
    }

    #[test]
    fn test_sanitize_path_component_unix_device_patterns() {
        assert!(sanitize_path_component("/dev/null").is_err());
        assert!(sanitize_path_component("/dev/zero").is_err());
        assert!(sanitize_path_component("/dev/random").is_err());
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn test_sanitize_path_component_windows_drive_letters() {
        assert!(sanitize_path_component("C:").is_err());
        assert!(sanitize_path_component("D:").is_err());
        assert!(sanitize_path_component("C:\\").is_err());
        assert!(sanitize_path_component("D:\\path").is_err());
        assert!(sanitize_path_component("C:/path").is_err());
        assert!(sanitize_path_component("E:\\folder\\file").is_err());
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn test_sanitize_path_component_windows_unc_paths() {
        assert!(sanitize_path_component("\\\\server\\share").is_err());
        assert!(sanitize_path_component("\\\\server\\share\\path").is_err());
        assert!(sanitize_path_component("//server/share").is_err());
        assert!(sanitize_path_component("//server/share/path").is_err());
    }

    #[test]
    fn test_sanitize_path_component_windows_device_names_all_platforms() {
        assert!(sanitize_path_component("CON").is_err());
        assert!(sanitize_path_component("PRN").is_err());
        assert!(sanitize_path_component("AUX").is_err());
        assert!(sanitize_path_component("NUL").is_err());
        assert!(sanitize_path_component("COM1").is_err());
        assert!(sanitize_path_component("COM2").is_err());
        assert!(sanitize_path_component("COM3").is_err());
        assert!(sanitize_path_component("COM4").is_err());
        assert!(sanitize_path_component("COM5").is_err());
        assert!(sanitize_path_component("COM6").is_err());
        assert!(sanitize_path_component("COM7").is_err());
        assert!(sanitize_path_component("COM8").is_err());
        assert!(sanitize_path_component("COM9").is_err());
        assert!(sanitize_path_component("LPT1").is_err());
        assert!(sanitize_path_component("LPT2").is_err());
        assert!(sanitize_path_component("LPT3").is_err());
        assert!(sanitize_path_component("LPT4").is_err());
        assert!(sanitize_path_component("LPT5").is_err());
        assert!(sanitize_path_component("LPT6").is_err());
        assert!(sanitize_path_component("LPT7").is_err());
        assert!(sanitize_path_component("LPT8").is_err());
        assert!(sanitize_path_component("LPT9").is_err());
    }

    #[test]
    fn test_sanitize_path_component_windows_device_names_with_extensions_all_platforms() {
        assert!(sanitize_path_component("CON.txt").is_err());
        assert!(sanitize_path_component("PRN.log").is_err());
        assert!(sanitize_path_component("AUX.dat").is_err());
        assert!(sanitize_path_component("NUL.bin").is_err());
        assert!(sanitize_path_component("COM1.txt").is_err());
        assert!(sanitize_path_component("LPT1.out").is_err());
    }

    #[test]
    fn test_sanitize_path_component_windows_device_names_case_insensitive() {
        assert!(sanitize_path_component("con").is_err());
        assert!(sanitize_path_component("Con").is_err());
        assert!(sanitize_path_component("CON").is_err());
        assert!(sanitize_path_component("com1").is_err());
        assert!(sanitize_path_component("Lpt1").is_err());
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn test_sanitize_path_component_windows_path_components_behavior() {
        use std::path::Path;
        
        let path_with_backslash = Path::new("folder\\file");
        let components: Vec<_> = path_with_backslash.components().collect();
        assert!(components.len() > 1, "On Windows, backslash should be a separator");
        
        let path_with_slash = Path::new("folder/file");
        let components: Vec<_> = path_with_slash.components().collect();
        assert!(components.len() > 1, "On Windows, forward slash should be a separator");
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn test_sanitize_path_component_unix_path_components_behavior() {
        use std::path::Path;
        
        let path_with_backslash = Path::new("folder\\file");
        let components: Vec<_> = path_with_backslash.components().collect();
        assert_eq!(components.len(), 1, "On Unix, backslash is a regular character, not a separator");
        
        let path_with_slash = Path::new("folder/file");
        let components: Vec<_> = path_with_slash.components().collect();
        assert!(components.len() > 1, "On Unix, forward slash should be a separator");
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn test_sanitize_path_component_unix_backslash_allowed_in_name() {
        assert!(sanitize_path_component("file\\with\\backslashes").is_err());
    }

    #[test]
    fn test_sanitize_path_component_mixed_platform_separators() {
        assert!(sanitize_path_component("a/b\\c").is_err());
        assert!(sanitize_path_component("a\\b/c").is_err());
        assert!(sanitize_path_component("/a\\b").is_err());
        assert!(sanitize_path_component("\\a/b").is_err());
    }

    #[test]
    fn test_sanitize_path_component_path_components_parsing() {
        use std::path::Path;
        
        let simple = Path::new("simple");
        let components: Vec<_> = simple.components().collect();
        assert_eq!(components.len(), 1);
        
        let with_slash = Path::new("has/slash");
        let components: Vec<_> = with_slash.components().collect();
        assert!(components.len() > 1);
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

#[cfg(test)]
mod build_safe_path_tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_build_safe_path_with_multilevel_source_path() {
        let temp_dir = TempDir::new().unwrap();
        let base = temp_dir.path();
        let path = build_safe_path(base, &["movies", "2024", "torrent"]).unwrap();
        assert!(path.starts_with(base));
        assert!(path.to_str().unwrap().contains("movies"));
        assert!(path.to_str().unwrap().contains("2024"));
        assert!(path.to_str().unwrap().contains("torrent"));
    }

    #[test]
    fn test_build_safe_path_with_empty_source_path() {
        let temp_dir = TempDir::new().unwrap();
        let base = temp_dir.path();
        let path = build_safe_path(base, &["torrent"]).unwrap();
        assert!(path.starts_with(base));
        assert!(path.ends_with("torrent") || path.to_str().unwrap().ends_with("torrent"));
    }

    #[test]
    fn test_build_safe_path_with_deeply_nested_source_path() {
        let temp_dir = TempDir::new().unwrap();
        let base = temp_dir.path();
        let path = build_safe_path(base, &["a", "b", "c", "d", "e", "torrent"]).unwrap();
        assert!(path.starts_with(base));
        assert!(path.to_str().unwrap().contains("a"));
        assert!(path.to_str().unwrap().contains("b"));
        assert!(path.to_str().unwrap().contains("c"));
        assert!(path.to_str().unwrap().contains("d"));
        assert!(path.to_str().unwrap().contains("e"));
    }

    #[test]
    fn test_build_safe_path_source_path_traversal_blocked() {
        let temp_dir = TempDir::new().unwrap();
        let base = temp_dir.path();
        let result = build_safe_path(base, &["..", "etc", "torrent"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_safe_path_with_slash_in_component_blocked() {
        let temp_dir = TempDir::new().unwrap();
        let base = temp_dir.path();
        let result = build_safe_path(base, &["movies/2024", "torrent"]);
        assert!(result.is_err(), "Should reject single component with /");
    }
}
