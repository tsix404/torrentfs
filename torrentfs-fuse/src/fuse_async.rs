//! Async I/O Channel System for FUSE Operations
//!
//! This module implements a multi-producer single-consumer (MPSC) channel system
//! for handling FUSE filesystem requests asynchronously with proper backpressure
//! and timeout mechanisms.
//!
//! ## Architecture
//!
//! The system uses two types of channels:
//! - **MPSC Channel**: For forwarding FUSE requests from multiple synchronous FUSE threads
//!   to a single async Tokio runtime (capacity: 256 requests)
//! - **Oneshot Channel**: For returning async responses back to the synchronous FUSE thread
//!
//! ## Workflow
//!
//! 1. FUSE operation (synchronous) calls `send_command_with_timeout()`
//! 2. Creates oneshot channel for response
//! 3. Sends command via MPSC channel to async runtime
//! 4. Async runtime processes command in `run_command_loop()`
//! 5. Response sent back via oneshot channel
//! 6. Original caller receives response with timeout protection
//!
//! ## Error Handling
//!
//! - **ChannelClosed**: MPSC channel dropped (runtime shutdown)
//! - **Timeout**: Operation exceeds 30 seconds
//! - **DatabaseError/SessionError**: Wrapped from underlying operations

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use torrentfs::metadata::MetadataManager;
use torrentfs::repo::TorrentRepo;
use torrentfs::DownloadCoordinator;
use torrentfs_libtorrent::Session;

/// Timeout for I/O operations (30 seconds)
const IO_TIMEOUT: Duration = Duration::from_secs(30);

/// MPSC channel buffer size to prevent overflow under load
const CHANNEL_BUFFER_SIZE: usize = 256;

/// Commands sent from synchronous FUSE threads to async runtime
/// 
/// Each command includes a oneshot channel for returning the response.
/// This enables request-response pattern across the sync/async boundary.
#[derive(Debug)]
pub enum FuseCommand {
    ListTorrents {
        reply: oneshot::Sender<Result<Vec<TorrentInfo>, FuseError>>,
    },
    GetTorrentFiles {
        torrent_name: String,
        reply: oneshot::Sender<Result<Vec<FileInfo>, FuseError>>,
    },
    ProcessTorrentData {
        data: Vec<u8>,
        source_path: String,
        reply: oneshot::Sender<Result<ParsedTorrentInfo, FuseError>>,
    },
    AddTorrentPaused {
        data: Vec<u8>,
        save_path: String,
        reply: oneshot::Sender<Result<(), FuseError>>,
    },
    PersistToDb {
        parsed: ParsedTorrentInfo,
        reply: oneshot::Sender<Result<PersistResult, FuseError>>,
    },
    GetFileInfoForInode {
        torrent_name: String,
        file_path: String,
        reply: oneshot::Sender<Result<FileInfoForRead, FuseError>>,
    },
    ReadFilePiece {
        info_hash: String,
        piece_index: u32,
        reply: oneshot::Sender<Result<Vec<u8>, FuseError>>,
    },
}

#[derive(Debug, Clone)]
pub enum PersistResult {
    Inserted,
    AlreadyExists,
}

#[derive(Debug, Clone)]
pub struct TorrentInfo {
    pub name: String,
    pub source_path: String,
}

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: String,
    pub size: i64,
    pub first_piece: i64,
    pub last_piece: i64,
    pub offset: i64,
}

#[derive(Debug, Clone)]
pub struct FileInfoForRead {
    pub torrent_name: String,
    pub info_hash: String,
    pub file_path: String,
    pub file_size: i64,
    pub piece_size: u32,
    pub first_piece: i64,
    pub last_piece: i64,
    pub file_offset: u64,
}

#[derive(Debug, Clone)]
pub struct ParsedTorrentInfo {
    pub torrent_name: String,
    pub info_hash: Vec<u8>,
    pub total_size: i64,
    pub file_count: i64,
    pub files: Vec<FileInfo>,
    pub source_path: String,
    pub torrent_data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub enum FuseError {
    DatabaseError(String),
    TorrentParseError(String),
    SessionError(String),
    Timeout(String),
    ChannelClosed,
}

impl std::fmt::Display for FuseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FuseError::DatabaseError(msg) => write!(f, "Database error: {}", msg),
            FuseError::TorrentParseError(msg) => write!(f, "Torrent parse error: {}", msg),
            FuseError::SessionError(msg) => write!(f, "Session error: {}", msg),
            FuseError::Timeout(msg) => write!(f, "Operation timed out: {}", msg),
            FuseError::ChannelClosed => write!(f, "Async channel closed"),
        }
    }
}

impl std::error::Error for FuseError {}

/// Async runtime for FUSE operations
/// 
/// Bridges synchronous FUSE callbacks with async Tokio operations using
/// MPSC channel for request forwarding and oneshot channels for responses.
pub struct FuseAsyncRuntime {
    command_tx: mpsc::Sender<FuseCommand>,
    rt: tokio::runtime::Runtime,
    _task_handle: tokio::task::JoinHandle<()>,
    #[allow(dead_code)]
    download_coordinator: Option<Arc<DownloadCoordinator>>,
}

impl FuseAsyncRuntime {
    /// Creates a new async runtime with MPSC channel system
    /// 
    /// Spawns a background task that processes commands from the MPSC channel.
    /// The channel has a capacity of 256 to handle bursts of FUSE operations.
    pub fn new(
        metadata_manager: Arc<MetadataManager>,
        session: Arc<Session>,
        state_dir: &Path,
    ) -> Self {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
        let (command_tx, command_rx) = mpsc::channel(CHANNEL_BUFFER_SIZE);
        
        let piece_cache = Arc::new(
            torrentfs::PieceCache::with_state_dir(state_dir).expect("Failed to create PieceCache")
        );
        let download_coordinator = Arc::new(DownloadCoordinator::new(
            Arc::clone(&session),
            Arc::clone(&piece_cache),
        ));
        
        let dc_clone = Arc::clone(&download_coordinator);
        let task_handle = rt.spawn(async move {
            Self::run_command_loop(command_rx, metadata_manager, session, dc_clone).await;
        });
        
        Self {
            command_tx,
            rt,
            _task_handle: task_handle,
            download_coordinator: Some(download_coordinator),
        }
    }
    
    pub fn new_with_download_coordinator(
        metadata_manager: Arc<MetadataManager>,
        session: Arc<Session>,
        download_coordinator: Arc<DownloadCoordinator>,
    ) -> Self {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
        let (command_tx, command_rx) = mpsc::channel(CHANNEL_BUFFER_SIZE);
        
        let dc_clone = Arc::clone(&download_coordinator);
        let task_handle = rt.spawn(async move {
            Self::run_command_loop(command_rx, metadata_manager, session, dc_clone).await;
        });
        
        Self {
            command_tx,
            rt,
            _task_handle: task_handle,
            download_coordinator: Some(download_coordinator),
        }
    }
    
    /// Sends a command through the MPSC channel and waits for response
    /// 
    /// Creates a oneshot channel for the response, sends the command via MPSC,
    /// and waits for up to IO_TIMEOUT (30 seconds) for the response.
    /// 
    /// # Errors
    /// - `ChannelClosed`: MPSC sender dropped (runtime shutdown)
    /// - `Timeout`: Response not received within timeout period
    pub fn send_command_with_timeout<R, F>(&self, f: F) -> Result<R, FuseError>
    where
        F: FnOnce(oneshot::Sender<Result<R, FuseError>>) -> FuseCommand,
    {
        let (reply_tx, reply_rx) = oneshot::channel();
        let command = f(reply_tx);
        
        self.command_tx
            .blocking_send(command)
            .map_err(|_| FuseError::ChannelClosed)?;
        
        self.rt.block_on(async {
            match timeout(IO_TIMEOUT, reply_rx).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err(FuseError::ChannelClosed),
                Err(_) => Err(FuseError::Timeout("Async operation timed out".to_string())),
            }
        })
    }
    
    /// Main command processing loop running in async context
    /// 
    /// Receives commands from the MPSC channel and dispatches them to appropriate
    /// handlers. Each handler sends its response via the oneshot channel included
    /// in the command.
    async fn run_command_loop(
        mut command_rx: mpsc::Receiver<FuseCommand>,
        metadata_manager: Arc<MetadataManager>,
        session: Arc<Session>,
        download_coordinator: Arc<DownloadCoordinator>,
    ) {
        while let Some(command) = command_rx.recv().await {
            match command {
                FuseCommand::ListTorrents { reply } => {
                    let result = Self::handle_list_torrents(&metadata_manager).await;
                    let _ = reply.send(result);
                }
                FuseCommand::GetTorrentFiles { torrent_name, reply } => {
                    let result = Self::handle_get_torrent_files(&metadata_manager, &torrent_name).await;
                    let _ = reply.send(result);
                }
                FuseCommand::ProcessTorrentData { data, source_path, reply } => {
                    let result = Self::handle_process_torrent_data(&metadata_manager, &data, &source_path).await;
                    let _ = reply.send(result);
                }
                FuseCommand::AddTorrentPaused { data, save_path, reply } => {
                    let session = session.clone();
                    let data = data.clone();
                    let save_path = save_path.clone();
                    tokio::task::spawn_blocking(move || {
                        let result = Self::handle_add_torrent_paused(&session, &data, &save_path);
                        let _ = reply.send(result);
                    });
                }
                FuseCommand::PersistToDb { parsed, reply } => {
                    let result = Self::handle_persist_to_db(&metadata_manager.repo, &parsed).await;
                    let _ = reply.send(result);
                }
                FuseCommand::GetFileInfoForInode { torrent_name, file_path, reply } => {
                    let result = Self::handle_get_file_info_for_inode(&metadata_manager, &torrent_name, &file_path).await;
                    let _ = reply.send(result);
                }
                FuseCommand::ReadFilePiece { info_hash, piece_index, reply } => {
                    let dc = download_coordinator.clone();
                    tokio::task::spawn_blocking(move || {
                        let result = Self::handle_read_file_piece(&dc, &info_hash, piece_index);
                        let _ = reply.send(result);
                    });
                }
            }
        }
        
        tracing::info!("FUSE async command loop stopped");
    }
    
    async fn handle_list_torrents(
        metadata_manager: &MetadataManager,
    ) -> Result<Vec<TorrentInfo>, FuseError> {
        let torrents = metadata_manager
            .list_torrents()
            .await
            .map_err(|e| FuseError::DatabaseError(e.to_string()))?;
        
        Ok(torrents
            .into_iter()
            .map(|t| TorrentInfo {
                name: t.name,
                source_path: t.source_path,
            })
            .collect())
    }
    
    async fn handle_get_torrent_files(
        metadata_manager: &MetadataManager,
        torrent_name: &str,
    ) -> Result<Vec<FileInfo>, FuseError> {
        let files = metadata_manager
            .get_torrent_files(torrent_name)
            .await
            .map_err(|e| FuseError::DatabaseError(e.to_string()))?;
        
        Ok(files
            .into_iter()
            .map(|f| FileInfo {
                path: f.path,
                size: f.size,
                first_piece: f.first_piece,
                last_piece: f.last_piece,
                offset: f.offset,
            })
            .collect())
    }
    
    async fn handle_process_torrent_data(
        metadata_manager: &MetadataManager,
        data: &[u8],
        source_path: &str,
    ) -> Result<ParsedTorrentInfo, FuseError> {
        let parsed = metadata_manager
            .process_torrent_data(data, source_path)
            .await
            .map_err(|e| FuseError::TorrentParseError(e.to_string()))?;
        
        Ok(ParsedTorrentInfo {
            torrent_name: parsed.torrent_name,
            info_hash: parsed.info_hash,
            total_size: parsed.total_size,
            file_count: parsed.file_count,
            files: parsed.files.into_iter().map(|f| FileInfo {
                path: f.path,
                size: f.size,
                first_piece: f.first_piece,
                last_piece: f.last_piece,
                offset: f.offset,
            }).collect(),
            source_path: parsed.source_path,
            torrent_data: parsed.torrent_data,
        })
    }
    
    fn handle_add_torrent_paused(
        session: &Session,
        data: &[u8],
        save_path: &str,
    ) -> Result<(), FuseError> {
        session
            .add_torrent_paused(data, save_path)
            .map_err(|e| FuseError::SessionError(e.to_string()))
    }
    
    async fn handle_persist_to_db(
        repo: &TorrentRepo,
        parsed: &ParsedTorrentInfo,
    ) -> Result<PersistResult, FuseError> {
        let repo_files: Vec<torrentfs::repo::FileEntry> = parsed.files.iter().map(|f| {
            torrentfs::repo::FileEntry {
                id: 0,
                torrent_id: 0,
                path: f.path.clone(),
                size: f.size,
                first_piece: f.first_piece,
                last_piece: f.last_piece,
                offset: f.offset,
            }
        }).collect();
        
        let result = repo
            .insert_if_not_exists(
                &parsed.info_hash,
                &parsed.torrent_name,
                parsed.total_size,
                parsed.file_count,
                &parsed.source_path,
                Some(&parsed.torrent_data),
                repo_files,
            )
            .await
            .map_err(|e| FuseError::DatabaseError(e.to_string()))?;
        
        match result {
            torrentfs::repo::InsertResult::Inserted(_) => Ok(PersistResult::Inserted),
            torrentfs::repo::InsertResult::AlreadyExists(_) => Ok(PersistResult::AlreadyExists),
        }
    }
    
    async fn handle_get_file_info_for_inode(
        metadata_manager: &MetadataManager,
        torrent_name: &str,
        file_path: &str,
    ) -> Result<FileInfoForRead, FuseError> {
        let torrent = metadata_manager
            .repo
            .find_by_name(torrent_name)
            .await
            .map_err(|e| FuseError::DatabaseError(e.to_string()))?
            .ok_or_else(|| FuseError::DatabaseError(format!("Torrent '{}' not found", torrent_name)))?;
        
        let info_hash = hex::encode(&torrent.info_hash);
        
        let files = metadata_manager
            .repo
            .get_files(torrent.id)
            .await
            .map_err(|e| FuseError::DatabaseError(e.to_string()))?;
        
        let file = files
            .iter()
            .find(|f| f.path == file_path)
            .ok_or_else(|| FuseError::DatabaseError(format!("File '{}' not found in torrent", file_path)))?;
        
        let torrent_data = torrent.torrent_data
            .ok_or_else(|| FuseError::DatabaseError("Torrent data not stored".to_string()))?;
        
        let torrent_info = torrentfs_libtorrent::parse_torrent(&torrent_data)
            .map_err(|e| FuseError::TorrentParseError(e.to_string()))?;
        
        Ok(FileInfoForRead {
            torrent_name: torrent_name.to_string(),
            info_hash,
            file_path: file_path.to_string(),
            file_size: file.size,
            piece_size: torrent_info.piece_size,
            first_piece: file.first_piece,
            last_piece: file.last_piece,
            file_offset: file.offset as u64,
        })
    }
    
    fn handle_read_file_piece(
        download_coordinator: &DownloadCoordinator,
        info_hash: &str,
        piece_index: u32,
    ) -> Result<Vec<u8>, FuseError> {
        download_coordinator
            .get_piece(info_hash, piece_index)
            .map_err(|e| FuseError::SessionError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use torrentfs::database::Database;
    use tempfile::TempDir;
    use sqlx::sqlite::SqliteConnectOptions;
    use sqlx::SqlitePool;
    use std::str::FromStr;

    async fn setup_test_db() -> (TempDir, Database) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let options = SqliteConnectOptions::from_str(&db_path.to_string_lossy())
            .unwrap()
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(options).await.unwrap();
        let db = Database::with_pool(pool);
        db.migrate().await.unwrap();
        (temp_dir, db)
    }

    #[test]
    fn test_fuse_async_runtime_creation() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let temp_state_dir = TempDir::new().unwrap();
        let (_temp_dir, db) = rt.block_on(setup_test_db());
        let metadata_manager = Arc::new(MetadataManager::new(Arc::new(db)).unwrap());
        let session = Arc::new(Session::new().unwrap());
        
        let _runtime = FuseAsyncRuntime::new(metadata_manager, session, temp_state_dir.path());
    }

    #[test]
    fn test_list_torrents_empty() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let temp_state_dir = TempDir::new().unwrap();
        let (_temp_dir, db) = rt.block_on(setup_test_db());
        let metadata_manager = Arc::new(MetadataManager::new(Arc::new(db)).unwrap());
        let session = Arc::new(Session::new().unwrap());
        
        let runtime = FuseAsyncRuntime::new(metadata_manager, session, temp_state_dir.path());
        
        let result = runtime.send_command_with_timeout(|reply| {
            FuseCommand::ListTorrents { reply }
        });
        
        assert!(result.is_ok());
        let torrents = result.unwrap();
        assert!(torrents.is_empty());
    }

    #[test]
    fn test_channel_capacity() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let temp_state_dir = TempDir::new().unwrap();
        let (_temp_dir, db) = rt.block_on(setup_test_db());
        let metadata_manager = Arc::new(MetadataManager::new(Arc::new(db)).unwrap());
        let session = Arc::new(Session::new().unwrap());
        
        let runtime = FuseAsyncRuntime::new(metadata_manager, session, temp_state_dir.path());
        
        let mut handles = vec![];
        for i in 0..10 {
            let runtime = runtime.command_tx.clone();
            handles.push(std::thread::spawn(move || {
                let (reply_tx, reply_rx) = oneshot::channel();
                let command = FuseCommand::ListTorrents { reply: reply_tx };
                runtime.blocking_send(command).unwrap();
                let result = rt.block_on(timeout(IO_TIMEOUT, reply_rx)).unwrap().unwrap();
                assert!(result.is_ok());
                result.unwrap()
            }));
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_mpsc_oneshot_integration() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let temp_state_dir = TempDir::new().unwrap();
        let (_temp_dir, db) = rt.block_on(setup_test_db());
        let metadata_manager = Arc::new(MetadataManager::new(Arc::new(db)).unwrap());
        let session = Arc::new(Session::new().unwrap());
        
        let runtime = FuseAsyncRuntime::new(metadata_manager, session, temp_state_dir.path());
        
        let result1 = runtime.send_command_with_timeout(|reply| {
            FuseCommand::ListTorrents { reply }
        });
        
        let result2 = runtime.send_command_with_timeout(|reply| {
            FuseCommand::GetTorrentFiles {
                torrent_name: "nonexistent".to_string(),
                reply,
            }
        });
        
        assert!(result1.is_ok());
        assert!(result2.is_err());
    }

    #[test]
    fn test_timeout_mechanism() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let temp_state_dir = TempDir::new().unwrap();
        let (_temp_dir, db) = rt.block_on(setup_test_db());
        let metadata_manager = Arc::new(MetadataManager::new(Arc::new(db)).unwrap());
        let session = Arc::new(Session::new().unwrap());
        
        let runtime = FuseAsyncRuntime::new(metadata_manager, session, temp_state_dir.path());
        
        let start = std::time::Instant::now();
        let result = runtime.send_command_with_timeout(|reply| {
            FuseCommand::ListTorrents { reply }
        });
        let elapsed = start.elapsed();
        
        assert!(result.is_ok());
        assert!(elapsed < IO_TIMEOUT);
    }
}
