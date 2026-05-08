use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use torrentfs::metadata::MetadataManager;
use torrentfs::repo::TorrentRepo;
use torrentfs::DownloadCoordinator;
use torrentfs_libtorrent::Session;

const IO_TIMEOUT: Duration = Duration::from_secs(30);
const CHANNEL_BUFFER_SIZE: usize = 256;
const MAX_COMMAND_RETRIES: u32 = 3;
const RETRY_DELAY_MS: u64 = 500;

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

impl FuseError {
    pub fn is_retryable(&self) -> bool {
        matches!(self, FuseError::Timeout(_) | FuseError::SessionError(_))
    }
}

pub struct FuseAsyncRuntime {
    command_tx: mpsc::Sender<FuseCommand>,
    rt: tokio::runtime::Runtime,
    _task_handle: tokio::task::JoinHandle<()>,
    #[allow(dead_code)]
    download_coordinator: Option<Arc<DownloadCoordinator>>,
}

impl FuseAsyncRuntime {
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
    
    pub fn send_command_with_timeout<R, F>(&self, mut f: F) -> Result<R, FuseError>
    where
        F: FnMut(oneshot::Sender<Result<R, FuseError>>) -> FuseCommand,
    {
        let mut last_error = FuseError::Timeout("No attempts made".to_string());
        
        for attempt in 0..MAX_COMMAND_RETRIES {
            let (reply_tx, reply_rx) = oneshot::channel();
            let command = f(reply_tx);
            
            match self.command_tx.blocking_send(command) {
                Ok(()) => {}
                Err(_) => return Err(FuseError::ChannelClosed),
            }
            
            let result = self.rt.block_on(async {
                match timeout(IO_TIMEOUT, reply_rx).await {
                    Ok(Ok(result)) => result,
                    Ok(Err(_)) => Err(FuseError::ChannelClosed),
                    Err(_) => Err(FuseError::Timeout("Async operation timed out".to_string())),
                }
            });
            
            match result {
                Ok(value) => return Ok(value),
                Err(e) => {
                    if !e.is_retryable() {
                        return Err(e);
                    }
                    
                    last_error = e;
                    
                    if attempt + 1 < MAX_COMMAND_RETRIES {
                        let delay = RETRY_DELAY_MS * (1 << attempt);
                        tracing::warn!(
                            attempt = attempt + 1,
                            delay_ms = delay,
                            error = %last_error,
                            "FUSE command failed, retrying"
                        );
                        std::thread::sleep(Duration::from_millis(delay));
                    }
                }
            }
        }
        
        Err(last_error)
    }
    
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
    fn test_fuse_error_is_retryable() {
        assert!(FuseError::Timeout("test".to_string()).is_retryable());
        assert!(FuseError::SessionError("test".to_string()).is_retryable());
        assert!(!FuseError::DatabaseError("test".to_string()).is_retryable());
        assert!(!FuseError::TorrentParseError("test".to_string()).is_retryable());
        assert!(!FuseError::ChannelClosed.is_retryable());
    }
}
