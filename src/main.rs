use clap::Parser;
use fuser::{
    FileAttr, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyEntry, ReplyOpen,
    ReplyDirectory, Request, ReplyWrite, ReplyCreate,
};
use libc::{EACCES, EEXIST, EFBIG, EINVAL, EIO, EISDIR, ENOENT, ENOTDIR};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

mod db;
mod error;
mod torrent_info;
mod download;
mod cache;

use db::{Database, FileEntry, InsertTorrentResult};
use torrent_info::TorrentInfo;
use download::DownloadManager;
use cache::CacheManager;

const ROOT_INO: u64 = 1;
const METADATA_INO: u64 = 2;
const DATA_INO: u64 = 3;
const MAX_TORRENT_SIZE: usize = 10 * 1024 * 1024;
const DATA_TORRENT_INO_BASE: u64 = 1_000_000;
const DATA_DIR_INO_BASE: u64 = 2_000_000;
const DATA_FILE_INO_BASE: u64 = 3_000_000;

static NEXT_INO: AtomicU64 = AtomicU64::new(4);
static NEXT_FH: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
enum InodeData {
    Directory { parent: u64, name: String },
    File { parent: u64, name: String, data: Vec<u8> },
}

#[derive(Clone, Debug)]
enum DataInode {
    SourcePathDir { path: String },
    TorrentRoot { torrent_id: i64, source_path: String, name: String },
    TorrentDir { torrent_id: i64, dir_id: i64, name: String },
    TorrentFile { torrent_id: i64, file_id: i64, name: String, size: i64 },
}

#[derive(Parser, Debug)]
#[command(name = "torrentfs")]
#[command(about = "A FUSE filesystem for torrent management")]
struct Args {
    #[arg(help = "Mount point path")]
    mountpoint: PathBuf,
    #[arg(long, help = "Database path")]
    db: Option<PathBuf>,
    #[arg(long, help = "Cache directory for downloaded pieces")]
    cache: Option<PathBuf>,
}

struct TorrentFs {
    creation_time: Duration,
    inodes: HashMap<u64, InodeData>,
    data_inodes: HashMap<u64, DataInode>,
    open_files: HashMap<u64, u64>,
    db: Option<Arc<Mutex<Database>>>,
    processing_torrents: Arc<Mutex<HashMap<String, ()>>>,
    download_manager: Option<Arc<Mutex<DownloadManager>>>,
    torrent_data_cache: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    cache_manager: Option<Arc<Mutex<CacheManager>>>,
}

impl TorrentFs {
    fn new() -> Self {
        Self::new_with_cache_path(PathBuf::from("/tmp/torrentfs-cache"))
    }
    
    fn new_with_cache_path(cache_path: PathBuf) -> Self {
        let mut inodes = HashMap::new();
        inodes.insert(ROOT_INO, InodeData::Directory { parent: 0, name: String::new() });
        inodes.insert(METADATA_INO, InodeData::Directory { parent: ROOT_INO, name: "metadata".to_string() });
        inodes.insert(DATA_INO, InodeData::Directory { parent: ROOT_INO, name: "data".to_string() });
        
        if !cache_path.exists() {
            if let Err(e) = std::fs::create_dir_all(&cache_path) {
                warn!("Failed to create cache directory {:?}: {:?}", cache_path, e);
            }
        }
        
        let download_manager = DownloadManager::new(cache_path.as_path()).ok();
        
        let cache_manager = CacheManager::new(&cache_path, 1024 * 1024 * 1024).ok();
        
        Self {
            creation_time: Duration::from_secs(
                std::time::SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            ),
            inodes,
            data_inodes: HashMap::new(),
            open_files: HashMap::new(),
            db: None,
            processing_torrents: Arc::new(Mutex::new(HashMap::new())),
            download_manager: download_manager.map(|dm| Arc::new(Mutex::new(dm))),
            torrent_data_cache: Arc::new(Mutex::new(HashMap::new())),
            cache_manager: cache_manager.map(|cm| Arc::new(Mutex::new(cm))),
        }
    }

    fn new_with_db(db: Database) -> Self {
        Self::new()
    }
    
    fn new_with_db_and_cache(db: Database, cache_path: PathBuf) -> Self {
        let mut fs = Self::new_with_cache_path(cache_path);
        fs.db = Some(Arc::new(Mutex::new(db)));
        fs
    }

    fn make_torrent_root_ino(torrent_id: i64) -> u64 {
        DATA_TORRENT_INO_BASE + (torrent_id as u64)
    }

    fn make_torrent_dir_ino(dir_id: i64) -> u64 {
        DATA_DIR_INO_BASE + (dir_id as u64)
    }

    fn make_torrent_file_ino(file_id: i64) -> u64 {
        DATA_FILE_INO_BASE + (file_id as u64)
    }

    fn is_data_ino(ino: u64) -> bool {
        ino >= DATA_TORRENT_INO_BASE
    }

    fn get_db(&self) -> Result<&Arc<Mutex<Database>>, i32> {
        self.db.as_ref().ok_or_else(|| {
            error!("Database not available");
            EIO
        })
    }

    fn resolve_data_lookup(&self, parent: u64, name: &str) -> Option<(u64, DataInode)> {
        if parent == DATA_INO {
            return self.resolve_data_root_lookup(name);
        }

        let data_inode = self.data_inodes.get(&parent)?;
        match data_inode {
            DataInode::SourcePathDir { path } => {
                self.resolve_source_path_dir_lookup(path, name)
            }
            DataInode::TorrentRoot { torrent_id, .. } => {
                self.resolve_torrent_root_lookup(*torrent_id, name)
            }
            DataInode::TorrentDir { torrent_id, dir_id, .. } => {
                self.resolve_torrent_dir_lookup(*torrent_id, Some(*dir_id), name)
            }
            DataInode::TorrentFile { .. } => None,
        }
    }

    fn resolve_data_root_lookup(&self, name: &str) -> Option<(u64, DataInode)> {
        let db = self.get_db().ok()?;
        let db_guard = db.lock().ok()?;

        let torrents = db_guard.get_torrents_by_source_path(name).ok()?;
        
        if !torrents.is_empty() {
            let torrent = torrents.first()?;
            let ino = Self::make_torrent_root_ino(torrent.id);
            return Some((ino, DataInode::TorrentRoot {
                torrent_id: torrent.id,
                source_path: torrent.source_path.clone(),
                name: torrent.name.clone(),
            }));
        }

        let root_torrents = db_guard.get_torrents_by_source_path("").ok()?;
        for torrent in root_torrents {
            if torrent.name == name {
                let ino = Self::make_torrent_root_ino(torrent.id);
                return Some((ino, DataInode::TorrentRoot {
                    torrent_id: torrent.id,
                    source_path: torrent.source_path.clone(),
                    name: torrent.name.clone(),
                }));
            }
        }

        let prefixes = db_guard.get_source_path_prefixes("").ok()?;
        if prefixes.contains(&name.to_string()) {
            let full_path = name.to_string();
            let ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
            return Some((ino, DataInode::SourcePathDir { path: full_path }));
        }

        None
    }

    fn resolve_source_path_dir_lookup(&self, prefix: &str, name: &str) -> Option<(u64, DataInode)> {
        let db = self.get_db().ok()?;
        let db_guard = db.lock().ok()?;

        let new_path = if prefix.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", prefix, name)
        };

        let torrents = db_guard.get_torrents_by_source_path(&new_path).ok()?;
        if !torrents.is_empty() {
            let torrent = torrents.first()?;
            let ino = Self::make_torrent_root_ino(torrent.id);
            return Some((ino, DataInode::TorrentRoot {
                torrent_id: torrent.id,
                source_path: torrent.source_path.clone(),
                name: torrent.name.clone(),
            }));
        }

        let prefixes = db_guard.get_source_path_prefixes(&new_path).ok()?;
        if prefixes.contains(&name.to_string()) {
            let ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
            return Some((ino, DataInode::SourcePathDir { path: new_path }));
        }

        None
    }

    fn resolve_torrent_root_lookup(&self, torrent_id: i64, name: &str) -> Option<(u64, DataInode)> {
        self.resolve_torrent_dir_lookup(torrent_id, None, name)
    }

    fn resolve_torrent_dir_lookup(&self, torrent_id: i64, parent_dir_id: Option<i64>, name: &str) -> Option<(u64, DataInode)> {
        let db = self.get_db().ok()?;
        let db_guard = db.lock().ok()?;

        if let Some(dir) = db_guard.get_torrent_directory(torrent_id, parent_dir_id, name).ok()? {
            let ino = Self::make_torrent_dir_ino(dir.id);
            return Some((ino, DataInode::TorrentDir {
                torrent_id,
                dir_id: dir.id,
                name: dir.name,
            }));
        }

        let files = if let Some(pid) = parent_dir_id {
            db_guard.get_files_in_directory(pid).ok()?
        } else {
            db_guard.get_root_files(torrent_id).ok()?
        };

        for file in files {
            if file.name == name {
                let ino = Self::make_torrent_file_ino(file.id);
                return Some((ino, DataInode::TorrentFile {
                    torrent_id,
                    file_id: file.id,
                    name: file.name,
                    size: file.size,
                }));
            }
        }

        None
    }

    fn lookup_data_inode(&mut self, parent: u64, name: &str) -> Option<(u64, fuser::FileType, u64)> {
        let (ino, data_inode) = self.resolve_data_lookup(parent, name)?;
        
        self.data_inodes.insert(ino, data_inode.clone());
        
        match &data_inode {
            DataInode::SourcePathDir { .. } |
            DataInode::TorrentRoot { .. } |
            DataInode::TorrentDir { .. } => {
                Some((ino, fuser::FileType::Directory, 0))
            }
            DataInode::TorrentFile { size, .. } => {
                Some((ino, fuser::FileType::RegularFile, *size as u64))
            }
        }
    }

    fn readdir_data(&mut self, ino: u64, offset: i64) -> Option<Vec<(u64, i64, fuser::FileType, String)>> {
        let mut entries: Vec<(u64, i64, fuser::FileType, String)> = Vec::new();
        let mut cache_entries: Vec<(u64, DataInode)> = Vec::new();
        
        if ino == DATA_INO {
            entries.push((DATA_INO, 1, fuser::FileType::Directory, ".".to_string()));
            entries.push((ROOT_INO, 2, fuser::FileType::Directory, "..".to_string()));
            
            {
                let db = self.get_db().ok()?;
                let db_guard = db.lock().ok()?;
                
                let mut offset_counter = 3i64;
                
                let root_torrents = db_guard.get_torrents_by_source_path("").ok()?;
                for torrent in root_torrents {
                    let torrent_ino = Self::make_torrent_root_ino(torrent.id);
                    let name = torrent.name.clone();
                    cache_entries.push((torrent_ino, DataInode::TorrentRoot {
                        torrent_id: torrent.id,
                        source_path: torrent.source_path.clone(),
                        name: torrent.name.clone(),
                    }));
                    entries.push((torrent_ino, offset_counter, fuser::FileType::Directory, name));
                    offset_counter += 1;
                }
                
                let prefixes = db_guard.get_source_path_prefixes("").ok()?;
                
                for prefix in prefixes {
                    let torrents = db_guard.get_torrents_by_source_path(&prefix).ok()?;
                    if !torrents.is_empty() {
                        let torrent = torrents.first()?;
                        let torrent_ino = Self::make_torrent_root_ino(torrent.id);
                        cache_entries.push((torrent_ino, DataInode::TorrentRoot {
                            torrent_id: torrent.id,
                            source_path: torrent.source_path.clone(),
                            name: torrent.name.clone(),
                        }));
                        entries.push((torrent_ino, offset_counter, fuser::FileType::Directory, prefix));
                        offset_counter += 1;
                    } else {
                        let child_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
                        cache_entries.push((child_ino, DataInode::SourcePathDir { path: prefix.clone() }));
                        entries.push((child_ino, offset_counter, fuser::FileType::Directory, prefix));
                        offset_counter += 1;
                    }
                }
            }
            
            for (cache_ino, cache_inode) in cache_entries {
                self.data_inodes.insert(cache_ino, cache_inode);
            }
            
            return Some(entries.into_iter().filter(|(_, o, _, _)| *o > offset).collect());
        }

        let data_inode = self.data_inodes.get(&ino)?.clone();
        
        match data_inode {
            DataInode::SourcePathDir { path } => {
                entries.push((ino, 1, fuser::FileType::Directory, ".".to_string()));
                entries.push((DATA_INO, 2, fuser::FileType::Directory, "..".to_string()));
                
                {
                    let db = self.get_db().ok()?;
                    let db_guard = db.lock().ok()?;
                    
                    let mut offset_counter = 3i64;
                    
                    let sub_prefixes = db_guard.get_source_path_prefixes(&path).ok()?;
                    for sub in sub_prefixes {
                        let new_path = if path.is_empty() { sub.clone() } else { format!("{}/{}", path, sub) };
                        let torrents = db_guard.get_torrents_by_source_path(&new_path).ok()?;
                        
                        if !torrents.is_empty() {
                            let torrent = torrents.first()?;
                            let torrent_ino = Self::make_torrent_root_ino(torrent.id);
                            cache_entries.push((torrent_ino, DataInode::TorrentRoot {
                                torrent_id: torrent.id,
                                source_path: torrent.source_path.clone(),
                                name: torrent.name.clone(),
                            }));
                            entries.push((torrent_ino, offset_counter, fuser::FileType::Directory, sub));
                        } else {
                            let child_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
                            cache_entries.push((child_ino, DataInode::SourcePathDir { path: new_path.clone() }));
                            entries.push((child_ino, offset_counter, fuser::FileType::Directory, sub));
                        }
                        offset_counter += 1;
                    }
                    
                    let direct_torrents = db_guard.get_torrents_by_source_path(&path).ok()?;
                    for torrent in direct_torrents {
                        let torrent_ino = Self::make_torrent_root_ino(torrent.id);
                        let name = torrent.name.clone();
                        cache_entries.push((torrent_ino, DataInode::TorrentRoot {
                            torrent_id: torrent.id,
                            source_path: torrent.source_path.clone(),
                            name: torrent.name.clone(),
                        }));
                        entries.push((torrent_ino, offset_counter, fuser::FileType::Directory, name));
                        offset_counter += 1;
                    }
                }
                
                for (cache_ino, cache_inode) in cache_entries {
                    self.data_inodes.insert(cache_ino, cache_inode);
                }
            }
            DataInode::TorrentRoot { torrent_id, source_path, .. } => {
                entries.push((ino, 1, fuser::FileType::Directory, ".".to_string()));
                
                let parent_ino = if source_path.is_empty() {
                    DATA_INO
                } else {
                    let path_parts: Vec<&str> = source_path.split('/').collect();
                    if path_parts.len() == 1 {
                        DATA_INO
                    } else {
                        let parent_path = path_parts[..path_parts.len() - 1].join("/");
                        let db = self.get_db().ok()?;
                        let db_guard = db.lock().ok()?;
                        
                        let torrents = db_guard.get_torrents_by_source_path(&parent_path).ok().unwrap_or_default();
                        if !torrents.is_empty() {
                            Self::make_torrent_root_ino(torrents[0].id)
                        } else {
                            DATA_INO
                        }
                    }
                };
                entries.push((parent_ino, 2, fuser::FileType::Directory, "..".to_string()));
                
                {
                    let db = self.get_db().ok()?;
                    let db_guard = db.lock().ok()?;
                    
                    let mut offset_counter = 3i64;
                    
                    let root_dirs = db_guard.get_torrent_directories_by_parent(None, torrent_id).ok()?;
                    for dir in root_dirs {
                        let dir_ino = Self::make_torrent_dir_ino(dir.id);
                        cache_entries.push((dir_ino, DataInode::TorrentDir {
                            torrent_id,
                            dir_id: dir.id,
                            name: dir.name.clone(),
                        }));
                        entries.push((dir_ino, offset_counter, fuser::FileType::Directory, dir.name));
                        offset_counter += 1;
                    }
                    
                    let root_files = db_guard.get_root_files(torrent_id).ok()?;
                    for file in root_files {
                        let file_ino = Self::make_torrent_file_ino(file.id);
                        cache_entries.push((file_ino, DataInode::TorrentFile {
                            torrent_id,
                            file_id: file.id,
                            name: file.name.clone(),
                            size: file.size,
                        }));
                        entries.push((file_ino, offset_counter, fuser::FileType::RegularFile, file.name));
                        offset_counter += 1;
                    }
                }
                
                for (cache_ino, cache_inode) in cache_entries {
                    self.data_inodes.insert(cache_ino, cache_inode);
                }
            }
            DataInode::TorrentDir { torrent_id, dir_id, .. } => {
                entries.push((ino, 1, fuser::FileType::Directory, ".".to_string()));
                
                {
                    let db = self.get_db().ok()?;
                    let db_guard = db.lock().ok()?;
                    
                    let parent_ino = db_guard.get_torrent_directory_by_id(dir_id)
                        .ok()
                        .flatten()
                        .and_then(|d| d.parent_id)
                        .map(|pid| Self::make_torrent_dir_ino(pid))
                        .unwrap_or_else(|| Self::make_torrent_root_ino(torrent_id));
                    entries.push((parent_ino, 2, fuser::FileType::Directory, "..".to_string()));
                    
                    let mut offset_counter = 3i64;
                    
                    let sub_dirs = db_guard.get_torrent_directories_by_parent(Some(dir_id), torrent_id).ok()?;
                    for dir in sub_dirs {
                        let sub_dir_ino = Self::make_torrent_dir_ino(dir.id);
                        cache_entries.push((sub_dir_ino, DataInode::TorrentDir {
                            torrent_id,
                            dir_id: dir.id,
                            name: dir.name.clone(),
                        }));
                        entries.push((sub_dir_ino, offset_counter, fuser::FileType::Directory, dir.name));
                        offset_counter += 1;
                    }
                    
                    let dir_files = db_guard.get_files_in_directory(dir_id).ok()?;
                    for file in dir_files {
                        let file_ino = Self::make_torrent_file_ino(file.id);
                        cache_entries.push((file_ino, DataInode::TorrentFile {
                            torrent_id,
                            file_id: file.id,
                            name: file.name.clone(),
                            size: file.size,
                        }));
                        entries.push((file_ino, offset_counter, fuser::FileType::RegularFile, file.name));
                        offset_counter += 1;
                    }
                }
                
                for (cache_ino, cache_inode) in cache_entries {
                    self.data_inodes.insert(cache_ino, cache_inode);
                }
            }
            DataInode::TorrentFile { .. } => {
                return None;
            }
        }
        
        Some(entries.into_iter().filter(|(_, o, _, _)| *o > offset).collect())
    }

    fn extract_source_path(&self, parent: u64) -> String {
        if parent == METADATA_INO {
            return String::new();
        }
        
        let full_path = self.get_full_path(parent);
        if full_path.starts_with("metadata/") {
            full_path["metadata/".len()..].to_string()
        } else {
            full_path
        }
    }

    fn process_torrent(&self, data: &[u8], source_path: &str, filename: &str) -> Result<(), i32> {
        let info = TorrentInfo::from_bytes(data.to_vec())
            .map_err(|e| {
                warn!("Failed to parse torrent {}: {:?}", filename, e);
                EINVAL
            })?;

        let metadata = info.metadata().map_err(|e| {
            error!("Failed to get torrent metadata {}: {:?}", filename, e);
            EIO
        })?;

        let info_hash_hex = hex::encode(metadata.info_hash);

        let db = match &self.db {
            Some(db) => db,
            None => {
                info!("Parsed torrent {} (no DB configured, skipping insert)", metadata.name);
                return Ok(());
            }
        };

        let mut db_guard = db.lock().map_err(|_| {
            error!("Database lock poisoned");
            EIO
        })?;

        let result = db_guard.insert_torrent(
            source_path,
            &metadata.name,
            metadata.total_size as i64,
            &info_hash_hex,
            metadata.num_files as i64,
        ).map_err(|e| {
            error!("Failed to insert torrent {}: {:?}", filename, e);
            EIO
        })?;

        match result {
            InsertTorrentResult::Inserted(torrent_id) => {
                let files: Vec<FileEntry> = metadata.files.iter().map(|f| FileEntry {
                    path: f.path.clone(),
                    size: f.size as i64,
                }).collect();

                db_guard.insert_files(torrent_id, &files).map_err(|e| {
                    error!("Failed to insert files for {}: {:?}", filename, e);
                    EIO
                })?;

                db_guard.set_torrent_data(torrent_id, data).map_err(|e| {
                    error!("Failed to store torrent data for {}: {:?}", filename, e);
                    EIO
                })?;

                info!(
                    "Persisted torrent '{}' ({} files, {} bytes) from {}",
                    metadata.name, metadata.num_files, metadata.total_size, 
                    if source_path.is_empty() { "root" } else { source_path }
                );
            }
            InsertTorrentResult::Duplicate(existing_id) => {
                info!(
                    "Torrent '{}' already exists (id={}), duplicate recorded",
                    metadata.name, existing_id
                );
            }
        }

        Ok(())
    }

    fn attr_for_dir(&self, ino: u64, writable: bool) -> FileAttr {
        FileAttr {
            ino,
            size: 0,
            blocks: 0,
            atime: UNIX_EPOCH + self.creation_time,
            mtime: UNIX_EPOCH + self.creation_time,
            ctime: UNIX_EPOCH + self.creation_time,
            crtime: UNIX_EPOCH + self.creation_time,
            kind: fuser::FileType::Directory,
            perm: if writable { 0o755 } else { 0o555 },
            nlink: 2,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            blksize: 512,
            flags: 0,
        }
    }

    fn attr_for_file(&self, ino: u64, size: u64) -> FileAttr {
        FileAttr {
            ino,
            size,
            blocks: (size + 511) / 512,
            atime: UNIX_EPOCH + self.creation_time,
            mtime: UNIX_EPOCH + self.creation_time,
            ctime: UNIX_EPOCH + self.creation_time,
            crtime: UNIX_EPOCH + self.creation_time,
            kind: fuser::FileType::RegularFile,
            perm: 0o644,
            nlink: 1,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            blksize: 512,
            flags: 0,
        }
    }

    fn get_full_path(&self, ino: u64) -> String {
        let mut path_parts = Vec::new();
        let mut current_ino = ino;
        
        while current_ino != ROOT_INO && current_ino != 0 {
            if let Some(data) = self.inodes.get(&current_ino) {
                match data {
                    InodeData::Directory { parent, name } => {
                        if !name.is_empty() {
                            path_parts.push(name.clone());
                        }
                        current_ino = *parent;
                    }
                    InodeData::File { parent, name, .. } => {
                        path_parts.push(name.clone());
                        current_ino = *parent;
                    }
                }
            } else {
                break;
            }
        }
        
        path_parts.reverse();
        path_parts.join("/")
    }

    fn is_metadata_child(&self, ino: u64) -> bool {
        if ino == METADATA_INO {
            return true;
        }
        
        let mut current_ino = ino;
        while current_ino != ROOT_INO && current_ino != 0 {
            if let Some(data) = self.inodes.get(&current_ino) {
                match data {
                    InodeData::Directory { parent, .. } => {
                        if *parent == METADATA_INO || current_ino == METADATA_INO {
                            return true;
                        }
                        current_ino = *parent;
                    }
                    InodeData::File { parent, .. } => {
                        current_ino = *parent;
                    }
                }
            } else {
                break;
            }
        }
        false
    }

    fn find_child_by_name(&self, parent: u64, name: &str) -> Option<u64> {
        for (ino, data) in &self.inodes {
            match data {
                InodeData::Directory { parent: p, name: n } if *p == parent && n == name => {
                    return Some(*ino);
                }
                InodeData::File { parent: p, name: n, .. } if *p == parent && n == name => {
                    return Some(*ino);
                }
                _ => {}
            }
        }
        None
    }

    fn read_torrent_file_data(&self, torrent_id: i64, file_id: i64, offset: usize, size: usize) -> Result<Vec<u8>, i32> {
        let db = self.get_db()?;
        let db_guard = db.lock().map_err(|_| {
            error!("Database lock poisoned");
            EIO
        })?;
        
        let torrent = db_guard.get_torrent_by_id(torrent_id)
            .map_err(|e| {
                error!("Failed to get torrent by id: {:?}", e);
                EIO
            })?
            .ok_or_else(|| {
                error!("Torrent not found: {}", torrent_id);
                ENOENT
            })?;
        
        let files = db_guard.get_files_by_torrent_id(torrent_id)
            .map_err(|e| {
                error!("Failed to get files for torrent: {:?}", e);
                EIO
            })?;
        
        let file = files.iter().find(|f| f.id == file_id)
            .ok_or_else(|| {
                error!("File not found: {}", file_id);
                ENOENT
            })?;
        
        let file_index = files.iter().position(|f| f.id == file_id)
            .ok_or_else(|| {
                error!("File index not found for file_id: {}", file_id);
                EIO
            })? as i32;
        
        drop(db_guard);
        
        let source_path = torrent.source_path.clone();
        let info_hash = torrent.info_hash.clone();
        
        let cache_key = format!("{}:{}", info_hash, file_id);
        {
            let cache = self.torrent_data_cache.lock().map_err(|_| EIO)?;
            if let Some(cached) = cache.get(&cache_key) {
                let end = std::cmp::min(offset + size, cached.len());
                if offset < cached.len() {
                    return Ok(cached[offset..end].to_vec());
                } else {
                    return Ok(Vec::new());
                }
            }
        }
        
        if let Some(dm) = &self.download_manager {
            let torrent_data = self.get_torrent_raw_data(&source_path)?;
            let info = TorrentInfo::from_bytes(torrent_data).map_err(|e| {
                error!("Failed to parse torrent info for download: {:?}", e);
                EIO
            })?;
            
            let mut dm_guard = dm.lock().map_err(|_| {
                error!("Download manager lock poisoned");
                EIO
            })?;
            
            match dm_guard.read_file_range(&info, file_index, offset as u64, size as u32) {
                Ok(data) => {
                    info!("Successfully read {} bytes from torrent file (torrent_id={}, file_id={})",
                          data.len(), torrent_id, file_id);
                    Ok(data)
                }
                Err(e) => {
                    error!("Failed to read from BitTorrent network: {:?}", e);
                    Err(EIO)
                }
            }
        } else {
            error!("Download manager not available");
            Err(EIO)
        }
    }
    
    fn get_torrent_raw_data(&self, source_path: &str) -> Result<Vec<u8>, i32> {
        let db = self.get_db()?;
        let db_guard = db.lock().map_err(|_| {
            error!("Database lock poisoned");
            EIO
        })?;
        
        let torrent = db_guard.get_torrent_by_source_path(source_path)
            .map_err(|e| {
                error!("Failed to get torrent: {:?}", e);
                EIO
            })?
            .ok_or_else(|| {
                error!("Torrent not found for source_path: {}", source_path);
                ENOENT
            })?;
        
        if let Some(ref data) = torrent.torrent_data {
            if !data.is_empty() {
                return Ok(data.clone());
            }
        }
        
        let metadata_dir_ino = METADATA_INO;
        for (ino, data) in &self.inodes {
            if let InodeData::File { name, data: file_data, .. } = data {
                if name.ends_with(".torrent") && !file_data.is_empty() {
                    if let Ok(info) = TorrentInfo::from_bytes(file_data.clone()) {
                        if let Ok(metadata) = info.metadata() {
                            if hex::encode(metadata.info_hash) == torrent.info_hash {
                                return Ok(file_data.clone());
                            }
                        }
                    }
                }
            }
            let _ = metadata_dir_ino;
        }
        
        Err(ENOENT)
    }
}

impl Filesystem for TorrentFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = name.to_string_lossy();
        
        if parent == ROOT_INO {
            match name_str.as_ref() {
                "metadata" => {
                    reply.entry(&Duration::from_secs(1), &self.attr_for_dir(METADATA_INO, true), 0);
                }
                "data" => {
                    reply.entry(&Duration::from_secs(1), &self.attr_for_dir(DATA_INO, false), 0);
                }
                _ => reply.error(ENOENT),
            }
            return;
        }
        
        if parent == DATA_INO || Self::is_data_ino(parent) {
            if let Some((ino, kind, size)) = self.lookup_data_inode(parent, &name_str) {
                match kind {
                    fuser::FileType::Directory => {
                        reply.entry(&Duration::from_secs(1), &self.attr_for_dir(ino, false), 0);
                    }
                    fuser::FileType::RegularFile => {
                        reply.entry(&Duration::from_secs(1), &self.attr_for_file(ino, size), 0);
                    }
                    _ => reply.error(ENOENT),
                }
                return;
            }
            reply.error(ENOENT);
            return;
        }
        
        if let Some(child_ino) = self.find_child_by_name(parent, &name_str) {
            if let Some(data) = self.inodes.get(&child_ino) {
                match data {
                    InodeData::Directory { .. } => {
                        reply.entry(&Duration::from_secs(1), &self.attr_for_dir(child_ino, true), 0);
                    }
                    InodeData::File { data: file_data, .. } => {
                        reply.entry(&Duration::from_secs(1), &self.attr_for_file(child_ino, file_data.len() as u64), 0);
                    }
                }
                return;
            }
        }
        
        reply.error(ENOENT);
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match ino {
            ROOT_INO => reply.attr(&Duration::from_secs(1), &self.attr_for_dir(ino, true)),
            METADATA_INO => reply.attr(&Duration::from_secs(1), &self.attr_for_dir(ino, true)),
            DATA_INO => reply.attr(&Duration::from_secs(1), &self.attr_for_dir(ino, false)),
            _ => {
                if Self::is_data_ino(ino) {
                    if let Some(data_inode) = self.data_inodes.get(&ino) {
                        match data_inode {
                            DataInode::SourcePathDir { .. } |
                            DataInode::TorrentRoot { .. } |
                            DataInode::TorrentDir { .. } => {
                                reply.attr(&Duration::from_secs(1), &self.attr_for_dir(ino, false));
                            }
                            DataInode::TorrentFile { size, .. } => {
                                reply.attr(&Duration::from_secs(1), &self.attr_for_file(ino, *size as u64));
                            }
                        }
                        return;
                    }
                    
                    let torrent_id = (ino - DATA_TORRENT_INO_BASE) as i64;
                    if ino >= DATA_TORRENT_INO_BASE && ino < DATA_DIR_INO_BASE {
                        if let Ok(db) = self.get_db() {
                            if let Ok(db_guard) = db.lock() {
                                if db_guard.get_torrent_by_id(torrent_id).ok().flatten().is_some() {
                                    reply.attr(&Duration::from_secs(1), &self.attr_for_dir(ino, false));
                                    return;
                                }
                            }
                        }
                    }
                    
                    reply.error(ENOENT);
                    return;
                }
                
                if let Some(data) = &self.inodes.get(&ino) {
                    match data {
                        InodeData::Directory { .. } => {
                            reply.attr(&Duration::from_secs(1), &self.attr_for_dir(ino, self.is_metadata_child(ino)));
                        }
                        InodeData::File { data: file_data, .. } => {
                            reply.attr(&Duration::from_secs(1), &self.attr_for_file(ino, file_data.len() as u64));
                        }
                    }
                } else {
                    reply.error(ENOENT);
                }
            }
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if ino == DATA_INO || Self::is_data_ino(ino) {
            if let Some(entries) = self.readdir_data(ino, offset) {
                for (entry_ino, entry_offset, kind, name) in entries {
                    if reply.add(entry_ino, entry_offset, kind, &name) {
                        break;
                    }
                }
                reply.ok();
                return;
            }
            reply.error(ENOENT);
            return;
        }
        
        let mut entries: Vec<(u64, i64, fuser::FileType, &str)> = vec![
            (ino, 1, fuser::FileType::Directory, "."),
        ];
        
        if ino == ROOT_INO {
            entries.push((ROOT_INO, 2, fuser::FileType::Directory, ".."));
            entries.push((METADATA_INO, 3, fuser::FileType::Directory, "metadata"));
            entries.push((DATA_INO, 4, fuser::FileType::Directory, "data"));
        } else if let Some(InodeData::Directory { parent, .. }) = self.inodes.get(&ino) {
            entries.push((*parent, 2, fuser::FileType::Directory, ".."));
            
            let mut offset_counter = entries.len() as i64 + 1;
            for (child_ino, data) in &self.inodes {
                match data {
                    InodeData::Directory { parent, name } if *parent == ino && !name.is_empty() => {
                        entries.push((*child_ino, offset_counter, fuser::FileType::Directory, name.as_str()));
                        offset_counter += 1;
                    }
                    InodeData::File { parent, name, .. } if *parent == ino => {
                        entries.push((*child_ino, offset_counter, fuser::FileType::RegularFile, name.as_str()));
                        offset_counter += 1;
                    }
                    _ => {}
                }
            }
        } else {
            reply.error(ENOTDIR);
            return;
        }

        for (ino_child, offset_child, kind, name) in entries.iter() {
            if *offset_child <= offset {
                continue;
            }
            if reply.add(*ino_child, *offset_child, *kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn open(&mut self, _req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        match ino {
            ROOT_INO | METADATA_INO | DATA_INO => reply.opened(0, 0),
            _ => {
                if Self::is_data_ino(ino) {
                    if let Some(DataInode::TorrentFile { .. }) = self.data_inodes.get(&ino) {
                        let fh = NEXT_FH.fetch_add(1, Ordering::SeqCst);
                        self.open_files.insert(fh, ino);
                        reply.opened(fh, 0);
                    } else {
                        reply.opened(0, 0);
                    }
                    return;
                }
                
                if self.inodes.contains_key(&ino) {
                    let fh = NEXT_FH.fetch_add(1, Ordering::SeqCst);
                    self.open_files.insert(fh, ino);
                    reply.opened(fh, 0);
                } else {
                    reply.error(ENOENT);
                }
            }
        }
    }

    fn release(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        if let Some(ino) = self.open_files.remove(&fh) {
            if let Some(InodeData::File { data, name, parent }) = self.inodes.get(&ino).cloned() {
                if name.ends_with(".torrent") && !data.is_empty() {
                    if data.len() > MAX_TORRENT_SIZE {
                        warn!("Torrent file {} exceeds size limit ({} bytes)", name, data.len());
                        self.inodes.remove(&ino);
                        reply.error(EFBIG);
                        return;
                    }

                    let source_path = self.extract_source_path(parent);
                    
                    {
                        let mut processing = self.processing_torrents.lock().unwrap();
                        if processing.contains_key(&source_path) {
                            warn!("Torrent {} already being processed, skipping", source_path);
                            reply.ok();
                            return;
                        }
                        processing.insert(source_path.clone(), ());
                    }

                    match self.process_torrent(&data, &source_path, &name) {
                        Ok(()) => {
                            info!("Successfully processed torrent: {}", name);
                        }
                        Err(e) => {
                            error!("Failed to process torrent {}: {}", name, e);
                            self.inodes.remove(&ino);
                            let mut processing = self.processing_torrents.lock().unwrap();
                            processing.remove(&source_path);
                            reply.error(e);
                            return;
                        }
                    }

                    let mut processing = self.processing_torrents.lock().unwrap();
                    processing.remove(&source_path);
                }
            }
        }
        
        reply.ok();
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        match ino {
            ROOT_INO | METADATA_INO | DATA_INO => reply.error(EISDIR),
            _ => {
                if Self::is_data_ino(ino) {
                    if let Some(DataInode::TorrentFile { torrent_id, file_id, name, size: file_size }) = self.data_inodes.get(&ino) {
                        let offset = offset as usize;
                        let read_size = size as usize;
                        
                        info!("Read request for torrent file: {} (torrent_id={}, file_id={}, offset={}, size={})", 
                              name, torrent_id, file_id, offset, read_size);
                        
                        let actual_size = *file_size as usize;
                        if offset >= actual_size {
                            reply.data(&[]);
                            return;
                        }
                        
                        let end = std::cmp::min(offset + read_size, actual_size);
                        let result_size = end - offset;
                        
                        match self.read_torrent_file_data(*torrent_id, *file_id, offset, result_size) {
                            Ok(data) => {
                                reply.data(&data);
                            }
                            Err(e) => {
                                warn!("Failed to read torrent file data: {:?}", e);
                                reply.error(EIO);
                            }
                        }
                    } else {
                        reply.error(ENOENT);
                    }
                    return;
                }
                
                if let Some(InodeData::File { data, .. }) = &self.inodes.get(&ino) {
                    let offset = offset as usize;
                    let end = std::cmp::min(offset + size as usize, data.len());
                    if offset < data.len() {
                        reply.data(&data[offset..end]);
                    } else {
                        reply.data(&[]);
                    }
                } else {
                    reply.error(ENOENT);
                }
            }
        }
    }

    fn opendir(&mut self, _req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        match ino {
            ROOT_INO | METADATA_INO | DATA_INO => reply.opened(0, 0),
            _ => {
                if Self::is_data_ino(ino) {
                    reply.opened(0, 0);
                    return;
                }
                
                if self.inodes.contains_key(&ino) {
                    reply.opened(0, 0);
                } else {
                    reply.error(ENOENT);
                }
            }
        }
    }

    fn releasedir(
        &mut self,
        _req: &Request,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        reply: fuser::ReplyEmpty,
    ) {
        reply.ok();
    }

    fn mknod(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        if !self.is_metadata_child(parent) {
            reply.error(EACCES);
            return;
        }
        
        let name_str = name.to_string_lossy();
        if !name_str.ends_with(".torrent") {
            reply.error(EACCES);
            return;
        }
        
        if self.find_child_by_name(parent, &name_str).is_some() {
            reply.error(EEXIST);
            return;
        }
        
        let new_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
        
        self.inodes.insert(new_ino, InodeData::File {
            parent,
            name: name_str.to_string(),
            data: Vec::new(),
        });
        
        info!("Created file {} with inode {} in {}", name_str, new_ino, self.get_full_path(parent));
        reply.entry(&Duration::from_secs(1), &self.attr_for_file(new_ino, 0), 0);
    }

    fn create(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        if !self.is_metadata_child(parent) {
            reply.error(EACCES);
            return;
        }
        
        let name_str = name.to_string_lossy();
        if !name_str.ends_with(".torrent") {
            reply.error(EACCES);
            return;
        }
        
        if self.find_child_by_name(parent, &name_str).is_some() {
            reply.error(EEXIST);
            return;
        }
        
        let new_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
        
        self.inodes.insert(new_ino, InodeData::File {
            parent,
            name: name_str.to_string(),
            data: Vec::new(),
        });
        
        let fh = NEXT_FH.fetch_add(1, Ordering::SeqCst);
        self.open_files.insert(fh, new_ino);
        
        info!("Created file {} with inode {} in {}", name_str, new_ino, self.get_full_path(parent));
        reply.created(&Duration::from_secs(1), &self.attr_for_file(new_ino, 0), 0, fh, 0);
    }

    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        if let Some(inode_data) = self.inodes.get_mut(&ino) {
            if let InodeData::File { data: ref mut file_data, name, .. } = inode_data {
                let offset = offset as usize;
                
                if offset > file_data.len() {
                    file_data.resize(offset, 0);
                }
                
                if offset + data.len() > file_data.len() {
                    file_data.resize(offset + data.len(), 0);
                }
                
                file_data[offset..offset + data.len()].copy_from_slice(data);
                
                info!("Wrote {} bytes to file {}", data.len(), name);
                reply.written(data.len() as u32);
            } else {
                reply.error(EISDIR);
            }
        } else {
            reply.error(ENOENT);
        }
    }

    fn mkdir(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        if !self.is_metadata_child(parent) {
            reply.error(EACCES);
            return;
        }
        
        let name_str = name.to_string_lossy();
        
        if self.find_child_by_name(parent, &name_str).is_some() {
            reply.error(EEXIST);
            return;
        }
        
        let new_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
        self.inodes.insert(new_ino, InodeData::Directory {
            parent,
            name: name_str.to_string(),
        });
        
        info!("Created directory {} with inode {} in {}", name_str, new_ino, self.get_full_path(parent));
        reply.entry(&Duration::from_secs(1), &self.attr_for_dir(new_ino, true), 0);
    }

    fn setattr(
        &mut self,
        _req: &Request,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<std::time::SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        if let Some(data) = &self.inodes.get(&ino) {
            match data {
                InodeData::Directory { .. } => {
                    reply.attr(&Duration::from_secs(1), &self.attr_for_dir(ino, self.is_metadata_child(ino)));
                }
                InodeData::File { data: file_data, .. } => {
                    reply.attr(&Duration::from_secs(1), &self.attr_for_file(ino, file_data.len() as u64));
                }
            }
        } else {
            reply.error(ENOENT);
        }
    }
}

fn fuse_allow_other_enabled() -> io::Result<bool> {
    let file = File::open("/etc/fuse.conf")?;
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim() == "user_allow_other" {
            return Ok(true);
        }
    }
    Ok(false)
}

fn user_in_fuse_group() -> bool {
    use std::fs;
    if let Ok(current_uid) = std::env::var("UID") {
        if let Ok(group_file) = fs::read_to_string("/etc/group") {
            for line in group_file.lines() {
                let parts: Vec<&str> = line.split(':').collect();
                if parts.len() >= 4 && parts[0] == "fuse" {
                    let members = parts[3];
                    if let Ok(current_user) = std::env::var("USER") {
                        if members.split(',').any(|m| m.trim() == current_user) {
                            return true;
                        }
                    }
                }
            }
        }
        let _ = current_uid;
    }
    
    if let Ok(output) = std::process::Command::new("groups").output() {
        let groups = String::from_utf8_lossy(&output.stdout);
        if groups.split_whitespace().any(|g| g == "fuse") {
            return true;
        }
    }
    
    false
}

fn main() {
    let log_level = std::env::var("RUST_LOG")
        .ok()
        .and_then(|v| match v.to_lowercase().as_str() {
            "trace" => Some(Level::TRACE),
            "debug" => Some(Level::DEBUG),
            "info" => Some(Level::INFO),
            "warn" => Some(Level::WARN),
            "error" => Some(Level::ERROR),
            _ => None,
        })
        .unwrap_or(Level::INFO);
    
    let subscriber = FmtSubscriber::builder()
        .with_max_level(log_level)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set tracing subscriber");

    let args = Args::parse();

    if !args.mountpoint.exists() {
        std::fs::create_dir_all(&args.mountpoint)
            .expect("Failed to create mountpoint");
    }

    let cache_path = args.cache.clone().unwrap_or_else(|| {
        args.mountpoint.join(".torrentfs/cache")
    });
    
    if !cache_path.exists() {
        if let Err(e) = std::fs::create_dir_all(&cache_path) {
            warn!("Failed to create cache directory {:?}: {:?}", cache_path, e);
        }
    }

    let db_path = if let Some(db_path) = &args.db {
        db_path.clone()
    } else {
        args.mountpoint.join(".torrentfs/metadata.db")
    };

    if let Some(parent) = db_path.parent() {
        if !parent.exists() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                warn!("Failed to create database directory: {:?}", e);
            }
        }
    }

    let allow_other_enabled = fuse_allow_other_enabled().unwrap_or(false);
    
    if allow_other_enabled {
        let options = vec![
            MountOption::FSName("torrentfs".to_string()),
            MountOption::AutoUnmount,
            MountOption::AllowOther,
        ];
        
        let db = match Database::open(&db_path) {
            Ok(db) => {
                info!("Database opened at {:?}", db_path);
                Some(db)
            }
            Err(e) => {
                if args.db.is_some() {
                    error!("Failed to open database: {:?}", e);
                    std::process::exit(1);
                }
                warn!("Failed to open database at {:?}: {:?}, running without persistence", db_path, e);
                None
            }
        };
        
        let fs = match db {
            Some(d) => TorrentFs::new_with_db_and_cache(d, cache_path.clone()),
            None => TorrentFs::new_with_cache_path(cache_path.clone()),
        };
        
        match fuser::mount2(fs, &args.mountpoint, &options) {
            Ok(()) => {
                info!("torrentfs unmounted successfully");
                return;
            }
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
                warn!("Mount with AllowOther failed, falling back to owner-only mode");
            }
            Err(e) => {
                error!("Failed to mount filesystem: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        warn!("user_allow_other not set in /etc/fuse.conf, mount will only be accessible by owner");
    }

    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
    ];
    
    let db = match Database::open(&db_path) {
        Ok(db) => {
            info!("Database opened at {:?}", db_path);
            Some(db)
        }
        Err(e) => {
            if args.db.is_some() {
                error!("Failed to open database: {:?}", e);
                std::process::exit(1);
            }
            warn!("Failed to open database at {:?}: {:?}, running without persistence", db_path, e);
            None
        }
    };
    
    let fs = match db {
        Some(d) => TorrentFs::new_with_db_and_cache(d, cache_path.clone()),
        None => TorrentFs::new_with_cache_path(cache_path.clone()),
    };
    
    match fuser::mount2(fs, &args.mountpoint, &options) {
        Ok(()) => info!("torrentfs unmounted successfully"),
        Err(e) => {
            let error_msg = e.to_string();
            if e.kind() == io::ErrorKind::PermissionDenied {
                let mut hints = Vec::new();
                
                if !allow_other_enabled {
                    hints.push("'user_allow_other' is not set in /etc/fuse.conf");
                }
                
                if !user_in_fuse_group() {
                    hints.push("user may not be in the 'fuse' group (some systems require this)");
                }
                
                hints.push("running in a container or restricted environment");
                hints.push("SELinux/AppArmor restrictions");
                hints.push("/dev/fuse device permissions");
                
                error!(
                    "Mount failed: Operation not permitted. Possible causes:\n  - {}",
                    hints.join("\n  - ")
                );
                std::process::exit(2);
            }
            error!("Failed to mount filesystem: {}", error_msg);
            std::process::exit(1);
        }
    }
}
