use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

use crate::db::{Database, FileEntry, InsertTorrentResult};
use crate::torrent_info::TorrentInfo;
use crate::download::DownloadManager;
use crate::cache::CacheManager;

const ROOT_INO: u64 = 1;
const METADATA_INO: u64 = 2;
const DATA_INO: u64 = 3;
const MAX_TORRENT_SIZE: usize = 10 * 1024 * 1024;
const DATA_TORRENT_INO_BASE: u64 = 1_000_000;
const DATA_DIR_INO_BASE: u64 = 2_000_000;
const DATA_FILE_INO_BASE: u64 = 3_000_000;

static NEXT_INO: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(4);
static NEXT_FH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

#[derive(Clone, Debug)]
pub enum InodeData {
    Directory { parent: u64, name: String },
    File { parent: u64, name: String, data: Vec<u8> },
}

#[derive(Clone, Debug)]
pub enum DataInode {
    SourcePathDir { path: String },
    TorrentRoot { torrent_id: i64, source_path: String, name: String },
    TorrentDir { torrent_id: i64, dir_id: i64, name: String },
    TorrentFile { torrent_id: i64, file_id: i64, name: String, size: i64 },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FileType {
    Directory,
    RegularFile,
}

#[derive(Debug, Clone)]
pub struct FileAttr {
    pub ino: u64,
    pub size: u64,
    pub kind: FileType,
    pub perm: u16,
}

#[derive(Debug)]
pub enum FsError {
    NoSuchEntry,
    NotDirectory,
    IsDirectory,
    PermissionDenied,
    FileExists,
    IoError(String),
    InvalidArgument,
    FileTooBig,
}

impl std::fmt::Display for FsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FsError::NoSuchEntry => write!(f, "No such file or directory"),
            FsError::NotDirectory => write!(f, "Not a directory"),
            FsError::IsDirectory => write!(f, "Is a directory"),
            FsError::PermissionDenied => write!(f, "Permission denied"),
            FsError::FileExists => write!(f, "File exists"),
            FsError::IoError(e) => write!(f, "IO error: {}", e),
            FsError::InvalidArgument => write!(f, "Invalid argument"),
            FsError::FileTooBig => write!(f, "File too big"),
        }
    }
}

impl std::error::Error for FsError {}

#[derive(Debug, Clone)]
pub struct DirEntry {
    pub ino: u64,
    pub kind: FileType,
    pub name: String,
}

pub struct MockTorrentFs {
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

impl MockTorrentFs {
    pub fn new() -> Self {
        Self::new_with_cache_path(PathBuf::from("/tmp/torrentfs-cache-mock"))
    }
    
    pub fn new_with_cache_path(cache_path: PathBuf) -> Self {
        let mut inodes = HashMap::new();
        inodes.insert(ROOT_INO, InodeData::Directory { parent: 0, name: String::new() });
        inodes.insert(METADATA_INO, InodeData::Directory { parent: ROOT_INO, name: "metadata".to_string() });
        inodes.insert(DATA_INO, InodeData::Directory { parent: ROOT_INO, name: "data".to_string() });
        
        if !cache_path.exists() {
            let _ = std::fs::create_dir_all(&cache_path);
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

    pub fn new_with_db(db: Database) -> Self {
        Self::new_with_db_and_cache(db, PathBuf::from("/tmp/torrentfs-cache-mock"))
    }
    
    pub fn new_with_db_and_cache(db: Database, cache_path: PathBuf) -> Self {
        let mut fs = Self::new_with_cache_path(cache_path);
        fs.db = Some(Arc::new(Mutex::new(db)));
        fs
    }

    pub fn new_in_memory() -> Self {
        let db = Database::open_in_memory().expect("Failed to create in-memory database");
        let temp_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
        Self::new_with_db_and_cache(db, temp_dir.path().to_path_buf())
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

    fn get_db(&self) -> Result<&Arc<Mutex<Database>>, FsError> {
        self.db.as_ref().ok_or(FsError::IoError("Database not available".to_string()))
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

        let prefixes = db_guard.get_source_path_prefixes("").ok()?;
        if prefixes.contains(&name.to_string()) {
            let full_path = name.to_string();
            let ino = NEXT_INO.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
            let ino = NEXT_INO.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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

    fn lookup_data_inode(&mut self, parent: u64, name: &str) -> Option<(u64, FileType, u64)> {
        let (ino, data_inode) = self.resolve_data_lookup(parent, name)?;
        
        self.data_inodes.insert(ino, data_inode.clone());
        
        match &data_inode {
            DataInode::SourcePathDir { .. } |
            DataInode::TorrentRoot { .. } |
            DataInode::TorrentDir { .. } => {
                Some((ino, FileType::Directory, 0))
            }
            DataInode::TorrentFile { size, .. } => {
                Some((ino, FileType::RegularFile, *size as u64))
            }
        }
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

    fn process_torrent(&self, data: &[u8], source_path: &str, filename: &str) -> Result<(), FsError> {
        let info = TorrentInfo::from_bytes(data.to_vec())
            .map_err(|e| FsError::InvalidArgument)?;

        let metadata = info.metadata()
            .map_err(|_| FsError::IoError("Failed to get metadata".to_string()))?;

        let info_hash_hex = hex::encode(metadata.info_hash);

        let db = match &self.db {
            Some(db) => db,
            None => return Ok(()),
        };

        let mut db_guard = db.lock().map_err(|_| FsError::IoError("Database lock poisoned".to_string()))?;

        let result = db_guard.insert_torrent(
            source_path,
            &metadata.name,
            metadata.total_size as i64,
            &info_hash_hex,
            metadata.num_files as i64,
        ).map_err(|e| FsError::IoError(e.to_string()))?;

        match result {
            InsertTorrentResult::Inserted(torrent_id) => {
                let files: Vec<FileEntry> = metadata.files.iter().map(|f| FileEntry {
                    path: f.path.clone(),
                    size: f.size as i64,
                }).collect();

                db_guard.insert_files(torrent_id, &files)
                    .map_err(|e| FsError::IoError(e.to_string()))?;
            }
            InsertTorrentResult::Duplicate(_) => {}
        }

        Ok(())
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

    fn attr_for_dir(&self, ino: u64, writable: bool) -> FileAttr {
        FileAttr {
            ino,
            size: 0,
            kind: FileType::Directory,
            perm: if writable { 0o755 } else { 0o555 },
        }
    }

    fn attr_for_file(&self, ino: u64, size: u64) -> FileAttr {
        FileAttr {
            ino,
            size,
            kind: FileType::RegularFile,
            perm: 0o644,
        }
    }

    pub fn lookup(&mut self, parent: u64, name: &str) -> Result<FileAttr, FsError> {
        if parent == ROOT_INO {
            match name {
                "metadata" => return Ok(self.attr_for_dir(METADATA_INO, true)),
                "data" => return Ok(self.attr_for_dir(DATA_INO, false)),
                _ => return Err(FsError::NoSuchEntry),
            }
        }
        
        if parent == DATA_INO || Self::is_data_ino(parent) {
            if let Some((ino, kind, size)) = self.lookup_data_inode(parent, name) {
                match kind {
                    FileType::Directory => return Ok(self.attr_for_dir(ino, false)),
                    FileType::RegularFile => return Ok(self.attr_for_file(ino, size)),
                }
            }
            return Err(FsError::NoSuchEntry);
        }
        
        if let Some(child_ino) = self.find_child_by_name(parent, name) {
            if let Some(data) = self.inodes.get(&child_ino) {
                match data {
                    InodeData::Directory { .. } => {
                        return Ok(self.attr_for_dir(child_ino, true));
                    }
                    InodeData::File { data: file_data, .. } => {
                        return Ok(self.attr_for_file(child_ino, file_data.len() as u64));
                    }
                }
            }
        }
        
        Err(FsError::NoSuchEntry)
    }

    pub fn getattr(&self, ino: u64) -> Result<FileAttr, FsError> {
        match ino {
            ROOT_INO => Ok(self.attr_for_dir(ino, true)),
            METADATA_INO => Ok(self.attr_for_dir(ino, true)),
            DATA_INO => Ok(self.attr_for_dir(ino, false)),
            _ => {
                if Self::is_data_ino(ino) {
                    if let Some(data_inode) = self.data_inodes.get(&ino) {
                        match data_inode {
                            DataInode::SourcePathDir { .. } |
                            DataInode::TorrentRoot { .. } |
                            DataInode::TorrentDir { .. } => {
                                return Ok(self.attr_for_dir(ino, false));
                            }
                            DataInode::TorrentFile { size, .. } => {
                                return Ok(self.attr_for_file(ino, *size as u64));
                            }
                        }
                    }
                    return Err(FsError::NoSuchEntry);
                }
                
                if let Some(data) = self.inodes.get(&ino) {
                    match data {
                        InodeData::Directory { .. } => {
                            Ok(self.attr_for_dir(ino, self.is_metadata_child(ino)))
                        }
                        InodeData::File { data: file_data, .. } => {
                            Ok(self.attr_for_file(ino, file_data.len() as u64))
                        }
                    }
                } else {
                    Err(FsError::NoSuchEntry)
                }
            }
        }
    }

    pub fn readdir(&mut self, ino: u64) -> Result<Vec<DirEntry>, FsError> {
        if Self::is_data_ino(ino) {
            return self.readdir_data(ino);
        }

        let mut entries: Vec<DirEntry> = vec![
            DirEntry { ino, kind: FileType::Directory, name: ".".to_string() },
        ];
        
        if ino == ROOT_INO {
            entries.push(DirEntry { ino: ROOT_INO, kind: FileType::Directory, name: "..".to_string() });
            entries.push(DirEntry { ino: METADATA_INO, kind: FileType::Directory, name: "metadata".to_string() });
            entries.push(DirEntry { ino: DATA_INO, kind: FileType::Directory, name: "data".to_string() });
        } else if let Some(InodeData::Directory { parent, .. }) = self.inodes.get(&ino) {
            entries.push(DirEntry { ino: *parent, kind: FileType::Directory, name: "..".to_string() });
        } else {
            return Err(FsError::NotDirectory);
        }
        
        for (child_ino, data) in &self.inodes {
            match data {
                InodeData::Directory { parent, name } if *parent == ino && !name.is_empty() => {
                    entries.push(DirEntry { ino: *child_ino, kind: FileType::Directory, name: name.clone() });
                }
                InodeData::File { parent, name, .. } if *parent == ino => {
                    entries.push(DirEntry { ino: *child_ino, kind: FileType::RegularFile, name: name.clone() });
                }
                _ => {}
            }
        }

        Ok(entries)
    }

    fn readdir_data(&mut self, ino: u64) -> Result<Vec<DirEntry>, FsError> {
        let mut entries: Vec<DirEntry> = Vec::new();
        let mut cache_entries: Vec<(u64, DataInode)> = Vec::new();
        
        if ino == DATA_INO {
            entries.push(DirEntry { ino: DATA_INO, kind: FileType::Directory, name: ".".to_string() });
            entries.push(DirEntry { ino: ROOT_INO, kind: FileType::Directory, name: "..".to_string() });
            
            {
                let db = self.get_db()?;
                let db_guard = db.lock().map_err(|_| FsError::IoError("Database lock poisoned".to_string()))?;
                
                let prefixes = db_guard.get_source_path_prefixes("")
                    .map_err(|e| FsError::IoError(e.to_string()))?;
                
                for prefix in prefixes {
                    let torrents = db_guard.get_torrents_by_source_path(&prefix)
                        .map_err(|e| FsError::IoError(e.to_string()))?;
                    if !torrents.is_empty() {
                        let torrent = torrents.first().unwrap();
                        let torrent_ino = Self::make_torrent_root_ino(torrent.id);
                        let name = format!("{}/", torrent.name);
                        cache_entries.push((torrent_ino, DataInode::TorrentRoot {
                            torrent_id: torrent.id,
                            source_path: torrent.source_path.clone(),
                            name: torrent.name.clone(),
                        }));
                        entries.push(DirEntry { ino: torrent_ino, kind: FileType::Directory, name });
                    } else {
                        let child_ino = NEXT_INO.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        cache_entries.push((child_ino, DataInode::SourcePathDir { path: prefix.clone() }));
                        entries.push(DirEntry { ino: child_ino, kind: FileType::Directory, name: prefix });
                    }
                }
            }
            
            for (cache_ino, cache_inode) in cache_entries {
                self.data_inodes.insert(cache_ino, cache_inode);
            }
            
            return Ok(entries);
        }

        let data_inode = self.data_inodes.get(&ino).cloned()
            .ok_or(FsError::NoSuchEntry)?;
        
        match data_inode {
            DataInode::SourcePathDir { path } => {
                entries.push(DirEntry { ino, kind: FileType::Directory, name: ".".to_string() });
                entries.push(DirEntry { ino: DATA_INO, kind: FileType::Directory, name: "..".to_string() });
                
                {
                    let db = self.get_db()?;
                    let db_guard = db.lock().map_err(|_| FsError::IoError("Database lock poisoned".to_string()))?;
                    
                    let sub_prefixes = db_guard.get_source_path_prefixes(&path)
                        .map_err(|e| FsError::IoError(e.to_string()))?;
                    for sub in sub_prefixes {
                        let new_path = if path.is_empty() { sub.clone() } else { format!("{}/{}", path, sub) };
                        let torrents = db_guard.get_torrents_by_source_path(&new_path)
                            .map_err(|e| FsError::IoError(e.to_string()))?;
                        
                        if !torrents.is_empty() {
                            let torrent = torrents.first().unwrap();
                            let torrent_ino = Self::make_torrent_root_ino(torrent.id);
                            let name = format!("{}/", torrent.name);
                            cache_entries.push((torrent_ino, DataInode::TorrentRoot {
                                torrent_id: torrent.id,
                                source_path: torrent.source_path.clone(),
                                name: torrent.name.clone(),
                            }));
                            entries.push(DirEntry { ino: torrent_ino, kind: FileType::Directory, name });
                        } else {
                            let child_ino = NEXT_INO.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            cache_entries.push((child_ino, DataInode::SourcePathDir { path: new_path.clone() }));
                            entries.push(DirEntry { ino: child_ino, kind: FileType::Directory, name: sub });
                        }
                    }
                    
                    let direct_torrents = db_guard.get_torrents_by_source_path(&path)
                        .map_err(|e| FsError::IoError(e.to_string()))?;
                    for torrent in direct_torrents {
                        let torrent_ino = Self::make_torrent_root_ino(torrent.id);
                        let name = format!("{}/", torrent.name);
                        cache_entries.push((torrent_ino, DataInode::TorrentRoot {
                            torrent_id: torrent.id,
                            source_path: torrent.source_path.clone(),
                            name: torrent.name.clone(),
                        }));
                        entries.push(DirEntry { ino: torrent_ino, kind: FileType::Directory, name });
                    }
                }
                
                for (cache_ino, cache_inode) in cache_entries {
                    self.data_inodes.insert(cache_ino, cache_inode);
                }
            }
            DataInode::TorrentRoot { torrent_id, .. } => {
                entries.push(DirEntry { ino, kind: FileType::Directory, name: ".".to_string() });
                entries.push(DirEntry { ino: DATA_INO, kind: FileType::Directory, name: "..".to_string() });
                
                {
                    let db = self.get_db()?;
                    let db_guard = db.lock().map_err(|_| FsError::IoError("Database lock poisoned".to_string()))?;
                    
                    let root_dirs = db_guard.get_torrent_directories_by_parent(None, torrent_id)
                        .map_err(|e| FsError::IoError(e.to_string()))?;
                    for dir in root_dirs {
                        let dir_ino = Self::make_torrent_dir_ino(dir.id);
                        cache_entries.push((dir_ino, DataInode::TorrentDir {
                            torrent_id,
                            dir_id: dir.id,
                            name: dir.name.clone(),
                        }));
                        entries.push(DirEntry { ino: dir_ino, kind: FileType::Directory, name: dir.name });
                    }
                    
                    let root_files = db_guard.get_root_files(torrent_id)
                        .map_err(|e| FsError::IoError(e.to_string()))?;
                    for file in root_files {
                        let file_ino = Self::make_torrent_file_ino(file.id);
                        cache_entries.push((file_ino, DataInode::TorrentFile {
                            torrent_id,
                            file_id: file.id,
                            name: file.name.clone(),
                            size: file.size,
                        }));
                        entries.push(DirEntry { ino: file_ino, kind: FileType::RegularFile, name: file.name });
                    }
                }
                
                for (cache_ino, cache_inode) in cache_entries {
                    self.data_inodes.insert(cache_ino, cache_inode);
                }
            }
            DataInode::TorrentDir { torrent_id, dir_id, .. } => {
                entries.push(DirEntry { ino, kind: FileType::Directory, name: ".".to_string() });
                entries.push(DirEntry { ino: Self::make_torrent_dir_ino(dir_id), kind: FileType::Directory, name: "..".to_string() });
                
                {
                    let db = self.get_db()?;
                    let db_guard = db.lock().map_err(|_| FsError::IoError("Database lock poisoned".to_string()))?;
                    
                    let sub_dirs = db_guard.get_torrent_directories_by_parent(Some(dir_id), torrent_id)
                        .map_err(|e| FsError::IoError(e.to_string()))?;
                    for dir in sub_dirs {
                        let sub_dir_ino = Self::make_torrent_dir_ino(dir.id);
                        cache_entries.push((sub_dir_ino, DataInode::TorrentDir {
                            torrent_id,
                            dir_id: dir.id,
                            name: dir.name.clone(),
                        }));
                        entries.push(DirEntry { ino: sub_dir_ino, kind: FileType::Directory, name: dir.name });
                    }
                    
                    let dir_files = db_guard.get_files_in_directory(dir_id)
                        .map_err(|e| FsError::IoError(e.to_string()))?;
                    for file in dir_files {
                        let file_ino = Self::make_torrent_file_ino(file.id);
                        cache_entries.push((file_ino, DataInode::TorrentFile {
                            torrent_id,
                            file_id: file.id,
                            name: file.name.clone(),
                            size: file.size,
                        }));
                        entries.push(DirEntry { ino: file_ino, kind: FileType::RegularFile, name: file.name });
                    }
                }
                
                for (cache_ino, cache_inode) in cache_entries {
                    self.data_inodes.insert(cache_ino, cache_inode);
                }
            }
            DataInode::TorrentFile { .. } => {
                return Err(FsError::NotDirectory);
            }
        }
        
        Ok(entries)
    }

    pub fn open(&mut self, ino: u64) -> Result<u64, FsError> {
        match ino {
            ROOT_INO | METADATA_INO | DATA_INO => Ok(0),
            _ => {
                if Self::is_data_ino(ino) {
                    if let Some(DataInode::TorrentFile { .. }) = self.data_inodes.get(&ino) {
                        let fh = NEXT_FH.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        self.open_files.insert(fh, ino);
                        return Ok(fh);
                    }
                    return Ok(0);
                }
                
                if self.inodes.contains_key(&ino) {
                    let fh = NEXT_FH.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    self.open_files.insert(fh, ino);
                    Ok(fh)
                } else {
                    Err(FsError::NoSuchEntry)
                }
            }
        }
    }

    pub fn release(&mut self, _ino: u64, fh: u64) -> Result<(), FsError> {
        if let Some(ino) = self.open_files.remove(&fh) {
            if let Some(InodeData::File { data, name, parent }) = self.inodes.get(&ino).cloned() {
                if name.ends_with(".torrent") && !data.is_empty() {
                    if data.len() > MAX_TORRENT_SIZE {
                        self.inodes.remove(&ino);
                        return Err(FsError::FileTooBig);
                    }

                    let source_path = self.extract_source_path(parent);
                    
                    {
                        let mut processing = self.processing_torrents.lock().unwrap();
                        if processing.contains_key(&source_path) {
                            return Ok(());
                        }
                        processing.insert(source_path.clone(), ());
                    }

                    let result = self.process_torrent(&data, &source_path, &name);

                    let mut processing = self.processing_torrents.lock().unwrap();
                    processing.remove(&source_path);
                    
                    return result;
                }
            }
        }
        
        Ok(())
    }

    pub fn read(&mut self, ino: u64, offset: usize, size: usize) -> Result<Vec<u8>, FsError> {
        match ino {
            ROOT_INO | METADATA_INO | DATA_INO => Err(FsError::IsDirectory),
            _ => {
                if Self::is_data_ino(ino) {
                    if let Some(DataInode::TorrentFile { size: file_size, .. }) = self.data_inodes.get(&ino) {
                        let actual_size = *file_size as usize;
                        if offset >= actual_size {
                            return Ok(Vec::new());
                        }
                        
                        let end = std::cmp::min(offset + size, actual_size);
                        let _result_size = end - offset;
                        
                        return Ok(vec![0u8; _result_size]);
                    }
                    return Err(FsError::NoSuchEntry);
                }
                
                if let Some(InodeData::File { data, .. }) = self.inodes.get(&ino) {
                    let end = std::cmp::min(offset + size, data.len());
                    if offset < data.len() {
                        Ok(data[offset..end].to_vec())
                    } else {
                        Ok(Vec::new())
                    }
                } else {
                    Err(FsError::NoSuchEntry)
                }
            }
        }
    }

    pub fn mknod(&mut self, parent: u64, name: &str) -> Result<FileAttr, FsError> {
        if !self.is_metadata_child(parent) {
            return Err(FsError::PermissionDenied);
        }
        
        if !name.ends_with(".torrent") {
            return Err(FsError::PermissionDenied);
        }
        
        if self.find_child_by_name(parent, name).is_some() {
            return Err(FsError::FileExists);
        }
        
        let new_ino = NEXT_INO.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        
        self.inodes.insert(new_ino, InodeData::File {
            parent,
            name: name.to_string(),
            data: Vec::new(),
        });
        
        Ok(self.attr_for_file(new_ino, 0))
    }

    pub fn create(&mut self, parent: u64, name: &str) -> Result<(FileAttr, u64), FsError> {
        if !self.is_metadata_child(parent) {
            return Err(FsError::PermissionDenied);
        }
        
        if !name.ends_with(".torrent") {
            return Err(FsError::PermissionDenied);
        }
        
        if self.find_child_by_name(parent, name).is_some() {
            return Err(FsError::FileExists);
        }
        
        let new_ino = NEXT_INO.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        
        self.inodes.insert(new_ino, InodeData::File {
            parent,
            name: name.to_string(),
            data: Vec::new(),
        });
        
        let fh = NEXT_FH.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.open_files.insert(fh, new_ino);
        
        Ok((self.attr_for_file(new_ino, 0), fh))
    }

    pub fn write(&mut self, ino: u64, offset: usize, data: &[u8]) -> Result<usize, FsError> {
        if let Some(inode_data) = self.inodes.get_mut(&ino) {
            if let InodeData::File { data: ref mut file_data, .. } = inode_data {
                if offset > file_data.len() {
                    file_data.resize(offset, 0);
                }
                
                if offset + data.len() > file_data.len() {
                    file_data.resize(offset + data.len(), 0);
                }
                
                file_data[offset..offset + data.len()].copy_from_slice(data);
                
                Ok(data.len())
            } else {
                Err(FsError::IsDirectory)
            }
        } else {
            Err(FsError::NoSuchEntry)
        }
    }

    pub fn mkdir(&mut self, parent: u64, name: &str) -> Result<FileAttr, FsError> {
        if !self.is_metadata_child(parent) {
            return Err(FsError::PermissionDenied);
        }
        
        if self.find_child_by_name(parent, name).is_some() {
            return Err(FsError::FileExists);
        }
        
        let new_ino = NEXT_INO.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inodes.insert(new_ino, InodeData::Directory {
            parent,
            name: name.to_string(),
        });
        
        Ok(self.attr_for_dir(new_ino, true))
    }

    pub fn get_inode_data(&self, ino: u64) -> Option<&InodeData> {
        self.inodes.get(&ino)
    }

    pub fn get_data_inode(&self, ino: u64) -> Option<&DataInode> {
        self.data_inodes.get(&ino)
    }

    pub fn db(&self) -> Option<&Arc<Mutex<Database>>> {
        self.db.as_ref()
    }
}

impl Default for MockTorrentFs {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_fs_basic() {
        let fs = MockTorrentFs::new_in_memory();
        
        let attr = fs.getattr(ROOT_INO).unwrap();
        assert_eq!(attr.kind, FileType::Directory);
        
        let attr = fs.getattr(METADATA_INO).unwrap();
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o755);
        
        let attr = fs.getattr(DATA_INO).unwrap();
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o555);
    }

    #[test]
    fn test_readdir_root() {
        let mut fs = MockTorrentFs::new_in_memory();
        
        let entries = fs.readdir(ROOT_INO).unwrap();
        assert!(entries.iter().any(|e| e.name == "metadata"));
        assert!(entries.iter().any(|e| e.name == "data"));
    }

    #[test]
    fn test_mkdir_and_lookup() {
        let mut fs = MockTorrentFs::new_in_memory();
        
        let attr = fs.mkdir(METADATA_INO, "testdir").unwrap();
        assert_eq!(attr.kind, FileType::Directory);
        
        let lookup_attr = fs.lookup(METADATA_INO, "testdir").unwrap();
        assert_eq!(lookup_attr.ino, attr.ino);
    }

    #[test]
    fn test_create_write_read_file() {
        let mut fs = MockTorrentFs::new_in_memory();
        
        let (attr, fh) = fs.create(METADATA_INO, "test.torrent").unwrap();
        assert_eq!(attr.kind, FileType::RegularFile);
        
        let data = b"test data";
        let written = fs.write(attr.ino, 0, data).unwrap();
        assert_eq!(written, data.len());
        
        let read_data = fs.read(attr.ino, 0, data.len()).unwrap();
        assert_eq!(read_data.as_slice(), data);
    }

    #[test]
    fn test_create_non_torrent_denied() {
        let mut fs = MockTorrentFs::new_in_memory();
        
        let result = fs.create(METADATA_INO, "test.txt");
        assert!(matches!(result, Err(FsError::PermissionDenied)));
    }

    #[test]
    fn test_create_in_data_denied() {
        let mut fs = MockTorrentFs::new_in_memory();
        
        let result = fs.create(DATA_INO, "test.torrent");
        assert!(matches!(result, Err(FsError::PermissionDenied)));
    }

    #[test]
    fn test_lookup_nonexistent() {
        let mut fs = MockTorrentFs::new_in_memory();
        
        let result = fs.lookup(ROOT_INO, "nonexistent");
        assert!(matches!(result, Err(FsError::NoSuchEntry)));
    }

    #[test]
    fn test_file_exists_error() {
        let mut fs = MockTorrentFs::new_in_memory();
        
        fs.create(METADATA_INO, "test.torrent").unwrap();
        
        let result = fs.create(METADATA_INO, "test.torrent");
        assert!(matches!(result, Err(FsError::FileExists)));
    }

    #[test]
    fn test_write_to_directory_error() {
        let mut fs = MockTorrentFs::new_in_memory();
        
        let result = fs.write(METADATA_INO, 0, b"data");
        assert!(matches!(result, Err(FsError::IsDirectory)));
    }

    #[test]
    fn test_read_directory_error() {
        let mut fs = MockTorrentFs::new_in_memory();
        
        let result = fs.read(METADATA_INO, 0, 100);
        assert!(matches!(result, Err(FsError::IsDirectory)));
    }

    #[test]
    fn test_nested_directories() {
        let mut fs = MockTorrentFs::new_in_memory();
        
        let dir1 = fs.mkdir(METADATA_INO, "dir1").unwrap();
        let dir2 = fs.mkdir(dir1.ino, "dir2").unwrap();
        
        let (attr, _fh) = fs.create(dir2.ino, "test.torrent").unwrap();
        
        let lookup = fs.lookup(dir2.ino, "test.torrent").unwrap();
        assert_eq!(lookup.ino, attr.ino);
    }
}
