use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyWrite, Request, ReplyCreate,
};
use libc::{EEXIST, EINVAL, ENOENT, ENOTDIR, EFBIG, EIO};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::fuse_async::{FuseAsyncRuntime, FuseCommand, FuseError, TorrentInfo, FileInfo, ParsedTorrentInfo, PersistResult, FileInfoForRead};
use torrentfs::metadata::MetadataManager;
use torrentfs_libtorrent::Session;

const TTL: Duration = Duration::from_secs(1);
const MAX_FILE_SIZE: usize = 10 * 1024 * 1024;

pub const INO_ROOT: u64 = 1;
pub const INO_METADATA: u64 = 2;
pub const INO_DATA: u64 = 3;
const INO_DYNAMIC_START: u64 = 100;

pub fn dir_attr(ino: u64) -> FileAttr {
    FileAttr {
        ino,
        size: 0,
        blocks: 0,
        atime: UNIX_EPOCH,
        mtime: UNIX_EPOCH,
        ctime: UNIX_EPOCH,
        crtime: UNIX_EPOCH,
        kind: FileType::Directory,
        perm: 0o755,
        nlink: 2,
        uid: 0,
        gid: 0,
        rdev: 0,
        flags: 0,
        blksize: 512,
    }
}

fn file_attr(ino: u64, size: u64) -> FileAttr {
    let now = SystemTime::now();
    FileAttr {
        ino,
        size,
        blocks: (size + 511) / 512,
        atime: now,
        mtime: now,
        ctime: now,
        crtime: now,
        kind: FileType::RegularFile,
        perm: 0o644,
        nlink: 1,
        uid: 0,
        gid: 0,
        rdev: 0,
        flags: 0,
        blksize: 512,
    }
}

fn fuse_error_to_errno(e: &FuseError) -> i32 {
    match e {
        FuseError::TorrentParseError(_) => EINVAL,
        FuseError::Timeout(_) => EIO,
        FuseError::DatabaseError(_) => EIO,
        FuseError::SessionError(_) => EIO,
        FuseError::ChannelClosed => EIO,
    }
}

struct OpenFile {
    path: String,
    data: Vec<u8>,
}

struct OpenTorrentFile {
    #[allow(dead_code)]
    torrent_name: String,
    #[allow(dead_code)]
    file_path: String,
    info_hash: String,
    file_size: i64,
    piece_size: u32,
    #[allow(dead_code)]
    first_piece: i64,
    last_piece: i64,
    file_offset: u64,
}

struct MetadataEntry {
    path: String,
    size: u64,
}

struct MetadataDir {
    ino: u64,
    parent_ino: u64,
    relative_path: String,
}

impl Clone for MetadataDir {
    fn clone(&self) -> Self {
        Self {
            ino: self.ino,
            parent_ino: self.parent_ino,
            relative_path: self.relative_path.clone(),
        }
    }
}

pub struct TorrentFsFilesystem {
    state_dir: PathBuf,
    next_ino: u64,
    next_fh: u64,
    open_files: HashMap<u64, OpenFile>,
    open_torrent_files: HashMap<u64, OpenTorrentFile>,
    metadata_entries: HashMap<u64, MetadataEntry>,
    metadata_dirs: HashMap<u64, MetadataDir>,
    async_runtime: Option<Arc<FuseAsyncRuntime>>,
}

impl TorrentFsFilesystem {
    pub fn new(state_dir: PathBuf) -> Self {
        Self {
            state_dir,
            next_ino: INO_DYNAMIC_START,
            next_fh: 1,
            open_files: HashMap::new(),
            open_torrent_files: HashMap::new(),
            metadata_entries: HashMap::new(),
            metadata_dirs: HashMap::new(),
            async_runtime: None,
        }
    }

    pub fn new_with_async(
        state_dir: PathBuf,
        metadata_manager: Arc<MetadataManager>,
        session: Arc<Session>,
    ) -> Self {
        let async_runtime = Arc::new(FuseAsyncRuntime::new(
            Arc::clone(&metadata_manager),
            Arc::clone(&session),
        ));
        
        let mut fs = Self {
            state_dir,
            next_ino: INO_DYNAMIC_START,
            next_fh: 1,
            open_files: HashMap::new(),
            open_torrent_files: HashMap::new(),
            metadata_entries: HashMap::new(),
            metadata_dirs: HashMap::new(),
            async_runtime: Some(async_runtime),
        };
        
        fs.rebuild_metadata_dirs();
        fs
    }

    pub fn new_with_core(
        state_dir: PathBuf,
        metadata_manager: Arc<MetadataManager>,
        _tokio_runtime: tokio::runtime::Runtime,
        session: Arc<Session>,
    ) -> Self {
        Self::new_with_async(state_dir, metadata_manager, session)
    }

    pub fn new_with_download_coordinator(
        state_dir: PathBuf,
        metadata_manager: Arc<MetadataManager>,
        session: Arc<Session>,
        download_coordinator: Arc<torrentfs::DownloadCoordinator>,
    ) -> Self {
        let async_runtime = Arc::new(FuseAsyncRuntime::new_with_download_coordinator(
            Arc::clone(&metadata_manager),
            Arc::clone(&session),
            download_coordinator,
        ));
        
        let mut fs = Self {
            state_dir,
            next_ino: INO_DYNAMIC_START,
            next_fh: 1,
            open_files: HashMap::new(),
            open_torrent_files: HashMap::new(),
            metadata_entries: HashMap::new(),
            metadata_dirs: HashMap::new(),
            async_runtime: Some(async_runtime),
        };
        
        fs.rebuild_metadata_dirs();
        fs
    }

    fn allocate_ino(&mut self) -> u64 {
        let ino = self.next_ino;
        self.next_ino += 1;
        ino
    }

    fn allocate_fh(&mut self) -> u64 {
        let fh = self.next_fh;
        self.next_fh += 1;
        fh
    }

    fn torrent_inode(&self, source_path: &str, torrent_name: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        source_path.hash(&mut hasher);
        torrent_name.hash(&mut hasher);
        (hasher.finish() & 0x0FFFFFFFFFFFFFFF) | 0x8000000000000000
    }

    fn file_inode(&self, torrent_name: &str, file_path: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        torrent_name.hash(&mut hasher);
        file_path.hash(&mut hasher);
        (hasher.finish() & 0x0FFFFFFFFFFFFFFF) | 0x9000000000000000
    }

    fn data_dir_inode(&self, path: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        (hasher.finish() & 0x0FFFFFFFFFFFFFFF) | 0xA000000000000000
    }

    fn torrent_file_dir_inode(&self, torrent_name: &str, dir_path: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        torrent_name.hash(&mut hasher);
        dir_path.hash(&mut hasher);
        (hasher.finish() & 0x0FFFFFFFFFFFFFFF) | 0xB000000000000000
    }

    fn find_metadata_dir_by_parent_and_name(&self, parent: u64, name: &str) -> Option<(u64, &MetadataDir)> {
        for (ino, dir) in &self.metadata_dirs {
            if dir.parent_ino == parent {
                let dir_name = dir.relative_path.rsplit('/').next().unwrap_or(&dir.relative_path);
                if dir_name == name {
                    return Some((*ino, dir));
                }
            }
        }
        None
    }

    #[allow(dead_code)]
    fn is_under_metadata(&self, ino: u64) -> bool {
        if ino == INO_METADATA {
            return true;
        }
        self.metadata_dirs.contains_key(&ino)
    }

    fn list_torrents_safe(&self) -> Result<Vec<TorrentInfo>, FuseError> {
        if let Some(runtime) = &self.async_runtime {
            runtime.send_command_with_timeout(|reply| FuseCommand::ListTorrents { reply })
        } else {
            Ok(Vec::new())
        }
    }

    fn get_torrent_files_safe(&self, torrent_name: &str) -> Result<Vec<FileInfo>, FuseError> {
        if let Some(runtime) = &self.async_runtime {
            runtime.send_command_with_timeout(|reply| FuseCommand::GetTorrentFiles {
                torrent_name: torrent_name.to_string(),
                reply,
            })
        } else {
            Ok(Vec::new())
        }
    }

    fn process_torrent_data_safe(&self, data: &[u8], source_path: &str) -> Result<ParsedTorrentInfo, FuseError> {
        if let Some(runtime) = &self.async_runtime {
            runtime.send_command_with_timeout(|reply| FuseCommand::ProcessTorrentData {
                data: data.to_vec(),
                source_path: source_path.to_string(),
                reply,
            })
        } else {
            Err(FuseError::ChannelClosed)
        }
    }

    fn add_torrent_paused_safe(&self, data: &[u8], save_path: &str) -> Result<(), FuseError> {
        if let Some(runtime) = &self.async_runtime {
            runtime.send_command_with_timeout(|reply| FuseCommand::AddTorrentPaused {
                data: data.to_vec(),
                save_path: save_path.to_string(),
                reply,
            })
        } else {
            Err(FuseError::ChannelClosed)
        }
    }

    fn persist_to_db_safe(&self, parsed: &ParsedTorrentInfo) -> Result<PersistResult, FuseError> {
        if let Some(runtime) = &self.async_runtime {
            runtime.send_command_with_timeout(|reply| FuseCommand::PersistToDb {
                parsed: parsed.clone(),
                reply,
            })
        } else {
            Err(FuseError::ChannelClosed)
        }
    }

    fn get_file_info_for_read(&self, torrent_name: &str, file_path: &str) -> Result<FileInfoForRead, FuseError> {
        if let Some(runtime) = &self.async_runtime {
            runtime.send_command_with_timeout(|reply| FuseCommand::GetFileInfoForInode {
                torrent_name: torrent_name.to_string(),
                file_path: file_path.to_string(),
                reply,
            })
        } else {
            Err(FuseError::ChannelClosed)
        }
    }

    fn read_piece_from_torrent(&self, info_hash: &str, piece_index: u32) -> Result<Vec<u8>, FuseError> {
        if let Some(runtime) = &self.async_runtime {
            runtime.send_command_with_timeout(|reply| FuseCommand::ReadFilePiece {
                info_hash: info_hash.to_string(),
                piece_index,
                reply,
            })
        } else {
            Err(FuseError::ChannelClosed)
        }
    }
    
    fn find_torrent_file_by_ino(&self, ino: u64) -> Option<(String, String)> {
        if ino < 0x9000000000000000 || ino >= 0xA000000000000000 {
            return None;
        }
        
        if let Some(runtime) = &self.async_runtime {
            let result: Result<Vec<TorrentInfo>, _> = runtime.send_command_with_timeout(|reply| {
                FuseCommand::ListTorrents { reply }
            });
            
            match result {
                Ok(torrents) => {
                    for torrent in torrents {
                        let files_result: Result<Vec<FileInfo>, _> = runtime.send_command_with_timeout(|reply| {
                            FuseCommand::GetTorrentFiles {
                                torrent_name: torrent.name.clone(),
                                reply,
                            }
                        });
                        
                        match files_result {
                            Ok(files) => {
                                for file in files {
                                    let file_ino = self.file_inode(&torrent.name, &file.path);
                                    if file_ino == ino {
                                        return Some((torrent.name, file.path));
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    torrent_name = %torrent.name,
                                    error = %e,
                                    "Failed to get torrent files in find_torrent_file_by_ino"
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "Failed to list torrents in find_torrent_file_by_ino"
                    );
                }
            }
        }
        
        None
    }

    /// Rebuilds `metadata_dirs` from database `torrents.source_path` entries.
    ///
    /// This ensures metadata directory structures persist across FUSE restarts.
    /// Empty directories (created via `mkdir` without torrents) are NOT restored;
    /// only directories with actual torrent files are rebuilt.
    ///
    /// Failure to query the database is logged but not fatal - the filesystem
    /// will continue with an empty `metadata_dirs`, which is safe but may
    /// require users to re-create directory structures.
    pub fn rebuild_metadata_dirs(&mut self) {
        if let Some(runtime) = &self.async_runtime {
            let result: Result<Vec<TorrentInfo>, _> = runtime.send_command_with_timeout(|reply| {
                FuseCommand::ListTorrents { reply }
            });

            match result {
                Ok(torrents) => {
                    for torrent in torrents {
                        if !torrent.source_path.is_empty() {
                            self.ensure_metadata_dirs_for_path(&torrent.source_path);
                        }
                    }
                    tracing::info!(
                        dirs_count = self.metadata_dirs.len(),
                        "Rebuilt metadata_dirs from database"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "Failed to rebuild metadata_dirs from database"
                    );
                }
            }
        }
    }

    fn ensure_metadata_dirs_for_path(&mut self, source_path: &str) {
        let parts: Vec<&str> = source_path.split('/').filter(|p| !p.trim().is_empty()).collect();
        if parts.is_empty() {
            return;
        }

        let mut current_path = String::new();
        let mut parent_ino = INO_METADATA;

        for part in parts {
            let new_path = if current_path.is_empty() {
                part.to_string()
            } else {
                format!("{}/{}", current_path, part)
            };

            let existing = self.find_metadata_dir_by_parent_and_name(parent_ino, part);
            let ino = match existing {
                Some((ino, _)) => ino,
                None => {
                    let ino = self.allocate_ino();
                    self.metadata_dirs.insert(ino, MetadataDir {
                        ino,
                        parent_ino,
                        relative_path: new_path.clone(),
                    });
                    ino
                }
            };

            parent_ino = ino;
            current_path = new_path;
        }
    }
}

pub fn attr_for_ino(ino: u64) -> Option<FileAttr> {
    match ino {
        INO_ROOT | INO_METADATA | INO_DATA => Some(dir_attr(ino)),
        _ => None,
    }
}

impl Filesystem for TorrentFsFilesystem {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        if parent == INO_ROOT {
            match name.to_str() {
                Some("metadata") => {
                    reply.entry(&TTL, &dir_attr(INO_METADATA), 0);
                }
                Some("data") => {
                    reply.entry(&TTL, &dir_attr(INO_DATA), 0);
                }
                _ => reply.error(ENOENT),
            }
        } else if parent == INO_METADATA {
            let name_str = name.to_string_lossy();
            
            for (ino, entry) in &self.metadata_entries {
                let entry_name = entry.path.rsplit('/').next().unwrap_or(&entry.path);
                if entry_name == name_str {
                    reply.entry(&TTL, &file_attr(*ino, entry.size), 0);
                    return;
                }
            }
            
            if let Some((ino, _dir)) = self.find_metadata_dir_by_parent_and_name(parent, &name_str) {
                reply.entry(&TTL, &dir_attr(ino), 0);
                return;
            }
            
            reply.error(ENOENT);
        } else if parent == INO_DATA {
            let name_str = name.to_string_lossy();
            
            match self.list_torrents_safe() {
                Ok(torrents) => {
                    for torrent in &torrents {
                        if torrent.source_path.is_empty() {
                            if torrent.name == name_str {
                                let ino = self.torrent_inode(&torrent.source_path, &torrent.name);
                                reply.entry(&TTL, &dir_attr(ino), 0);
                                return;
                            }
                        } else {
                            let first_part = torrent.source_path.split('/').next().unwrap_or(&torrent.source_path);
                            if first_part == name_str {
                                let ino = self.data_dir_inode(first_part);
                                reply.entry(&TTL, &dir_attr(ino), 0);
                                return;
                            }
                        }
                    }
                    reply.error(ENOENT);
                }
                Err(e) => {
                    tracing::error!("Failed to list torrents in lookup: {}", e);
                    reply.error(EIO);
                }
            }
        } else if self.metadata_dirs.contains_key(&parent) {
            let name_str = name.to_string_lossy();
            let parent_dir = self.metadata_dirs.get(&parent).unwrap();
            let prefix = &parent_dir.relative_path;
            
            for (ino, entry) in &self.metadata_entries {
                if entry.path.starts_with(&format!("{}/", prefix)) {
                    let rest = &entry.path[prefix.len() + 1..];
                    if rest == name_str {
                        reply.entry(&TTL, &file_attr(*ino, entry.size), 0);
                        return;
                    }
                    if rest.starts_with(&format!("{}/", name_str)) {
                        if let Some((ino, _)) = self.find_metadata_dir_by_parent_and_name(parent, &name_str) {
                            reply.entry(&TTL, &dir_attr(ino), 0);
                            return;
                        }
                    }
                }
            }
            
            for (ino, dir) in &self.metadata_dirs {
                if dir.parent_ino == parent && dir.relative_path == format!("{}/{}", prefix, name_str) {
                    reply.entry(&TTL, &dir_attr(*ino), 0);
                    return;
                }
            }
            
            for (ino, entry) in &self.metadata_entries {
                if entry.path.starts_with(&format!("{}/", prefix)) {
                    let rest = &entry.path[prefix.len() + 1..];
                    if rest == name_str {
                        reply.entry(&TTL, &file_attr(*ino, entry.size), 0);
                        return;
                    }
                }
            }
            
            reply.error(ENOENT);
        } else if self.async_runtime.is_some() {
            match self.list_torrents_safe() {
                Ok(torrents) => {
                    for torrent in &torrents {
                        let torrent_ino = self.torrent_inode(&torrent.source_path, &torrent.name);
                        if parent == torrent_ino {
                            let name_str = name.to_string_lossy();
                            
                            match self.get_torrent_files_safe(&torrent.name) {
                                Ok(files) => {
                                    let mut seen_dirs: std::collections::HashSet<String> = std::collections::HashSet::new();
                                    
                                    for file in &files {
                                        let relative_path = file.path.strip_prefix(&torrent.name)
                                            .unwrap_or(&file.path);
                                        let relative_path = relative_path.strip_prefix('/').unwrap_or(relative_path);
                                        
                                        if relative_path.is_empty() {
                                            let file_name = file.path.rsplit('/').next().unwrap_or(&file.path);
                                            if file_name == name_str {
                                                let ino = self.file_inode(&torrent.name, &file.path);
                                                reply.entry(&TTL, &file_attr(ino, file.size as u64), 0);
                                                return;
                                            }
                                            continue;
                                        }
                                        
                                        let parts: Vec<&str> = relative_path.split('/').collect();
                                        
                                        if parts.len() == 1 && parts[0] == name_str {
                                            let ino = self.file_inode(&torrent.name, &file.path);
                                            reply.entry(&TTL, &file_attr(ino, file.size as u64), 0);
                                            return;
                                        } else if parts.len() > 1 {
                                            let dir_name = parts[0];
                                            if dir_name == name_str && !seen_dirs.contains(dir_name) {
                                                seen_dirs.insert(dir_name.to_string());
                                                let dir_path = format!("{}/{}", torrent.name, dir_name);
                                                let dir_ino = self.torrent_file_dir_inode(&torrent.name, &dir_path);
                                                reply.entry(&TTL, &dir_attr(dir_ino), 0);
                                                return;
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Failed to get torrent files in lookup: {}", e);
                                }
                            }
                            reply.error(ENOENT);
                            return;
                        }
                        
                        if parent >= 0xB000000000000000 && parent < 0xC000000000000000 {
                            match self.get_torrent_files_safe(&torrent.name) {
                                Ok(files) => {
                                    let mut current_dir_path: Option<String> = None;
                                    
                                    for file in &files {
                                        let relative_path = file.path.strip_prefix(&torrent.name)
                                            .unwrap_or(&file.path);
                                        let relative_path = relative_path.strip_prefix('/').unwrap_or(relative_path);
                                        
                                        let parts: Vec<&str> = relative_path.split('/').collect();
                                        let mut dir_path = torrent.name.clone();
                                        
                                        for (i, part) in parts.iter().enumerate() {
                                            if i < parts.len() - 1 {
                                                dir_path = format!("{}/{}", dir_path, part);
                                                let dir_ino = self.torrent_file_dir_inode(&torrent.name, &dir_path);
                                                
                                                if dir_ino == parent {
                                                    current_dir_path = Some(dir_path.clone());
                                                    break;
                                                }
                                            }
                                        }
                                        
                                        if current_dir_path.is_some() {
                                            break;
                                        }
                                    }
                                    
                                    if let Some(current_dir) = current_dir_path {
                                        let name_str = name.to_string_lossy();
                                        let mut seen_names: std::collections::HashSet<String> = std::collections::HashSet::new();
                                        
                                        for file in &files {
                                            let relative_path = file.path.strip_prefix(&torrent.name)
                                                .unwrap_or(&file.path);
                                            let relative_path = relative_path.strip_prefix('/').unwrap_or(relative_path);
                                            
                                            let starts_with_dir = relative_path.starts_with(&current_dir);
                                            let full_relative = format!("{}/{}", torrent.name, relative_path);
                                            let full_starts = full_relative.starts_with(&current_dir);
                                            
                                            if starts_with_dir || full_starts {
                                                let rest = if starts_with_dir {
                                                    relative_path[current_dir.len()..].to_string()
                                                } else {
                                                    full_relative[current_dir.len()..].to_string()
                                                };
                                                
                                                let rest = rest.strip_prefix('/').unwrap_or(&rest);
                                                let parts: Vec<&str> = rest.split('/').collect();
                                                
                                                if parts.len() == 1 && parts[0] == name_str {
                                                    let ino = self.file_inode(&torrent.name, &file.path);
                                                    reply.entry(&TTL, &file_attr(ino, file.size as u64), 0);
                                                    return;
                                                } else if parts.len() > 1 {
                                                    let subdir_name = parts[0];
                                                    if subdir_name == name_str && !seen_names.contains(subdir_name) {
                                                        seen_names.insert(subdir_name.to_string());
                                                        let subdir_path = format!("{}/{}", current_dir, subdir_name);
                                                        let subdir_ino = self.torrent_file_dir_inode(&torrent.name, &subdir_path);
                                                        reply.entry(&TTL, &dir_attr(subdir_ino), 0);
                                                        return;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Failed to get torrent files in lookup: {}", e);
                                }
                            }
                            reply.error(ENOENT);
                            return;
                        }
                    }
                
                    let mut data_dir_path = String::new();
                    for torrent in &torrents {
                        if !torrent.source_path.is_empty() {
                            let parts: Vec<&str> = torrent.source_path.split('/').filter(|p| !p.trim().is_empty()).collect();
                            let mut current_path = String::new();
                            for (i, part) in parts.iter().enumerate() {
                                if i > 0 {
                                    current_path.push('/');
                                }
                                current_path.push_str(part);
                                let dir_ino = self.data_dir_inode(&current_path);
                                if dir_ino == parent {
                                    data_dir_path = current_path.clone();
                                    break;
                                }
                            }
                        }
                    }
                    
                    if !data_dir_path.is_empty() {
                        let name_str = name.to_string_lossy();
                        for torrent in torrents {
                            let is_in_dir = torrent.source_path == data_dir_path 
                                || torrent.source_path.starts_with(&format!("{}/", data_dir_path));
                            if is_in_dir {
                                if torrent.source_path == data_dir_path {
                                    if torrent.name == name_str {
                                        let torrent_ino = self.torrent_inode(&torrent.source_path, &torrent.name);
                                        reply.entry(&TTL, &dir_attr(torrent_ino), 0);
                                        return;
                                    }
                                } else {
                                    let rest = &torrent.source_path[data_dir_path.len() + 1..];
                                    if !rest.is_empty() {
                                        let parts: Vec<&str> = rest.split('/').filter(|p| !p.trim().is_empty()).collect();
                                        if !parts.is_empty() {
                                            let first_part = parts[0];
                                            if first_part == name_str {
                                                let new_path = format!("{}/{}", data_dir_path, name_str);
                                                let dir_ino = self.data_dir_inode(&new_path);
                                                reply.entry(&TTL, &dir_attr(dir_ino), 0);
                                                return;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    
                    reply.error(ENOENT);
                }
                Err(e) => {
                    tracing::error!("Failed to list torrents in lookup: {}", e);
                    reply.error(EIO);
                }
            }
        } else {
            reply.error(ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        if let Some(attr) = attr_for_ino(ino) {
            reply.attr(&TTL, &attr);
            return;
        }
        if let Some(entry) = self.metadata_entries.get(&ino) {
            reply.attr(&TTL, &file_attr(ino, entry.size));
            return;
        }
        if self.metadata_dirs.contains_key(&ino) {
            reply.attr(&TTL, &dir_attr(ino));
            return;
        }
        
        if self.async_runtime.is_some() {
            match self.list_torrents_safe() {
                Ok(torrents) => {
                    for torrent in &torrents {
                        let torrent_ino = self.torrent_inode(&torrent.source_path, &torrent.name);
                        if torrent_ino == ino {
                            reply.attr(&TTL, &dir_attr(ino));
                            return;
                        }
                        
                        if ino >= 0xB000000000000000 && ino < 0xC000000000000000 {
                            match self.get_torrent_files_safe(&torrent.name) {
                                Ok(files) => {
                                    for file in &files {
                                        let relative_path = file.path.strip_prefix(&torrent.name)
                                            .unwrap_or(&file.path);
                                        let relative_path = relative_path.strip_prefix('/').unwrap_or(relative_path);
                                        
                                        let parts: Vec<&str> = relative_path.split('/').collect();
                                        let mut dir_path = torrent.name.clone();
                                        
                                        for (i, part) in parts.iter().enumerate() {
                                            if i < parts.len() - 1 {
                                                dir_path = format!("{}/{}", dir_path, part);
                                                let dir_ino = self.torrent_file_dir_inode(&torrent.name, &dir_path);
                                                
                                                if dir_ino == ino {
                                                    reply.attr(&TTL, &dir_attr(ino));
                                                    return;
                                                }
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Failed to get torrent files in getattr: {}", e);
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to list torrents in getattr: {}", e);
                }
            }
        }
        
        if ino >= 0xA000000000000000 && ino < 0xB000000000000000 {
            reply.attr(&TTL, &dir_attr(ino));
            return;
        }
        
        if self.async_runtime.is_some() {
            match self.list_torrents_safe() {
                Ok(torrents) => {
                    for torrent in torrents {
                        match self.get_torrent_files_safe(&torrent.name) {
                            Ok(files) => {
                                for file in files {
                                    let file_ino = self.file_inode(&torrent.name, &file.path);
                                    if file_ino == ino {
                                        reply.attr(&TTL, &file_attr(ino, file.size as u64));
                                        return;
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!("Failed to get torrent files in getattr: {}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to list torrents in getattr: {}", e);
                }
            }
        }
        
        reply.error(ENOENT);
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
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
        if let Some(entry) = self.metadata_entries.get_mut(&ino) {
            if let Some(new_size) = size {
                if new_size > usize::MAX as u64 {
                    reply.error(EFBIG);
                    return;
                }
                let new_size_usize = new_size as usize;
                if new_size_usize > MAX_FILE_SIZE {
                    reply.error(EFBIG);
                    return;
                }
                if let Some(open_file) = self.open_files.values_mut().find(|f| f.path == entry.path) {
                    open_file.data.resize(new_size_usize, 0);
                }
                entry.size = new_size;
                reply.attr(&TTL, &file_attr(ino, new_size));
            } else {
                reply.attr(&TTL, &file_attr(ino, entry.size));
            }
            return;
        }
        if let Some(attr) = attr_for_ino(ino) {
            reply.attr(&TTL, &attr);
            return;
        }
        if self.metadata_dirs.contains_key(&ino) {
            reply.attr(&TTL, &dir_attr(ino));
            return;
        }
        
        reply.error(ENOENT);
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if ino == INO_ROOT {
            let entries = [
                (INO_ROOT, FileType::Directory, "."),
                (INO_ROOT, FileType::Directory, ".."),
                (INO_METADATA, FileType::Directory, "metadata"),
                (INO_DATA, FileType::Directory, "data"),
            ];

            for (i, (ino, kind, name)) in entries.into_iter().enumerate() {
                let idx = (i + 1) as i64;
                if idx <= offset {
                    continue;
                }
                if reply.add(ino, idx, kind, name) {
                    break;
                }
            }
            reply.ok();
        } else if ino == INO_METADATA {
            let mut idx = 1i64;
            if idx > offset {
                let _ = reply.add(INO_METADATA, idx, FileType::Directory, ".");
            }
            idx += 1;
            if idx > offset {
                let _ = reply.add(INO_ROOT, idx, FileType::Directory, "..");
            }
            
            let mut added_names: std::collections::HashSet<String> = std::collections::HashSet::new();
            
            for (file_ino, entry) in &self.metadata_entries {
                let name = entry.path.rsplit('/').next().unwrap_or(&entry.path);
                if !entry.path.contains('/') {
                    idx += 1;
                    if idx > offset {
                        if reply.add(*file_ino, idx, FileType::RegularFile, name) {
                            break;
                        }
                    }
                    added_names.insert(name.to_string());
                }
            }
            
            for (dir_ino, dir) in &self.metadata_dirs {
                if dir.parent_ino == INO_METADATA {
                    let name = dir.relative_path.rsplit('/').next().unwrap_or(&dir.relative_path);
                    if !added_names.contains(name) {
                        idx += 1;
                        if idx > offset {
                            if reply.add(*dir_ino, idx, FileType::Directory, name) {
                                break;
                            }
                        }
                        added_names.insert(name.to_string());
                    }
                }
            }
            reply.ok();
        } else if ino == INO_DATA {
            let mut idx = 1i64;
            if idx > offset {
                let _ = reply.add(INO_DATA, idx, FileType::Directory, ".");
            }
            idx += 1;
            if idx > offset {
                let _ = reply.add(INO_ROOT, idx, FileType::Directory, "..");
            }
            
            match self.list_torrents_safe() {
                Ok(torrents) => {
                    let mut added_names: std::collections::HashSet<String> = std::collections::HashSet::new();
                    
                    for torrent in &torrents {
                        if torrent.source_path.is_empty() {
                            idx += 1;
                            if idx > offset {
                                let torrent_ino = self.torrent_inode(&torrent.source_path, &torrent.name);
                                if reply.add(torrent_ino, idx, FileType::Directory, &torrent.name) {
                                    break;
                                }
                            }
                        } else {
                            let first_part = torrent.source_path.split('/').next().unwrap_or(&torrent.source_path);
                            if !added_names.contains(first_part) {
                                idx += 1;
                                if idx > offset {
                                    let dir_ino = self.data_dir_inode(first_part);
                                    if reply.add(dir_ino, idx, FileType::Directory, first_part) {
                                        break;
                                    }
                                }
                                added_names.insert(first_part.to_string());
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to list torrents in readdir: {}", e);
                }
            }
            reply.ok();
        } else if let Some(dir) = self.metadata_dirs.get(&ino).cloned() {
            let prefix = dir.relative_path.clone();
            let parent_ino = dir.parent_ino;
            
            let mut idx = 1i64;
            if idx > offset {
                let _ = reply.add(ino, idx, FileType::Directory, ".");
            }
            idx += 1;
            if idx > offset {
                let _ = reply.add(parent_ino, idx, FileType::Directory, "..");
            }
            
            let mut added_names: std::collections::HashSet<String> = std::collections::HashSet::new();
            
            for (sub_dir_ino, sub_dir) in &self.metadata_dirs {
                if sub_dir.parent_ino == ino {
                    let name = sub_dir.relative_path.rsplit('/').next().unwrap_or(&sub_dir.relative_path);
                    idx += 1;
                    if idx > offset {
                        if reply.add(*sub_dir_ino, idx, FileType::Directory, name) {
                            break;
                        }
                    }
                    added_names.insert(name.to_string());
                }
            }
            
            for (file_ino, entry) in &self.metadata_entries {
                if entry.path.starts_with(&format!("{}/", prefix)) {
                    let rest = &entry.path[prefix.len() + 1..];
                    if !rest.contains('/') {
                        idx += 1;
                        if idx > offset {
                            if reply.add(*file_ino, idx, FileType::RegularFile, rest) {
                                break;
                            }
                        }
                    }
                }
            }
            reply.ok();
        } else if self.async_runtime.is_some() {
            match self.list_torrents_safe() {
                Ok(torrents) => {
                    for torrent in &torrents {
                        let torrent_ino = self.torrent_inode(&torrent.source_path, &torrent.name);
                        if ino == torrent_ino {
                            let mut idx = 1i64;
                            if idx > offset {
                                let _ = reply.add(torrent_ino, idx, FileType::Directory, ".");
                            }
                            idx += 1;
                            if idx > offset {
                                let _ = reply.add(INO_DATA, idx, FileType::Directory, "..");
                            }
                            
                            match self.get_torrent_files_safe(&torrent.name) {
                                Ok(files) => {
                                    let mut added_names: std::collections::HashSet<String> = std::collections::HashSet::new();
                                    
                                    for file in &files {
                                        let relative_path = file.path.strip_prefix(&torrent.name)
                                            .unwrap_or(&file.path);
                                        let relative_path = relative_path.strip_prefix('/').unwrap_or(relative_path);
                                        
                                        if relative_path.is_empty() {
                                            let file_name = file.path.rsplit('/').next().unwrap_or(&file.path);
                                            idx += 1;
                                            if idx > offset {
                                                let file_ino = self.file_inode(&torrent.name, &file.path);
                                                if reply.add(file_ino, idx, FileType::RegularFile, file_name) {
                                                    break;
                                                }
                                            }
                                            continue;
                                        }
                                        
                                        let parts: Vec<&str> = relative_path.split('/').collect();
                                        
                                        if parts.len() == 1 {
                                            idx += 1;
                                            if idx > offset {
                                                let file_ino = self.file_inode(&torrent.name, &file.path);
                                                if reply.add(file_ino, idx, FileType::RegularFile, parts[0]) {
                                                    break;
                                                }
                                            }
                                        } else if parts.len() > 1 {
                                            let dir_name = parts[0];
                                            if !added_names.contains(dir_name) {
                                                added_names.insert(dir_name.to_string());
                                                let dir_path = format!("{}/{}", torrent.name, dir_name);
                                                let dir_ino = self.torrent_file_dir_inode(&torrent.name, &dir_path);
                                                idx += 1;
                                                if idx > offset {
                                                    if reply.add(dir_ino, idx, FileType::Directory, dir_name) {
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Failed to get torrent files in readdir: {}", e);
                                }
                            }
                            reply.ok();
                            return;
                        }
                        
                        if ino >= 0xB000000000000000 && ino < 0xC000000000000000 {
                            match self.get_torrent_files_safe(&torrent.name) {
                                Ok(files) => {
                                    let mut current_dir_path: Option<String> = None;
                                    
                                    for file in &files {
                                        let relative_path = file.path.strip_prefix(&torrent.name)
                                            .unwrap_or(&file.path);
                                        let relative_path = relative_path.strip_prefix('/').unwrap_or(relative_path);
                                        
                                        let parts: Vec<&str> = relative_path.split('/').collect();
                                        let mut dir_path = torrent.name.clone();
                                        
                                        for (i, part) in parts.iter().enumerate() {
                                            if i < parts.len() - 1 {
                                                dir_path = format!("{}/{}", dir_path, part);
                                                let dir_ino = self.torrent_file_dir_inode(&torrent.name, &dir_path);
                                                
                                                if dir_ino == ino {
                                                    current_dir_path = Some(dir_path.clone());
                                                    break;
                                                }
                                            }
                                        }
                                        
                                        if current_dir_path.is_some() {
                                            break;
                                        }
                                    }
                                    
                                    if let Some(current_dir) = current_dir_path {
                                        let mut idx = 1i64;
                                        if idx > offset {
                                            let _ = reply.add(ino, idx, FileType::Directory, ".");
                                        }
                                        idx += 1;
                                        
                                        let parent_path = current_dir.rsplit_once('/')
                                            .map(|(p, _)| p)
                                            .unwrap_or(&torrent.name);
                                        let parent_ino = if parent_path == torrent.name {
                                            self.torrent_inode(&torrent.source_path, &torrent.name)
                                        } else {
                                            self.torrent_file_dir_inode(&torrent.name, parent_path)
                                        };
                                        if idx > offset {
                                            let _ = reply.add(parent_ino, idx, FileType::Directory, "..");
                                        }
                                        
                                        let mut added_names: std::collections::HashSet<String> = std::collections::HashSet::new();
                                        
                                        for file in &files {
                                            let relative_path = file.path.strip_prefix(&torrent.name)
                                                .unwrap_or(&file.path);
                                            let relative_path = relative_path.strip_prefix('/').unwrap_or(relative_path);
                                            
                                            let starts_with_dir = relative_path.starts_with(&current_dir);
                                            let full_relative = format!("{}/{}", torrent.name, relative_path);
                                            let full_starts = full_relative.starts_with(&current_dir);
                                            
                                            if starts_with_dir || full_starts {
                                                let rest = if starts_with_dir {
                                                    relative_path[current_dir.len()..].to_string()
                                                } else {
                                                    full_relative[current_dir.len()..].to_string()
                                                };
                                                
                                                let rest = rest.strip_prefix('/').unwrap_or(&rest);
                                                
                                                if rest.is_empty() {
                                                    continue;
                                                }
                                                
                                                let parts: Vec<&str> = rest.split('/').collect();
                                                
                                                if parts.len() == 1 && !parts[0].is_empty() {
                                                    idx += 1;
                                                    if idx > offset {
                                                        let file_ino = self.file_inode(&torrent.name, &file.path);
                                                        if reply.add(file_ino, idx, FileType::RegularFile, parts[0]) {
                                                            break;
                                                        }
                                                    }
                                                } else if parts.len() > 1 {
                                                    let subdir_name = parts[0];
                                                    if !added_names.contains(subdir_name) {
                                                        added_names.insert(subdir_name.to_string());
                                                        let subdir_path = format!("{}/{}", current_dir, subdir_name);
                                                        let subdir_ino = self.torrent_file_dir_inode(&torrent.name, &subdir_path);
                                                        idx += 1;
                                                        if idx > offset {
                                                            if reply.add(subdir_ino, idx, FileType::Directory, subdir_name) {
                                                                break;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        
                                        reply.ok();
                                        return;
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Failed to get torrent files in readdir: {}", e);
                                }
                            }
                        }
                    }
                    
                    let mut data_dir_path = String::new();
                    for torrent in &torrents {
                        if !torrent.source_path.is_empty() {
                            let parts: Vec<&str> = torrent.source_path.split('/').filter(|p| !p.trim().is_empty()).collect();
                            let mut current_path = String::new();
                            for (i, part) in parts.iter().enumerate() {
                                if i > 0 {
                                    current_path.push('/');
                                }
                                current_path.push_str(part);
                                let dir_ino = self.data_dir_inode(&current_path);
                                if dir_ino == ino {
                                    data_dir_path = current_path.clone();
                                    break;
                                }
                            }
                        }
                    }
                    
                    if !data_dir_path.is_empty() {
                        let mut idx = 1i64;
                        if idx > offset {
                            let _ = reply.add(ino, idx, FileType::Directory, ".");
                        }
                        idx += 1;
                        
                        let parent_path = data_dir_path.rsplit_once('/')
                            .map(|(p, _)| p)
                            .unwrap_or("");
                        let parent_ino = if parent_path.is_empty() {
                            INO_DATA
                        } else {
                            self.data_dir_inode(parent_path)
                        };
                        if idx > offset {
                            let _ = reply.add(parent_ino, idx, FileType::Directory, "..");
                        }
                        
                        let mut added_names: std::collections::HashSet<String> = std::collections::HashSet::new();
                        
                        for torrent in torrents {
                            let is_in_dir = torrent.source_path == data_dir_path 
                                || torrent.source_path.starts_with(&format!("{}/", data_dir_path));
                            if is_in_dir {
                                if torrent.source_path == data_dir_path {
                                    let torrent_ino = self.torrent_inode(&torrent.source_path, &torrent.name);
                                    idx += 1;
                                    if idx > offset {
                                        if reply.add(torrent_ino, idx, FileType::Directory, &torrent.name) {
                                            break;
                                        }
                                    }
                                } else {
                                    let rest = &torrent.source_path[data_dir_path.len() + 1..];
                                    if !rest.is_empty() {
                                        let parts: Vec<&str> = rest.split('/').filter(|p| !p.trim().is_empty()).collect();
                                        if !parts.is_empty() {
                                            let first_part = parts[0];
                                            if !added_names.contains(first_part) {
                                                let new_path = format!("{}/{}", data_dir_path, first_part);
                                                let dir_ino = self.data_dir_inode(&new_path);
                                                idx += 1;
                                                if idx > offset {
                                                    if reply.add(dir_ino, idx, FileType::Directory, first_part) {
                                                        break;
                                                    }
                                                }
                                                added_names.insert(first_part.to_string());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        reply.ok();
                        return;
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to list torrents in readdir: {}", e);
                }
            }
            reply.error(ENOTDIR);
        } else {
            reply.error(ENOTDIR);
        }
    }

    fn opendir(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) {
        reply.opened(0, 0);
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        if parent != INO_METADATA && !self.metadata_dirs.contains_key(&parent) {
            reply.error(ENOENT);
            return;
        }

        let name_str = name.to_string_lossy();
        
        let parent_path = if parent == INO_METADATA {
            String::new()
        } else {
            self.metadata_dirs.get(&parent).map(|d| d.relative_path.clone()).unwrap_or_default()
        };
        
        let new_path = if parent_path.is_empty() {
            name_str.to_string()
        } else {
            format!("{}/{}", parent_path, name_str)
        };
        
        for (_ino, entry) in &self.metadata_entries {
            if entry.path == new_path || entry.path.starts_with(&format!("{}/", new_path)) {
                reply.error(EEXIST);
                return;
            }
        }
        
        for (_ino, dir) in &self.metadata_dirs {
            if dir.relative_path == new_path {
                reply.error(EEXIST);
                return;
            }
        }

        let ino = self.allocate_ino();
        
        self.metadata_dirs.insert(ino, MetadataDir {
            ino,
            parent_ino: parent,
            relative_path: new_path,
        });

        reply.entry(&TTL, &dir_attr(ino), 0);
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        if parent != INO_METADATA && !self.metadata_dirs.contains_key(&parent) {
            reply.error(ENOENT);
            return;
        }

        let name_str = name.to_string_lossy();
        if !name_str.ends_with(".torrent") {
            reply.error(EINVAL);
            return;
        }

        let parent_path = if parent == INO_METADATA {
            String::new()
        } else {
            self.metadata_dirs.get(&parent).map(|d| d.relative_path.clone()).unwrap_or_default()
        };
        
        let file_path = if parent_path.is_empty() {
            name_str.to_string()
        } else {
            format!("{}/{}", parent_path, name_str)
        };

        for (_ino, entry) in &self.metadata_entries {
            if entry.path == file_path {
                reply.error(EEXIST);
                return;
            }
        }

        let ino = self.allocate_ino();
        let fh = self.allocate_fh();

        self.metadata_entries.insert(ino, MetadataEntry {
            path: file_path.clone(),
            size: 0,
        });
        self.open_files.insert(fh, OpenFile {
            path: file_path,
            data: Vec::new(),
        });

        let attr = file_attr(ino, 0);
        reply.created(&TTL, &attr, 0, fh, 0);
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let open_file = match self.open_files.get_mut(&fh) {
            Some(f) => f,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        if offset < 0 {
            reply.error(EINVAL);
            return;
        }
        let offset = offset as usize;
        let write_end = match offset.checked_add(data.len()) {
            Some(end) => end,
            None => {
                reply.error(EFBIG);
                return;
            }
        };
        if write_end > MAX_FILE_SIZE {
            reply.error(EFBIG);
            return;
        }

        let current_data_len = open_file.data.len();
        let additional_space = if write_end > current_data_len {
            write_end - current_data_len
        } else {
            0
        };
        
        if additional_space > 0 {
            let memory_limit = 100 * 1024 * 1024;
            let current_memory: usize = self.open_files.values().map(|f| f.data.len()).sum();
            let estimated_total = current_memory + additional_space;
            
            if estimated_total > memory_limit {
                tracing::warn!(
                    "Memory limit reached: current={} bytes, requested={} bytes, limit={} bytes",
                    current_memory, additional_space, memory_limit
                );
                reply.error(libc::ENOSPC);
                return;
            }
        }

        let open_file = self.open_files.get_mut(&fh).unwrap();
        if offset > open_file.data.len() {
            open_file.data.resize(offset, 0);
        }

        if offset + data.len() > open_file.data.len() {
            open_file.data.resize(offset + data.len(), 0);
        }

        open_file.data[offset..offset + data.len()].copy_from_slice(data);

        let new_size = open_file.data.len() as u64;
        if let Some(entry) = self.metadata_entries.get_mut(&ino) {
            entry.size = new_size;
        }

        reply.written(data.len() as u32);
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        if self.open_torrent_files.remove(&fh).is_some() {
            reply.ok();
            return;
        }
        
        let open_file = match self.open_files.remove(&fh) {
            Some(f) => f,
            None => {
                reply.ok();
                return;
            }
        };

        let path = open_file.path.clone();
        let data = open_file.data.clone();
        
        let name = path.rsplit('/').next().unwrap_or(&path).to_string();
        let source_path = if path.contains('/') {
            let parts: Vec<&str> = path.rsplitn(2, '/').collect();
            if parts.len() > 1 {
                parts[1].to_string()
            } else {
                String::new()
            }
        } else {
            String::new()
        };

        if self.async_runtime.is_some() {
            let save_path = dirs::home_dir()
                .map(|h| h.join(".local").join("share").join("torrentfs").join("data"))
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_else(|| "/tmp/torrentfs".to_string());

            match self.process_torrent_data_safe(&data, &source_path) {
                Ok(parsed) => {
                    match self.persist_to_db_safe(&parsed) {
                        Ok(PersistResult::Inserted) => {
                            match self.add_torrent_paused_safe(&data, &save_path) {
                                Ok(()) => {
                                    tracing::info!("Added torrent '{}' to libtorrent session (paused)", name);
                                }
                                Err(e) => {
                                    tracing::error!("Failed to add torrent to session: {}", e);
                                }
                            }
                            tracing::info!(
                                "Processed torrent '{}' ({} files, {} bytes) - kept in metadata/{}",
                                name, parsed.file_count, parsed.total_size, 
                                if source_path.is_empty() { "".to_string() } else { format!("/{}/", source_path) }
                            );
                        }
                        Ok(PersistResult::AlreadyExists) => {
                            tracing::info!("Torrent '{}' already exists in database, skipping (idempotent)", name);
                        }
                        Err(e) => {
                            tracing::error!("Failed to persist torrent to DB: {}", e);
                        }
                    }
                }
                Err(FuseError::TorrentParseError(e)) => {
                    tracing::error!("Invalid torrent file '{}': {}", name, e);
                    self.metadata_entries.remove(&ino);
                    reply.error(EINVAL);
                    return;
                }
                Err(e) => {
                    tracing::error!("Failed to process torrent data: {}", e);
                    self.metadata_entries.remove(&ino);
                    reply.error(EIO);
                    return;
                }
            }
        } else {
            let incoming_dir = self.state_dir.join("incoming");
            if let Err(e) = fs::create_dir_all(&incoming_dir) {
                tracing::error!("Failed to create incoming directory: {}", e);
                reply.error(libc::EIO);
                return;
            }

            let dest_path = incoming_dir.join(&name);
            if let Err(e) = fs::write(&dest_path, &data) {
                tracing::error!("Failed to write torrent file: {}", e);
                reply.error(libc::EIO);
                return;
            }

            tracing::info!("Persisted {} ({} bytes) to {}", name, data.len(), dest_path.display());
        }

        reply.ok();
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        if ino >= 0x9000000000000000 && ino < 0xA000000000000000 {
            if let Some((torrent_name, file_path)) = self.find_torrent_file_by_ino(ino) {
                match self.get_file_info_for_read(&torrent_name, &file_path) {
                    Ok(file_info) => {
                        let fh = self.allocate_fh();
                        self.open_torrent_files.insert(fh, OpenTorrentFile {
                            torrent_name: file_info.torrent_name,
                            file_path: file_info.file_path,
                            info_hash: file_info.info_hash,
                            file_size: file_info.file_size,
                            piece_size: file_info.piece_size,
                            first_piece: file_info.first_piece,
                            last_piece: file_info.last_piece,
                            file_offset: file_info.file_offset,
                        });
                        reply.opened(fh, 0);
                    }
                    Err(e) => {
                        tracing::error!("Failed to get file info for open: {}", e);
                        reply.error(fuse_error_to_errno(&e));
                    }
                }
            } else {
                reply.error(ENOENT);
            }
        } else if self.metadata_entries.contains_key(&ino) {
            let fh = self.allocate_fh();
            reply.opened(fh, 0);
        } else {
            reply.error(ENOENT);
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        if offset < 0 {
            reply.error(EINVAL);
            return;
        }
        
        if let Some(open_torrent_file) = self.open_torrent_files.get(&fh) {
            let file_size = open_torrent_file.file_size;
            let file_offset = open_torrent_file.file_offset;
            let file_rel_offset = offset as u64;
            
            if file_rel_offset >= file_size as u64 {
                reply.data(&[]);
                return;
            }
            
            let end_file_offset = std::cmp::min(file_rel_offset + size as u64, file_size as u64);
            let bytes_to_read = (end_file_offset - file_rel_offset) as usize;
            
            let piece_size = open_torrent_file.piece_size as u64;
            let last_piece = open_torrent_file.last_piece as u32;
            
            let torrent_start_offset = file_offset + file_rel_offset;
            let torrent_end_offset = file_offset + end_file_offset;
            
            let start_piece_idx = (torrent_start_offset / piece_size) as u32;
            let end_piece_idx = std::cmp::min(
                ((torrent_end_offset - 1) / piece_size) as u32,
                last_piece
            );
            
            let mut result = Vec::with_capacity(bytes_to_read);
            let mut current_torrent_offset = torrent_start_offset;
            
            for piece_idx in start_piece_idx..=end_piece_idx {
                match self.read_piece_from_torrent(&open_torrent_file.info_hash, piece_idx) {
                    Ok(piece_data) => {
                        let piece_start = (current_torrent_offset % piece_size) as usize;
                        let remaining_bytes = (torrent_end_offset - current_torrent_offset) as usize;
                        let piece_remaining = piece_data.len().saturating_sub(piece_start);
                        let bytes_from_piece = std::cmp::min(remaining_bytes, piece_remaining);
                        
                        if bytes_from_piece > 0 {
                            result.extend_from_slice(&piece_data[piece_start..piece_start + bytes_from_piece]);
                            current_torrent_offset += bytes_from_piece as u64;
                        }
                        
                        if result.len() >= bytes_to_read {
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            info_hash = %open_torrent_file.info_hash,
                            piece_index = piece_idx,
                            error = %e,
                            "Failed to read piece"
                        );
                        reply.error(fuse_error_to_errno(&e));
                        return;
                    }
                }
            }
            
            reply.data(&result);
        } else if let Some(_metadata_entry) = self.metadata_entries.get(&ino) {
            if let Some(open_file) = self.open_files.get(&fh) {
                let offset = offset as usize;
                let end = std::cmp::min(offset + size as usize, open_file.data.len());
                if offset >= open_file.data.len() {
                    reply.data(&[]);
                } else {
                    reply.data(&open_file.data[offset..end]);
                }
            } else {
                reply.error(ENOENT);
            }
        } else {
            reply.error(ENOENT);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_getattr_root() {
        let attr = attr_for_ino(INO_ROOT).unwrap();
        assert_eq!(attr.ino, INO_ROOT);
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o755);
    }

    #[test]
    fn test_getattr_metadata() {
        let attr = attr_for_ino(INO_METADATA).unwrap();
        assert_eq!(attr.ino, INO_METADATA);
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o755);
    }

    #[test]
    fn test_getattr_data() {
        let attr = attr_for_ino(INO_DATA).unwrap();
        assert_eq!(attr.ino, INO_DATA);
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o755);
    }

    #[test]
    fn test_getattr_unknown_returns_none() {
        assert!(attr_for_ino(999).is_none());
    }

    #[test]
    fn test_file_attr() {
        let attr = file_attr(100, 1024);
        assert_eq!(attr.ino, 100);
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.size, 1024);
        assert_eq!(attr.perm, 0o644);
    }

    #[test]
    fn test_new_filesystem() {
        let fs = TorrentFsFilesystem::new(PathBuf::from("/tmp/test"));
        assert_eq!(fs.next_ino, INO_DYNAMIC_START);
        assert_eq!(fs.next_fh, 1);
        assert!(fs.open_files.is_empty());
        assert!(fs.metadata_entries.is_empty());
        assert!(fs.metadata_dirs.is_empty());
    }

    #[test]
    fn test_allocate_ino() {
        let mut fs = TorrentFsFilesystem::new(PathBuf::from("/tmp/test"));
        let ino1 = fs.allocate_ino();
        let ino2 = fs.allocate_ino();
        assert_eq!(ino1, INO_DYNAMIC_START);
        assert_eq!(ino2, INO_DYNAMIC_START + 1);
    }

    #[test]
    fn test_allocate_fh() {
        let mut fs = TorrentFsFilesystem::new(PathBuf::from("/tmp/test"));
        let fh1 = fs.allocate_fh();
        let fh2 = fs.allocate_fh();
        assert_eq!(fh1, 1);
        assert_eq!(fh2, 2);
    }

    #[test]
    fn test_metadata_entry_size_tracking() {
        let state_dir = tempfile::tempdir().unwrap();
        let mut fs = TorrentFsFilesystem::new(state_dir.path().to_path_buf());

        let ino = fs.allocate_ino();
        let fh = fs.allocate_fh();

        fs.metadata_entries.insert(ino, MetadataEntry {
            path: "test.torrent".to_string(),
            size: 0,
        });
        fs.open_files.insert(fh, OpenFile {
            path: "test.torrent".to_string(),
            data: vec![0u8; 42],
        });
        fs.metadata_entries.get_mut(&ino).unwrap().size = 42;

        assert_eq!(fs.metadata_entries.get(&ino).unwrap().size, 42);
    }

    #[test]
    fn test_release_keeps_metadata_entries() {
        let state_dir = tempfile::tempdir().unwrap();
        let mut fs = TorrentFsFilesystem::new(state_dir.path().to_path_buf());

        let ino = fs.allocate_ino();
        let fh = fs.allocate_fh();

        fs.metadata_entries.insert(ino, MetadataEntry {
            path: "keep.torrent".to_string(),
            size: 5,
        });
        fs.open_files.insert(fh, OpenFile {
            path: "keep.torrent".to_string(),
            data: vec![1u8, 2, 3, 4, 5],
        });

        assert!(fs.metadata_entries.contains_key(&ino));

        let open_file = fs.open_files.remove(&fh).unwrap();

        let incoming_dir = state_dir.path().join("incoming");
        std::fs::create_dir_all(&incoming_dir).unwrap();
        std::fs::write(incoming_dir.join("keep.torrent"), &open_file.data).unwrap();

        assert!(fs.metadata_entries.contains_key(&ino), "metadata_entries should be kept after release");
        assert!(!fs.open_files.contains_key(&fh), "open_files should be cleaned up after release");
    }

    #[test]
    fn test_create_duplicate_torrent_rejected() {
        let state_dir = tempfile::tempdir().unwrap();
        let mut fs = TorrentFsFilesystem::new(state_dir.path().to_path_buf());

        let ino = fs.allocate_ino();
        let fh = fs.allocate_fh();
        fs.metadata_entries.insert(ino, MetadataEntry {
            path: "dup.torrent".to_string(),
            size: 0,
        });
        fs.open_files.insert(fh, OpenFile {
            path: "dup.torrent".to_string(),
            data: Vec::new(),
        });

        for entry in fs.metadata_entries.values() {
            if entry.path == "dup.torrent" {
                return;
            }
        }
        panic!("Duplicate name should have been detected");
    }

    #[test]
    fn test_async_runtime_field() {
        let state_dir = PathBuf::from("/tmp/test");
        let fs = TorrentFsFilesystem::new(state_dir);
        assert!(fs.async_runtime.is_none());
        assert_eq!(fs.next_ino, INO_DYNAMIC_START);
    }

    #[test]
    fn test_metadata_dir_tracking() {
        let state_dir = tempfile::tempdir().unwrap();
        let mut fs = TorrentFsFilesystem::new(state_dir.path().to_path_buf());
        
        let dir_ino = fs.allocate_ino();
        fs.metadata_dirs.insert(dir_ino, MetadataDir {
            ino: dir_ino,
            parent_ino: INO_METADATA,
            relative_path: "subdir".to_string(),
        });
        
        assert!(fs.metadata_dirs.contains_key(&dir_ino));
        assert_eq!(fs.metadata_dirs.get(&dir_ino).unwrap().relative_path, "subdir");
    }

    #[test]
    fn test_torrent_file_dir_inode_deterministic() {
        let fs = TorrentFsFilesystem::new(PathBuf::from("/tmp/test"));
        
        let ino1 = fs.torrent_file_dir_inode("test_torrent", "test_torrent/docs");
        let ino2 = fs.torrent_file_dir_inode("test_torrent", "test_torrent/docs");
        
        assert_eq!(ino1, ino2, "Same input should produce same inode");
    }

    #[test]
    fn test_torrent_file_dir_inode_different_torrents() {
        let fs = TorrentFsFilesystem::new(PathBuf::from("/tmp/test"));
        
        let ino1 = fs.torrent_file_dir_inode("torrent1", "torrent1/docs");
        let ino2 = fs.torrent_file_dir_inode("torrent2", "torrent2/docs");
        
        assert_ne!(ino1, ino2, "Different torrents should produce different inodes");
    }

    #[test]
    fn test_torrent_file_dir_inode_different_paths() {
        let fs = TorrentFsFilesystem::new(PathBuf::from("/tmp/test"));
        
        let ino1 = fs.torrent_file_dir_inode("test_torrent", "test_torrent/docs");
        let ino2 = fs.torrent_file_dir_inode("test_torrent", "test_torrent/images");
        
        assert_ne!(ino1, ino2, "Different paths should produce different inodes");
    }

    #[test]
    fn test_torrent_file_dir_inode_in_range() {
        let fs = TorrentFsFilesystem::new(PathBuf::from("/tmp/test"));
        
        let ino = fs.torrent_file_dir_inode("test_torrent", "test_torrent/docs");
        
        assert!(ino >= 0xB000000000000000, "Inode should be in torrent file dir range");
        assert!(ino < 0xC000000000000000, "Inode should be in torrent file dir range");
    }

    #[test]
    fn test_torrent_file_dir_inode_nested() {
        let fs = TorrentFsFilesystem::new(PathBuf::from("/tmp/test"));
        
        let ino1 = fs.torrent_file_dir_inode("test_torrent", "test_torrent/docs");
        let ino2 = fs.torrent_file_dir_inode("test_torrent", "test_torrent/docs/images");
        
        assert_ne!(ino1, ino2, "Nested directories should have different inodes");
    }

    #[test]
    fn test_single_file_torrent_relative_path_handling() {
        let torrent_name = "video.mkv";
        let file_path = "video.mkv";
        
        let relative_path = file_path.strip_prefix(torrent_name)
            .unwrap_or(file_path);
        let relative_path = relative_path.strip_prefix('/').unwrap_or(relative_path);
        
        assert!(relative_path.is_empty(), "Single-file torrent should have empty relative path");
        
        let file_name = file_path.rsplit('/').next().unwrap_or(file_path);
        assert_eq!(file_name, "video.mkv", "Should use basename for single-file torrent");
    }

    #[test]
    fn test_multi_file_torrent_relative_path_handling() {
        let torrent_name = "mytorrent";
        let file_path = "mytorrent/video.mkv";
        
        let relative_path = file_path.strip_prefix(torrent_name)
            .unwrap_or(file_path);
        let relative_path = relative_path.strip_prefix('/').unwrap_or(relative_path);
        
        assert_eq!(relative_path, "video.mkv", "Multi-file torrent should have non-empty relative path");
        
        let parts: Vec<&str> = relative_path.split('/').collect();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0], "video.mkv");
    }

    #[test]
    fn test_inode_ranges_are_disjoint() {
        let fs = TorrentFsFilesystem::new(PathBuf::from("/tmp/test"));
        
        let torrent_ino = fs.torrent_inode("series", "test_torrent");
        let file_ino = fs.file_inode("test_torrent", "test_torrent/video.mp4");
        let data_dir_ino = fs.data_dir_inode("series");
        let torrent_file_dir_ino = fs.torrent_file_dir_inode("test_torrent", "test_torrent/docs");
        
        assert!(torrent_ino >= 0x8000000000000000 && torrent_ino < 0x9000000000000000,
            "torrent_inode should be in range 0x8...");
        assert!(file_ino >= 0x9000000000000000 && file_ino < 0xA000000000000000,
            "file_inode should be in range 0x9...");
        assert!(data_dir_ino >= 0xA000000000000000 && data_dir_ino < 0xB000000000000000,
            "data_dir_inode should be in range 0xA...");
        assert!(torrent_file_dir_ino >= 0xB000000000000000 && torrent_file_dir_ino < 0xC000000000000000,
            "torrent_file_dir_inode should be in range 0xB...");
    }

    #[test]
    fn test_data_dir_inode_does_not_collide_with_torrent_file_dir_inode() {
        let fs = TorrentFsFilesystem::new(PathBuf::from("/tmp/test"));
        
        let data_dir_ino = fs.data_dir_inode("series");
        
        assert!(!(data_dir_ino >= 0xB000000000000000 && data_dir_ino < 0xC000000000000000),
            "data_dir_inode should NOT fall into torrent_file_dir range");
    }

    #[test]
    fn test_find_metadata_dir_by_parent_and_name_top_level() {
        let state_dir = tempfile::tempdir().unwrap();
        let mut fs = TorrentFsFilesystem::new(state_dir.path().to_path_buf());
        
        let dir_ino = fs.allocate_ino();
        fs.metadata_dirs.insert(dir_ino, MetadataDir {
            ino: dir_ino,
            parent_ino: INO_METADATA,
            relative_path: "anime".to_string(),
        });
        
        let result = fs.find_metadata_dir_by_parent_and_name(INO_METADATA, "anime");
        assert!(result.is_some(), "Should find top-level directory under metadata/");
        let (found_ino, dir) = result.unwrap();
        assert_eq!(found_ino, dir_ino);
        assert_eq!(dir.relative_path, "anime");
    }

    #[test]
    fn test_find_metadata_dir_by_parent_and_name_nested() {
        let state_dir = tempfile::tempdir().unwrap();
        let mut fs = TorrentFsFilesystem::new(state_dir.path().to_path_buf());
        
        let parent_ino = fs.allocate_ino();
        fs.metadata_dirs.insert(parent_ino, MetadataDir {
            ino: parent_ino,
            parent_ino: INO_METADATA,
            relative_path: "media".to_string(),
        });
        
        let child_ino = fs.allocate_ino();
        fs.metadata_dirs.insert(child_ino, MetadataDir {
            ino: child_ino,
            parent_ino: parent_ino,
            relative_path: "media/video".to_string(),
        });
        
        let result = fs.find_metadata_dir_by_parent_and_name(parent_ino, "video");
        assert!(result.is_some(), "Should find nested directory");
        let (found_ino, dir) = result.unwrap();
        assert_eq!(found_ino, child_ino);
        assert_eq!(dir.relative_path, "media/video");
    }

    #[test]
    fn test_find_metadata_dir_by_parent_and_name_unicode() {
        let state_dir = tempfile::tempdir().unwrap();
        let mut fs = TorrentFsFilesystem::new(state_dir.path().to_path_buf());
        
        let dir_ino = fs.allocate_ino();
        fs.metadata_dirs.insert(dir_ino, MetadataDir {
            ino: dir_ino,
            parent_ino: INO_METADATA,
            relative_path: "中文测试".to_string(),
        });
        
        let result = fs.find_metadata_dir_by_parent_and_name(INO_METADATA, "中文测试");
        assert!(result.is_some(), "Should find directory with unicode name");
        let (found_ino, _dir) = result.unwrap();
        assert_eq!(found_ino, dir_ino);
    }

    #[test]
    fn test_find_metadata_dir_by_parent_and_name_not_found() {
        let state_dir = tempfile::tempdir().unwrap();
        let fs = TorrentFsFilesystem::new(state_dir.path().to_path_buf());
        
        let result = fs.find_metadata_dir_by_parent_and_name(INO_METADATA, "nonexistent");
        assert!(result.is_none(), "Should return None for nonexistent directory");
    }

    #[test]
    fn test_ensure_metadata_dirs_for_path_single() {
        let state_dir = tempfile::tempdir().unwrap();
        let mut fs = TorrentFsFilesystem::new(state_dir.path().to_path_buf());
        
        fs.ensure_metadata_dirs_for_path("anime");
        
        assert_eq!(fs.metadata_dirs.len(), 1, "Should create one directory");
        
        let result = fs.find_metadata_dir_by_parent_and_name(INO_METADATA, "anime");
        assert!(result.is_some(), "Should find 'anime' under metadata/");
        let (_, dir) = result.unwrap();
        assert_eq!(dir.relative_path, "anime");
    }

    #[test]
    fn test_ensure_metadata_dirs_for_path_nested() {
        let state_dir = tempfile::tempdir().unwrap();
        let mut fs = TorrentFsFilesystem::new(state_dir.path().to_path_buf());
        
        fs.ensure_metadata_dirs_for_path("anime/series/2024");
        
        assert_eq!(fs.metadata_dirs.len(), 3, "Should create three nested directories");
        
        let result1 = fs.find_metadata_dir_by_parent_and_name(INO_METADATA, "anime");
        assert!(result1.is_some(), "Should find 'anime'");
        let (anime_ino, _) = result1.unwrap();
        
        let result2 = fs.find_metadata_dir_by_parent_and_name(anime_ino, "series");
        assert!(result2.is_some(), "Should find 'series' under 'anime'");
        let (series_ino, _) = result2.unwrap();
        
        let result3 = fs.find_metadata_dir_by_parent_and_name(series_ino, "2024");
        assert!(result3.is_some(), "Should find '2024' under 'series'");
    }

    #[test]
    fn test_ensure_metadata_dirs_for_path_idempotent() {
        let state_dir = tempfile::tempdir().unwrap();
        let mut fs = TorrentFsFilesystem::new(state_dir.path().to_path_buf());
        
        fs.ensure_metadata_dirs_for_path("media/video");
        let ino_after_first = fs.metadata_dirs.len();
        
        fs.ensure_metadata_dirs_for_path("media/video");
        let ino_after_second = fs.metadata_dirs.len();
        
        assert_eq!(ino_after_first, ino_after_second, "Should not create duplicate directories");
    }

    #[test]
    fn test_ensure_metadata_dirs_for_path_multiple_sharing_prefix() {
        let state_dir = tempfile::tempdir().unwrap();
        let mut fs = TorrentFsFilesystem::new(state_dir.path().to_path_buf());
        
        fs.ensure_metadata_dirs_for_path("media/video");
        fs.ensure_metadata_dirs_for_path("media/audio");
        
        assert_eq!(fs.metadata_dirs.len(), 3, "Should create 'media', 'media/video', 'media/audio'");
        
        let result = fs.find_metadata_dir_by_parent_and_name(INO_METADATA, "media");
        assert!(result.is_some(), "Should find shared parent 'media'");
        let (media_ino, _) = result.unwrap();
        
        let video = fs.find_metadata_dir_by_parent_and_name(media_ino, "video");
        assert!(video.is_some(), "Should find 'video'");
        
        let audio = fs.find_metadata_dir_by_parent_and_name(media_ino, "audio");
        assert!(audio.is_some(), "Should find 'audio'");
    }

    #[test]
    fn test_ensure_metadata_dirs_for_path_empty() {
        let state_dir = tempfile::tempdir().unwrap();
        let mut fs = TorrentFsFilesystem::new(state_dir.path().to_path_buf());
        
        fs.ensure_metadata_dirs_for_path("");
        
        assert!(fs.metadata_dirs.is_empty(), "Should not create directories for empty path");
    }

    #[test]
    fn test_ensure_metadata_dirs_for_path_whitespace_only() {
        let state_dir = tempfile::tempdir().unwrap();
        let mut fs = TorrentFsFilesystem::new(state_dir.path().to_path_buf());
        
        fs.ensure_metadata_dirs_for_path("   ");
        fs.ensure_metadata_dirs_for_path("  /  ");
        
        let parts: Vec<&str> = "   ".split('/').filter(|p| !p.is_empty()).collect();
        assert!(parts.iter().all(|p| p.trim().is_empty()), "Whitespace-only parts should be filtered");
    }
}
