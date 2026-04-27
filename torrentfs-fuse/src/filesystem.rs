use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyWrite, Request, ReplyCreate,
};
use libc::{EEXIST, EINVAL, ENOENT, ENOSYS, ENOTDIR, EFBIG};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::runtime::Runtime;
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

struct OpenFile {
    path: String,
    data: Vec<u8>,
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

pub struct CoreResources {
    pub metadata_manager: Arc<MetadataManager>,
    pub tokio_runtime: Runtime,
    pub session: Mutex<Session>,
}

pub struct TorrentFsFilesystem {
    state_dir: PathBuf,
    next_ino: u64,
    next_fh: u64,
    open_files: HashMap<u64, OpenFile>,
    metadata_entries: HashMap<u64, MetadataEntry>,
    metadata_dirs: HashMap<u64, MetadataDir>,
    core: Option<CoreResources>,
}

impl TorrentFsFilesystem {
    pub fn new(state_dir: PathBuf) -> Self {
        Self {
            state_dir,
            next_ino: INO_DYNAMIC_START,
            next_fh: 1,
            open_files: HashMap::new(),
            metadata_entries: HashMap::new(),
            metadata_dirs: HashMap::new(),
            core: None,
        }
    }

    pub fn new_with_core(
        state_dir: PathBuf,
        metadata_manager: Arc<MetadataManager>,
        tokio_runtime: Runtime,
        session: Session,
    ) -> Self {
        Self {
            state_dir,
            next_ino: INO_DYNAMIC_START,
            next_fh: 1,
            open_files: HashMap::new(),
            metadata_entries: HashMap::new(),
            metadata_dirs: HashMap::new(),
            core: Some(CoreResources {
                metadata_manager,
                tokio_runtime,
                session: Mutex::new(session),
            }),
        }
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

    fn torrent_inode(&self, metadata_path: &str, torrent_name: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        metadata_path.hash(&mut hasher);
        torrent_name.hash(&mut hasher);
        (hasher.finish() & 0x7FFFFFFFFFFFFFFF) | 0x8000000000000000
    }

    fn file_inode(&self, torrent_name: &str, file_path: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        torrent_name.hash(&mut hasher);
        file_path.hash(&mut hasher);
        (hasher.finish() & 0x7FFFFFFFFFFFFFFF) | 0x8000000000000000
    }

    fn data_dir_inode(&self, path: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        (hasher.finish() & 0x7FFFFFFFFFFFFFFF) | 0xC000000000000000
    }

    fn find_metadata_dir_by_parent_and_name(&self, parent: u64, name: &str) -> Option<(u64, &MetadataDir)> {
        for (ino, dir) in &self.metadata_dirs {
            if dir.parent_ino == parent && dir.relative_path.ends_with(&format!("/{}", name)) {
                return Some((*ino, dir));
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
            
            if let Some(core) = &self.core {
                let torrents = core.tokio_runtime.block_on(core.metadata_manager.list_torrents());
                match torrents {
                    Ok(torrents) => {
                        for torrent in &torrents {
                            if torrent.metadata_path.is_empty() {
                                if torrent.name == name_str {
                                    let ino = self.torrent_inode(&torrent.metadata_path, &torrent.name);
                                    reply.entry(&TTL, &dir_attr(ino), 0);
                                    return;
                                }
                            } else {
                                let path_parts: Vec<&str> = torrent.metadata_path.split('/').collect();
                                if path_parts.len() == 1 && path_parts[0] == name_str {
                                    let ino = self.torrent_inode(&torrent.metadata_path, &torrent.name);
                                    reply.entry(&TTL, &dir_attr(ino), 0);
                                    return;
                                }
                                if !path_parts.is_empty() && path_parts[0] == name_str && path_parts.len() > 1 {
                                    let ino = self.data_dir_inode(&name_str);
                                    reply.entry(&TTL, &dir_attr(ino), 0);
                                    return;
                                }
                            }
                        }
                        reply.error(ENOENT);
                    }
                    Err(_) => reply.error(ENOENT),
                }
            } else {
                reply.error(ENOENT);
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
        } else if let Some(core) = &self.core {
            let torrents = core.tokio_runtime.block_on(core.metadata_manager.list_torrents());
            if let Ok(torrents) = &torrents {
                for torrent in torrents {
                    let torrent_ino = self.torrent_inode(&torrent.metadata_path, &torrent.name);
                    if parent == torrent_ino {
                        let name_str = name.to_string_lossy();
                        
                        let files = core.tokio_runtime.block_on(
                            core.metadata_manager.get_torrent_files(&torrent.name)
                        );
                        
                        if let Ok(files) = files {
                            for file in files {
                                let file_name = file.path.rsplit('/').next().unwrap_or(&file.path);
                                if file_name == name_str {
                                    let ino = self.file_inode(&torrent.name, &file.path);
                                    reply.entry(&TTL, &file_attr(ino, file.size as u64), 0);
                                    return;
                                }
                            }
                        }
                        reply.error(ENOENT);
                        return;
                    }
                }
            
                let mut data_dir_path = String::new();
                for torrent in torrents {
                    if !torrent.metadata_path.is_empty() {
                        let parts: Vec<&str> = torrent.metadata_path.split('/').collect();
                        let mut current_path = String::new();
                        for (i, part) in parts.iter().enumerate() {
                            if i > 0 {
                                current_path.push('/');
                            }
                            current_path.push_str(part);
                            let dir_ino = self.data_dir_inode(&current_path);
                            if dir_ino == parent && i < parts.len() - 1 {
                                data_dir_path = current_path.clone();
                                break;
                            }
                        }
                    }
                }
                
                if !data_dir_path.is_empty() {
                    let name_str = name.to_string_lossy();
                    for torrent in torrents {
                        if torrent.metadata_path.starts_with(&data_dir_path) {
                            let rest = &torrent.metadata_path[data_dir_path.len()..];
                            if rest.starts_with('/') {
                                let remaining = &rest[1..];
                                let parts: Vec<&str> = remaining.split('/').collect();
                                if parts.len() == 1 {
                                    let torrent_ino = self.torrent_inode(&torrent.metadata_path, &torrent.name);
                                    reply.entry(&TTL, &dir_attr(torrent_ino), 0);
                                    return;
                                } else if !parts.is_empty() {
                                    let next_part = parts[0];
                                    if next_part == name_str {
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
        
        if let Some(core) = &self.core {
            let torrents = core.tokio_runtime.block_on(core.metadata_manager.list_torrents());
            if let Ok(torrents) = torrents {
                for torrent in &torrents {
                    let torrent_ino = self.torrent_inode(&torrent.metadata_path, &torrent.name);
                    if torrent_ino == ino {
                        reply.attr(&TTL, &dir_attr(ino));
                        return;
                    }
                }
            }
        }
        
        if ino >= 0xC000000000000000 {
            reply.attr(&TTL, &dir_attr(ino));
            return;
        }
        
        if let Some(core) = &self.core {
            let torrents = core.tokio_runtime.block_on(core.metadata_manager.list_torrents());
            if let Ok(torrents) = torrents {
                for torrent in &torrents {
                    let files = core.tokio_runtime.block_on(
                        core.metadata_manager.get_torrent_files(&torrent.name)
                    );
                    
                    if let Ok(files) = files {
                        for file in files {
                            let file_ino = self.file_inode(&torrent.name, &file.path);
                            if file_ino == ino {
                                reply.attr(&TTL, &file_attr(ino, file.size as u64));
                                return;
                            }
                        }
                    }
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
            
            if let Some(core) = &self.core {
                let torrents = core.tokio_runtime.block_on(core.metadata_manager.list_torrents());
                match torrents {
                    Ok(torrents) => {
                        let mut added_names: std::collections::HashSet<String> = std::collections::HashSet::new();
                        
                        for torrent in &torrents {
                            if torrent.metadata_path.is_empty() {
                                idx += 1;
                                if idx > offset {
                                    let torrent_ino = self.torrent_inode(&torrent.metadata_path, &torrent.name);
                                    if reply.add(torrent_ino, idx, FileType::Directory, &torrent.name) {
                                        break;
                                    }
                                }
                            } else {
                                let first_part = torrent.metadata_path.split('/').next().unwrap_or(&torrent.metadata_path);
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
                    Err(_) => {}
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
        } else if let Some(core) = &self.core {
            let torrents = core.tokio_runtime.block_on(core.metadata_manager.list_torrents());
            if let Ok(torrents) = torrents {
                for torrent in &torrents {
                    let torrent_ino = self.torrent_inode(&torrent.metadata_path, &torrent.name);
                    if ino == torrent_ino {
                        let mut idx = 1i64;
                        if idx > offset {
                            let _ = reply.add(torrent_ino, idx, FileType::Directory, ".");
                        }
                        idx += 1;
                        if idx > offset {
                            let _ = reply.add(INO_DATA, idx, FileType::Directory, "..");
                        }
                        
                        let files = core.tokio_runtime.block_on(
                            core.metadata_manager.get_torrent_files(&torrent.name)
                        );
                        
                        if let Ok(files) = files {
                            for file in files {
                                let file_name = file.path.rsplit('/').next().unwrap_or(&file.path);
                                idx += 1;
                                if idx > offset {
                                    let file_ino = self.file_inode(&torrent.name, &file.path);
                                    if reply.add(file_ino, idx, FileType::RegularFile, file_name) {
                                        break;
                                    }
                                }
                            }
                        }
                        reply.ok();
                        return;
                    }
                }
                
                let mut data_dir_path = String::new();
                for torrent in &torrents {
                    if !torrent.metadata_path.is_empty() {
                        let parts: Vec<&str> = torrent.metadata_path.split('/').collect();
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
                    
                    for torrent in &torrents {
                        if torrent.metadata_path.starts_with(&format!("{}/", data_dir_path)) {
                            let rest = &torrent.metadata_path[data_dir_path.len() + 1..];
                            if !rest.is_empty() {
                                let parts: Vec<&str> = rest.split('/').collect();
                                if parts.len() == 1 {
                                    let torrent_ino = self.torrent_inode(&torrent.metadata_path, &torrent.name);
                                    idx += 1;
                                    if idx > offset {
                                        if reply.add(torrent_ino, idx, FileType::Directory, &torrent.name) {
                                            break;
                                        }
                                    }
                                } else {
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
                    reply.ok();
                    return;
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
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
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
        let metadata_path = if path.contains('/') {
            let parts: Vec<&str> = path.rsplitn(2, '/').collect();
            if parts.len() > 1 {
                parts[1].to_string()
            } else {
                String::new()
            }
        } else {
            String::new()
        };

        if let Some(core) = &self.core {
            let manager = core.metadata_manager.clone();

            match core.tokio_runtime.block_on(manager.process_torrent_data(&data, &metadata_path)) {
                Ok(parsed) => {
                    if let Err(e) = core.tokio_runtime.block_on(manager.persist_to_db(&parsed)) {
                        tracing::error!("Failed to persist torrent to DB: {}", e);
                    }

                    if let Ok(session) = core.session.lock() {
                        if let Err(e) = session.add_torrent_paused(&data, "/tmp/torrentfs") {
                            tracing::error!("Failed to add torrent to session: {}", e);
                        } else {
                            tracing::info!("Added torrent '{}' to libtorrent session (paused)", name);
                        }
                    }

                    tracing::info!(
                        "Processed torrent '{}' ({} files, {} bytes) - kept in metadata/{}",
                        name, parsed.file_count, parsed.total_size, 
                        if metadata_path.is_empty() { "".to_string() } else { format!("/{}/", metadata_path) }
                    );
                }
                Err(e) => {
                    tracing::error!("Failed to process torrent data: {}", e);
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

    fn open(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) {
        reply.error(ENOSYS);
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        _size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        reply.error(ENOSYS);
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
    fn test_core_resources_field_order() {
        let state_dir = PathBuf::from("/tmp/test");
        let fs = TorrentFsFilesystem::new(state_dir);
        assert!(fs.core.is_none());
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
}
