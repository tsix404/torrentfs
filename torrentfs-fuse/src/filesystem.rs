use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyWrite, Request, ReplyCreate,
};
use libc::{EINVAL, ENOENT, ENOSYS, ENOTDIR, EFBIG};
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
    name: String,
    data: Vec<u8>,
}

struct MetadataEntry {
    name: String,
    size: u64,
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

    fn torrent_inode(&self, torrent_name: &str) -> u64 {
        // Generate deterministic inode for torrent directory
        // Using a hash function to avoid collisions
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        torrent_name.hash(&mut hasher);
        (hasher.finish() & 0x7FFFFFFFFFFFFFFF) | 0x8000000000000000 // Ensure high bit set
    }

    fn file_inode(&self, torrent_name: &str, file_path: &str) -> u64 {
        // Generate deterministic inode for file in torrent
        // Using a hash function to avoid collisions
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        torrent_name.hash(&mut hasher);
        file_path.hash(&mut hasher);
        (hasher.finish() & 0x7FFFFFFFFFFFFFFF) | 0x8000000000000000 // Ensure high bit set
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
                if entry.name == name_str {
                    reply.entry(&TTL, &file_attr(*ino, entry.size), 0);
                    return;
                }
            }
            reply.error(ENOENT);
        } else if parent == INO_DATA {
            // Lookup torrent directory in data/
            let name_str = name.to_string_lossy();
            
            // Check if this is a torrent directory
            if let Some(core) = &self.core {
                let torrents = core.tokio_runtime.block_on(core.metadata_manager.list_torrents());
                match torrents {
                    Ok(torrents) => {
                        for torrent in torrents {
                            if torrent.name == name_str {
                                let ino = self.torrent_inode(&torrent.name);
                                reply.entry(&TTL, &dir_attr(ino), 0);
                                return;
                            }
                        }
                        reply.error(ENOENT);
                    }
                    Err(_) => reply.error(ENOENT),
                }
            } else {
                reply.error(ENOENT);
            }
        } else {
            // Check if parent is a torrent directory
            if let Some(core) = &self.core {
                let torrents = core.tokio_runtime.block_on(core.metadata_manager.list_torrents());
                if let Ok(torrents) = torrents {
                    for torrent in torrents {
                        let torrent_ino = self.torrent_inode(&torrent.name);
                        if parent == torrent_ino {
                            // This is a lookup for a file within a torrent directory
                            let name_str = name.to_string_lossy();
                            
                            // Get files for this torrent
                            let files = core.tokio_runtime.block_on(
                                core.metadata_manager.get_torrent_files(&torrent.name)
                            );
                            
                            if let Ok(files) = files {
                                for file in files {
                                    if file.path == name_str {
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
                }
            }
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
        
        // Check if ino corresponds to a torrent directory
        if let Some(core) = &self.core {
            let torrents = core.tokio_runtime.block_on(core.metadata_manager.list_torrents());
            if let Ok(torrents) = torrents {
                for torrent in torrents {
                    let torrent_ino = self.torrent_inode(&torrent.name);
                    if torrent_ino == ino {
                        reply.attr(&TTL, &dir_attr(ino));
                        return;
                    }
                }
            }
        }
        
        // Check if ino corresponds to a file in a torrent directory
        if let Some(core) = &self.core {
            let torrents = core.tokio_runtime.block_on(core.metadata_manager.list_torrents());
            if let Ok(torrents) = torrents {
                for torrent in torrents {
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
                if let Some(open_file) = self.open_files.values_mut().find(|f| f.name == entry.name) {
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
        
        // Check if ino corresponds to a torrent directory
        if let Some(core) = &self.core {
            let torrents = core.tokio_runtime.block_on(core.metadata_manager.list_torrents());
            if let Ok(torrents) = torrents {
                for torrent in torrents {
                    let torrent_ino = self.torrent_inode(&torrent.name);
                    if torrent_ino == ino {
                        // Torrent directories are read-only
                        reply.attr(&TTL, &dir_attr(ino));
                        return;
                    }
                    
                    // Check if ino corresponds to a file in torrent directory
                    let files = core.tokio_runtime.block_on(
                        core.metadata_manager.get_torrent_files(&torrent.name)
                    );
                    
                    if let Ok(files) = files {
                        for file in files {
                            let file_ino = self.file_inode(&torrent.name, &file.path);
                            if file_ino == ino {
                                // Data files are read-only
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
            for (file_ino, entry) in &self.metadata_entries {
                idx += 1;
                if idx > offset {
                    if reply.add(*file_ino, idx, FileType::RegularFile, &entry.name) {
                        break;
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
            
            // List torrents from database
            if let Some(core) = &self.core {
                let torrents = core.tokio_runtime.block_on(core.metadata_manager.list_torrents());
                match torrents {
                    Ok(torrents) => {
                        for torrent in torrents {
                            idx += 1;
                            if idx > offset {
                                let ino = self.torrent_inode(&torrent.name);
                                if reply.add(ino, idx, FileType::Directory, &torrent.name) {
                                    break;
                                }
                            }
                        }
                    }
                    Err(_) => {
                        // If database error, return empty directory
                    }
                }
            }
            reply.ok();
        } else {
            // Check if this is a torrent directory
            if let Some(core) = &self.core {
                let torrents = core.tokio_runtime.block_on(core.metadata_manager.list_torrents());
                if let Ok(torrents) = torrents {
                    for torrent in torrents {
                        let torrent_ino = self.torrent_inode(&torrent.name);
                        if ino == torrent_ino {
                            // List files in torrent directory
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
                                    idx += 1;
                                    if idx > offset {
                                        let file_ino = self.file_inode(&torrent.name, &file.path);
                                        if reply.add(file_ino, idx, FileType::RegularFile, &file.path) {
                                            break;
                                        }
                                    }
                                }
                            }
                            reply.ok();
                            return;
                        }
                    }
                }
            }
            reply.error(ENOTDIR);
        }
    }

    fn opendir(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) {
        reply.opened(0, 0);
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
        if parent != INO_METADATA {
            reply.error(ENOENT);
            return;
        }

        let name_str = name.to_string_lossy();
        if !name_str.ends_with(".torrent") {
            reply.error(EINVAL);
            return;
        }

        let name_owned = name_str.into_owned();

        for entry in self.metadata_entries.values() {
            if entry.name == name_owned {
                reply.error(libc::EEXIST);
                return;
            }
        }

        let ino = self.allocate_ino();
        let fh = self.allocate_fh();

        self.metadata_entries.insert(ino, MetadataEntry {
            name: name_owned.clone(),
            size: 0,
        });
        self.open_files.insert(fh, OpenFile {
            name: name_owned,
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

        let name = open_file.name.clone();
        let data = open_file.data.clone();

        if let Some(core) = &self.core {
            let manager = core.metadata_manager.clone();

            match core.tokio_runtime.block_on(manager.process_torrent_data(&data)) {
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
                        "Processed torrent '{}' ({} files, {} bytes) - kept in metadata/",
                        name, parsed.file_count, parsed.total_size
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
            name: "test.torrent".to_string(),
            size: 0,
        });
        fs.open_files.insert(fh, OpenFile {
            name: "test.torrent".to_string(),
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
            name: "keep.torrent".to_string(),
            size: 5,
        });
        fs.open_files.insert(fh, OpenFile {
            name: "keep.torrent".to_string(),
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
            name: "dup.torrent".to_string(),
            size: 0,
        });
        fs.open_files.insert(fh, OpenFile {
            name: "dup.torrent".to_string(),
            data: Vec::new(),
        });

        for entry in fs.metadata_entries.values() {
            if entry.name == "dup.torrent" {
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
}
