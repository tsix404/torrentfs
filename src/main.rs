use clap::Parser;
use fuser::{
    FileAttr, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyEntry, ReplyOpen,
    ReplyDirectory, Request, ReplyWrite, ReplyCreate,
};
use libc::{EACCES, EEXIST, EFBIG, EINVAL, EIO, EISDIR, ENOENT, ENOTDIR};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

mod db;
mod error;
mod torrent_info;

use db::{Database, DbError, FileEntry, InsertTorrentResult};
use torrent_info::TorrentInfo;

const ROOT_INO: u64 = 1;
const METADATA_INO: u64 = 2;
const DATA_INO: u64 = 3;
const MAX_TORRENT_SIZE: usize = 10 * 1024 * 1024;

static NEXT_INO: AtomicU64 = AtomicU64::new(4);
static NEXT_FH: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
enum InodeData {
    Directory { parent: u64, name: String },
    File { parent: u64, name: String, data: Vec<u8> },
}

#[derive(Parser, Debug)]
#[command(name = "torrentfs")]
#[command(about = "A FUSE filesystem for torrent management")]
struct Args {
    #[arg(help = "Mount point path")]
    mountpoint: PathBuf,
    #[arg(long, help = "Database path")]
    db: Option<PathBuf>,
}

struct TorrentFs {
    creation_time: Duration,
    inodes: HashMap<u64, InodeData>,
    open_files: HashMap<u64, u64>,
    db: Option<Arc<Mutex<Database>>>,
    processing_torrents: Arc<Mutex<HashMap<String, ()>>>,
}

impl TorrentFs {
    fn new() -> Self {
        let mut inodes = HashMap::new();
        inodes.insert(ROOT_INO, InodeData::Directory { parent: 0, name: String::new() });
        inodes.insert(METADATA_INO, InodeData::Directory { parent: ROOT_INO, name: "metadata".to_string() });
        inodes.insert(DATA_INO, InodeData::Directory { parent: ROOT_INO, name: "data".to_string() });
        
        Self {
            creation_time: Duration::from_secs(
                std::time::SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            ),
            inodes,
            open_files: HashMap::new(),
            db: None,
            processing_torrents: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn new_with_db(db: Database) -> Self {
        let mut fs = Self::new();
        fs.db = Some(Arc::new(Mutex::new(db)));
        fs
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
        let mut entries: Vec<(u64, i64, fuser::FileType, &str)> = vec![
            (ino, 1, fuser::FileType::Directory, "."),
        ];
        
        if ino == ROOT_INO {
            entries.push((ROOT_INO, 2, fuser::FileType::Directory, ".."));
            entries.push((METADATA_INO, 3, fuser::FileType::Directory, "metadata"));
            entries.push((DATA_INO, 4, fuser::FileType::Directory, "data"));
        } else if let Some(InodeData::Directory { parent, .. }) = self.inodes.get(&ino) {
            entries.push((*parent, 2, fuser::FileType::Directory, ".."));
        } else {
            reply.error(ENOTDIR);
            return;
        }
        
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

fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set tracing subscriber");

    let args = Args::parse();

    if !args.mountpoint.exists() {
        std::fs::create_dir_all(&args.mountpoint)
            .expect("Failed to create mountpoint");
    }

    let fs = if let Some(db_path) = &args.db {
        match Database::open(db_path) {
            Ok(db) => {
                info!("Database opened at {:?}", db_path);
                TorrentFs::new_with_db(db)
            }
            Err(e) => {
                error!("Failed to open database: {:?}", e);
                std::process::exit(1);
            }
        }
    } else {
        let db_path = args.mountpoint.join(".torrentfs/metadata.db");
        if let Some(parent) = db_path.parent() {
            if !parent.exists() {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    warn!("Failed to create database directory: {:?}", e);
                }
            }
        }
        match Database::open(&db_path) {
            Ok(db) => {
                info!("Database opened at {:?}", db_path);
                TorrentFs::new_with_db(db)
            }
            Err(e) => {
                warn!("Failed to open database at {:?}: {:?}, running without persistence", db_path, e);
                TorrentFs::new()
            }
        }
    };

    info!("Mounting torrentfs at {:?}", args.mountpoint);

    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AllowOther,
        MountOption::AutoUnmount,
    ];

    fuser::mount2(fs, &args.mountpoint, &options)
        .expect("Failed to mount filesystem");

    info!("torrentfs unmounted successfully");
}
