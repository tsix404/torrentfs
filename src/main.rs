use clap::Parser;
use fuser::{
    FileAttr, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyEntry, ReplyOpen,
    ReplyDirectory, Request, ReplyWrite, ReplyCreate,
};
use libc::{EACCES, EEXIST, EISDIR, ENOENT};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, UNIX_EPOCH};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

mod error;
mod torrent_info;
use error::TorrentfsError;
use torrent_info::TorrentInfo;

const ROOT_INO: u64 = 1;
const METADATA_INO: u64 = 2;
const DATA_INO: u64 = 3;

static NEXT_INO: AtomicU64 = AtomicU64::new(4);

#[derive(Clone, Debug)]
enum InodeData {
    Directory,
    File { parent_path: String, name: String, data: Vec<u8> },
}

#[derive(Parser, Debug)]
#[command(name = "torrentfs")]
#[command(about = "A FUSE filesystem for torrent management")]
struct Args {
    #[arg(help = "Mount point path")]
    mountpoint: PathBuf,
}

struct TorrentFs {
    creation_time: Duration,
    inodes: HashMap<u64, InodeData>,
    open_files: HashMap<u64, u64>,
}

impl TorrentFs {
    fn new() -> Self {
        let mut inodes = HashMap::new();
        inodes.insert(ROOT_INO, InodeData::Directory);
        inodes.insert(METADATA_INO, InodeData::Directory);
        inodes.insert(DATA_INO, InodeData::Directory);
        
        Self {
            creation_time: Duration::from_secs(
                std::time::SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            ),
            inodes,
            open_files: HashMap::new(),
        }
    }

    fn attr_for_root(&self, ino: u64) -> FileAttr {
        FileAttr {
            ino,
            size: 0,
            blocks: 0,
            atime: UNIX_EPOCH + self.creation_time,
            mtime: UNIX_EPOCH + self.creation_time,
            ctime: UNIX_EPOCH + self.creation_time,
            crtime: UNIX_EPOCH + self.creation_time,
            kind: fuser::FileType::Directory,
            perm: 0o755,
            nlink: 2,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            blksize: 512,
            flags: 0,
        }
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
        
        for (ino, data) in &self.inodes {
            if let InodeData::File { parent_path, name: file_name, .. } = data {
                let expected_parent = if parent == METADATA_INO {
                    String::new()
                } else {
                    let parent_data = self.inodes.get(&parent);
                    match parent_data {
                        Some(InodeData::Directory) => String::new(),
                        Some(InodeData::File { parent_path: pp, .. }) => pp.clone(),
                        None => continue,
                    }
                };
                
                let matches = if parent == METADATA_INO {
                    file_name.as_str() == name_str
                } else if let Some(InodeData::Directory) = self.inodes.get(&parent) {
                    parent_path == &expected_parent && file_name.as_str() == name_str.as_ref()
                } else {
                    false
                };
                
                if matches {
                    let file_data = match &self.inodes.get(ino) {
                        Some(InodeData::File { data, .. }) => data.clone(),
                        _ => Vec::new(),
                    };
                    reply.entry(&Duration::from_secs(1), &self.attr_for_file(*ino, file_data.len() as u64), 0);
                    return;
                }
            }
        }
        
        for (ino, data) in &self.inodes {
            if let InodeData::Directory = data {
                if *ino == parent {
                    continue;
                }
                
                let is_child_of_parent = parent == METADATA_INO || 
                    self.inodes.get(&parent).map_or(false, |p| matches!(p, InodeData::Directory));
                
                if is_child_of_parent && *ino != parent {
                    reply.error(ENOENT);
                    return;
                }
            }
        }
        
        reply.error(ENOENT);
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match ino {
            ROOT_INO => reply.attr(&Duration::from_secs(1), &self.attr_for_root(ino)),
            METADATA_INO => reply.attr(&Duration::from_secs(1), &self.attr_for_dir(ino, true)),
            DATA_INO => reply.attr(&Duration::from_secs(1), &self.attr_for_dir(ino, false)),
            _ => {
                if let Some(data) = &self.inodes.get(&ino) {
                    match data {
                        InodeData::Directory => {
                            reply.attr(&Duration::from_secs(1), &self.attr_for_dir(ino, true));
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
        let mut entries: Vec<(u64, i64, fuser::FileType, &str)> = match ino {
            ROOT_INO => vec![
                (ROOT_INO, 1, fuser::FileType::Directory, "."),
                (ROOT_INO, 2, fuser::FileType::Directory, ".."),
                (METADATA_INO, 3, fuser::FileType::Directory, "metadata"),
                (DATA_INO, 4, fuser::FileType::Directory, "data"),
            ],
            METADATA_INO | DATA_INO => vec![
                (ino, 1, fuser::FileType::Directory, "."),
                (ROOT_INO, 2, fuser::FileType::Directory, ".."),
            ],
            _ => {
                if let Some(InodeData::Directory) = self.inodes.get(&ino) {
                    vec![
                        (ino, 1, fuser::FileType::Directory, "."),
                        (ROOT_INO, 2, fuser::FileType::Directory, ".."),
                    ]
                } else {
                    reply.error(ENOENT);
                    return;
                }
            }
        };
        
        let mut offset_counter = entries.len() as i64 + 1;
        for (child_ino, data) in &self.inodes {
            if let InodeData::File { parent_path, name, .. } = data {
                let is_direct_child = if ino == METADATA_INO {
                    parent_path.is_empty()
                } else if let Some(InodeData::Directory) = self.inodes.get(&ino) {
                    let _parent_data = self.inodes.iter()
                        .find(|(i, d)| matches!(d, InodeData::Directory) && **i == ino);
                    true
                } else {
                    false
                };
                
                if is_direct_child {
                    entries.push((*child_ino, offset_counter, fuser::FileType::RegularFile, name.as_str()));
                    offset_counter += 1;
                }
            } else if let InodeData::Directory = data {
                if *child_ino > DATA_INO && *child_ino != ino {
                    entries.push((*child_ino, offset_counter, fuser::FileType::Directory, ""));
                    offset_counter += 1;
                }
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
                    let fh = self.open_files.len() as u64;
                    self.open_files.insert(ino, fh);
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
        ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        self.open_files.remove(&ino);
        
        if let Some(InodeData::File { data, name, .. }) = self.inodes.get(&ino) {
            if name.ends_with(".torrent") && !data.is_empty() {
                let temp_dir = tempfile::tempdir();
                if let Ok(temp_dir) = temp_dir {
                    let temp_path = temp_dir.path().join(name);
                    if std::fs::write(&temp_path, data).is_ok() {
                        match TorrentInfo::from_file(&temp_path) {
                            Ok(info) => {
                                info!("Parsed torrent file: {} ({} bytes, {} files)", 
                                      info.name(), info.total_size(), info.num_files());
                            }
                            Err(e) => {
                                info!("Failed to parse torrent file {}: {:?}", name, e);
                            }
                        }
                    }
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
        if parent == DATA_INO {
            reply.error(EACCES);
            return;
        }
        
        if parent != METADATA_INO {
            reply.error(EACCES);
            return;
        }
        
        let name_str = name.to_string_lossy();
        if !name_str.ends_with(".torrent") {
            reply.error(EACCES);
            return;
        }
        
        for (_, data) in &self.inodes {
            if let InodeData::File { name: existing_name, .. } = data {
                if existing_name == name_str.as_ref() {
                    reply.error(EEXIST);
                    return;
                }
            }
        }
        
        let new_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
        let parent_path = String::new();
        
        self.inodes.insert(new_ino, InodeData::File {
            parent_path,
            name: name_str.to_string(),
            data: Vec::new(),
        });
        
        info!("Created file {} with inode {}", name_str, new_ino);
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
        if parent == DATA_INO {
            reply.error(EACCES);
            return;
        }
        
        if parent != METADATA_INO {
            reply.error(EACCES);
            return;
        }
        
        let name_str = name.to_string_lossy();
        if !name_str.ends_with(".torrent") {
            reply.error(EACCES);
            return;
        }
        
        for (_, data) in &self.inodes {
            if let InodeData::File { name: existing_name, .. } = data {
                if existing_name == name_str.as_ref() {
                    reply.error(EEXIST);
                    return;
                }
            }
        }
        
        let new_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
        let parent_path = String::new();
        
        self.inodes.insert(new_ino, InodeData::File {
            parent_path,
            name: name_str.to_string(),
            data: Vec::new(),
        });
        
        let fh = self.open_files.len() as u64;
        self.open_files.insert(new_ino, fh);
        
        info!("Created file {} with inode {}", name_str, new_ino);
        reply.created(&Duration::from_secs(1), &self.attr_for_file(new_ino, 0), 0, 0, 0);
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
        if parent == DATA_INO {
            reply.error(EACCES);
            return;
        }
        
        if parent != METADATA_INO {
            reply.error(EACCES);
            return;
        }
        
        let name_str = name.to_string_lossy();
        
        let new_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
        self.inodes.insert(new_ino, InodeData::Directory);
        
        info!("Created directory {} with inode {}", name_str, new_ino);
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
                InodeData::Directory => {
                    reply.attr(&Duration::from_secs(1), &self.attr_for_dir(ino, true));
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

fn main() -> Result<(), TorrentfsError> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|e| TorrentfsError::Initialization(e.to_string()))?;

    let args = Args::parse();

    if !args.mountpoint.exists() {
        std::fs::create_dir_all(&args.mountpoint)
            .map_err(|e| TorrentfsError::Mount(e.to_string()))?;
    }

    info!("Mounting torrentfs at {:?}", args.mountpoint);

    let fs = TorrentFs::new();
    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AllowOther,
        MountOption::AutoUnmount,
    ];

    fuser::mount2(fs, &args.mountpoint, &options)
        .map_err(|e| TorrentfsError::Mount(e.to_string()))?;

    info!("torrentfs unmounted successfully");
    Ok(())
}
