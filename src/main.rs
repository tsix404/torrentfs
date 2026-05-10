use clap::Parser;
use fuser::{
    FileAttr, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyEntry, ReplyOpen,
    ReplyDirectory, Request,
};
use libc::{EISDIR, ENOENT};
use std::ffi::{OsStr, OsString};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

mod db;
mod error;
use db::{Database, Torrent, TorrentDirectory, TorrentFile};
use error::TorrentfsError;

const ROOT_INO: u64 = 1;
const METADATA_INO: u64 = 2;
const DATA_INO: u64 = 3;
const TORRENT_INO_BASE: u64 = 1000;
const DIR_INO_BASE: u64 = 100000;
const FILE_INO_BASE: u64 = 1000000;

#[derive(Parser, Debug)]
#[command(name = "torrentfs")]
#[command(about = "A FUSE filesystem for torrent management")]
struct Args {
    #[arg(help = "Mount point path")]
    mountpoint: PathBuf,

    #[arg(long, help = "Database path (default: in-memory)")]
    db: Option<PathBuf>,
}

struct TorrentFs {
    creation_time: Duration,
    db: Arc<Mutex<Database>>,
}

impl TorrentFs {
    fn new(db: Database) -> Self {
        Self {
            creation_time: Duration::from_secs(
                std::time::SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            ),
            db: Arc::new(Mutex::new(db)),
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
            perm: 0o444,
            nlink: 1,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            blksize: 512,
            flags: 0,
        }
    }

    fn torrent_id_from_ino(ino: u64) -> Option<i64> {
        if ino >= TORRENT_INO_BASE && ino < DIR_INO_BASE {
            Some((ino - TORRENT_INO_BASE) as i64)
        } else {
            None
        }
    }

    fn dir_id_from_ino(ino: u64) -> Option<i64> {
        if ino >= DIR_INO_BASE && ino < FILE_INO_BASE {
            Some((ino - DIR_INO_BASE) as i64)
        } else {
            None
        }
    }

    fn file_id_from_ino(ino: u64) -> Option<i64> {
        if ino >= FILE_INO_BASE {
            Some((ino - FILE_INO_BASE) as i64)
        } else {
            None
        }
    }

    fn ino_from_torrent_id(id: i64) -> u64 {
        TORRENT_INO_BASE + id as u64
    }

    fn ino_from_dir_id(id: i64) -> u64 {
        DIR_INO_BASE + id as u64
    }

    fn ino_from_file_id(id: i64) -> u64 {
        FILE_INO_BASE + id as u64
    }

    fn get_torrent_by_source_path(&self, source_path: &str) -> Option<Torrent> {
        let db = self.db.lock().unwrap();
        db.get_torrent_by_source_path(source_path).ok().flatten()
    }

    fn get_all_torrents(&self) -> Vec<Torrent> {
        let db = self.db.lock().unwrap();
        db.get_all_torrents().unwrap_or_default()
    }

    fn get_torrent_directories(&self, torrent_id: i64, parent_id: Option<i64>) -> Vec<TorrentDirectory> {
        let db = self.db.lock().unwrap();
        db.get_torrent_directories_by_parent(parent_id, torrent_id).unwrap_or_default()
    }

    fn get_root_files(&self, torrent_id: i64) -> Vec<TorrentFile> {
        let db = self.db.lock().unwrap();
        db.get_root_files(torrent_id).unwrap_or_default()
    }

    fn get_files_in_directory(&self, directory_id: i64) -> Vec<TorrentFile> {
        let db = self.db.lock().unwrap();
        db.get_files_in_directory(directory_id).unwrap_or_default()
    }

    fn get_torrent_directory(&self, torrent_id: i64, parent_id: Option<i64>, name: &str) -> Option<TorrentDirectory> {
        let db = self.db.lock().unwrap();
        db.get_torrent_directory(torrent_id, parent_id, name).ok().flatten()
    }

    fn get_torrent_id_for_directory(&self, dir_id: i64) -> Option<i64> {
        let db = self.db.lock().unwrap();
        db.get_torrent_directories_by_parent(None, 0)
            .ok()?
            .into_iter()
            .find(|d| d.id == dir_id)
            .map(|d| d.torrent_id)
    }
}

struct DirEntry {
    ino: u64,
    offset: i64,
    kind: fuser::FileType,
    name: OsString,
}

impl Filesystem for TorrentFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = name.to_string_lossy();
        
        match parent {
            ROOT_INO => {
                match name_str.as_ref() {
                    "metadata" => {
                        reply.entry(&Duration::from_secs(1), &self.attr_for_dir(METADATA_INO, true), 0);
                    }
                    "data" => {
                        reply.entry(&Duration::from_secs(1), &self.attr_for_dir(DATA_INO, false), 0);
                    }
                    _ => reply.error(ENOENT),
                }
            }
            DATA_INO => {
                if let Some(torrent) = self.get_torrent_by_source_path(&name_str) {
                    let ino = Self::ino_from_torrent_id(torrent.id);
                    reply.entry(&Duration::from_secs(1), &self.attr_for_dir(ino, false), 0);
                } else {
                    reply.error(ENOENT);
                }
            }
            ino if Self::torrent_id_from_ino(ino).is_some() => {
                let torrent_id = Self::torrent_id_from_ino(ino).unwrap();
                
                if let Some(dir) = self.get_torrent_directory(torrent_id, None, &name_str) {
                    let dir_ino = Self::ino_from_dir_id(dir.id);
                    reply.entry(&Duration::from_secs(1), &self.attr_for_dir(dir_ino, false), 0);
                } else if let Some(dir_id) = Self::dir_id_from_ino(ino) {
                    if let Some(dir) = self.get_torrent_directory(torrent_id, Some(dir_id), &name_str) {
                        let dir_ino = Self::ino_from_dir_id(dir.id);
                        reply.entry(&Duration::from_secs(1), &self.attr_for_dir(dir_ino, false), 0);
                    } else {
                        let db = self.db.lock().unwrap();
                        let files = db.get_files_in_directory(dir_id).unwrap_or_default();
                        drop(db);
                        
                        if let Some(file) = files.iter().find(|f| f.name == name_str) {
                            let file_ino = Self::ino_from_file_id(file.id);
                            reply.entry(&Duration::from_secs(1), &self.attr_for_file(file_ino, file.size as u64), 0);
                        } else {
                            reply.error(ENOENT);
                        }
                    }
                } else {
                    let root_files = self.get_root_files(torrent_id);
                    if let Some(file) = root_files.iter().find(|f| f.name == name_str) {
                        let file_ino = Self::ino_from_file_id(file.id);
                        reply.entry(&Duration::from_secs(1), &self.attr_for_file(file_ino, file.size as u64), 0);
                    } else {
                        reply.error(ENOENT);
                    }
                }
            }
            _ => reply.error(ENOENT),
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match ino {
            ROOT_INO => reply.attr(&Duration::from_secs(1), &self.attr_for_root(ino)),
            METADATA_INO => reply.attr(&Duration::from_secs(1), &self.attr_for_dir(ino, true)),
            DATA_INO => reply.attr(&Duration::from_secs(1), &self.attr_for_dir(ino, false)),
            ino if Self::torrent_id_from_ino(ino).is_some() => {
                reply.attr(&Duration::from_secs(1), &self.attr_for_dir(ino, false));
            }
            ino if Self::dir_id_from_ino(ino).is_some() => {
                reply.attr(&Duration::from_secs(1), &self.attr_for_dir(ino, false));
            }
            _ => reply.error(ENOENT),
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
        let entries: Vec<DirEntry> = match ino {
            ROOT_INO => vec![
                DirEntry { ino: ROOT_INO, offset: 1, kind: fuser::FileType::Directory, name: OsString::from(".") },
                DirEntry { ino: ROOT_INO, offset: 2, kind: fuser::FileType::Directory, name: OsString::from("..") },
                DirEntry { ino: METADATA_INO, offset: 3, kind: fuser::FileType::Directory, name: OsString::from("metadata") },
                DirEntry { ino: DATA_INO, offset: 4, kind: fuser::FileType::Directory, name: OsString::from("data") },
            ],
            METADATA_INO => vec![
                DirEntry { ino, offset: 1, kind: fuser::FileType::Directory, name: OsString::from(".") },
                DirEntry { ino: ROOT_INO, offset: 2, kind: fuser::FileType::Directory, name: OsString::from("..") },
            ],
            DATA_INO => {
                let mut entries = vec![
                    DirEntry { ino, offset: 1, kind: fuser::FileType::Directory, name: OsString::from(".") },
                    DirEntry { ino: ROOT_INO, offset: 2, kind: fuser::FileType::Directory, name: OsString::from("..") },
                ];
                
                let torrents = self.get_all_torrents();
                for (idx, torrent) in torrents.iter().enumerate() {
                    let torrent_ino = Self::ino_from_torrent_id(torrent.id);
                    entries.push(DirEntry {
                        ino: torrent_ino,
                        offset: (idx + 3) as i64,
                        kind: fuser::FileType::Directory,
                        name: OsString::from(&torrent.source_path),
                    });
                }
                
                entries
            }
            ino if Self::torrent_id_from_ino(ino).is_some() => {
                let torrent_id = Self::torrent_id_from_ino(ino).unwrap();
                
                let mut entries = vec![
                    DirEntry { ino, offset: 1, kind: fuser::FileType::Directory, name: OsString::from(".") },
                    DirEntry { ino: DATA_INO, offset: 2, kind: fuser::FileType::Directory, name: OsString::from("..") },
                ];
                
                let mut offset_counter = 3i64;
                
                let dirs = self.get_torrent_directories(torrent_id, None);
                for dir in &dirs {
                    let dir_ino = Self::ino_from_dir_id(dir.id);
                    entries.push(DirEntry {
                        ino: dir_ino,
                        offset: offset_counter,
                        kind: fuser::FileType::Directory,
                        name: OsString::from(&dir.name),
                    });
                    offset_counter += 1;
                }
                
                let files = self.get_root_files(torrent_id);
                for file in &files {
                    let file_ino = Self::ino_from_file_id(file.id);
                    entries.push(DirEntry {
                        ino: file_ino,
                        offset: offset_counter,
                        kind: fuser::FileType::RegularFile,
                        name: OsString::from(&file.name),
                    });
                    offset_counter += 1;
                }
                
                entries
            }
            ino if Self::dir_id_from_ino(ino).is_some() => {
                let dir_id = Self::dir_id_from_ino(ino).unwrap();
                let torrent_id = match self.get_torrent_id_for_directory(dir_id) {
                    Some(id) => id,
                    None => {
                        reply.error(ENOENT);
                        return;
                    }
                };
                
                let mut entries = vec![
                    DirEntry { ino, offset: 1, kind: fuser::FileType::Directory, name: OsString::from(".") },
                    DirEntry { ino: DATA_INO, offset: 2, kind: fuser::FileType::Directory, name: OsString::from("..") },
                ];
                
                let mut offset_counter = 3i64;
                
                let dirs = self.get_torrent_directories(torrent_id, Some(dir_id));
                for dir in &dirs {
                    let dir_ino = Self::ino_from_dir_id(dir.id);
                    entries.push(DirEntry {
                        ino: dir_ino,
                        offset: offset_counter,
                        kind: fuser::FileType::Directory,
                        name: OsString::from(&dir.name),
                    });
                    offset_counter += 1;
                }
                
                let files = self.get_files_in_directory(dir_id);
                for file in &files {
                    let file_ino = Self::ino_from_file_id(file.id);
                    entries.push(DirEntry {
                        ino: file_ino,
                        offset: offset_counter,
                        kind: fuser::FileType::RegularFile,
                        name: OsString::from(&file.name),
                    });
                    offset_counter += 1;
                }
                
                entries
            }
            _ => {
                reply.error(ENOENT);
                return;
            }
        };

        for entry in entries.iter() {
            if entry.offset <= offset {
                continue;
            }
            if reply.add(entry.ino, entry.offset, entry.kind, &entry.name) {
                break;
            }
        }
        reply.ok();
    }

    fn open(&mut self, _req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        match ino {
            ROOT_INO | METADATA_INO | DATA_INO => reply.opened(0, 0),
            ino if Self::torrent_id_from_ino(ino).is_some() => reply.opened(0, 0),
            ino if Self::dir_id_from_ino(ino).is_some() => reply.opened(0, 0),
            _ => reply.error(ENOENT),
        }
    }

    fn release(
        &mut self,
        _req: &Request,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        reply.ok();
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        _offset: i64,
        _size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        match ino {
            ROOT_INO | METADATA_INO | DATA_INO => reply.error(EISDIR),
            ino if Self::torrent_id_from_ino(ino).is_some() => reply.error(EISDIR),
            ino if Self::dir_id_from_ino(ino).is_some() => reply.error(EISDIR),
            _ => reply.error(ENOENT),
        }
    }

    fn opendir(&mut self, _req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        match ino {
            ROOT_INO | METADATA_INO | DATA_INO => reply.opened(0, 0),
            ino if Self::torrent_id_from_ino(ino).is_some() => reply.opened(0, 0),
            ino if Self::dir_id_from_ino(ino).is_some() => reply.opened(0, 0),
            _ => reply.error(ENOENT),
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

    let db = match args.db {
        Some(path) => Database::open(&path).map_err(|e| TorrentfsError::Initialization(e.to_string()))?,
        None => Database::open_in_memory().map_err(|e| TorrentfsError::Initialization(e.to_string()))?,
    };

    info!("Mounting torrentfs at {:?}", args.mountpoint);

    let fs = TorrentFs::new(db);
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
