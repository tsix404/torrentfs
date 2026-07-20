use clap::Parser;
use fuser::{
    FileAttr, Filesystem, MountOption, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEntry, ReplyOpen, ReplyWrite, Request,
};
use libc::{EACCES, EEXIST, EFBIG, EINVAL, EIO, EISDIR, ENOENT, ENOTDIR, ENOTEMPTY};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{self, BufRead, BufReader};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

mod cache;
mod config;
mod db;
mod download;
mod error;
mod seeding;
mod torrent_info;

use cache::CacheManager;
use config::TorrentfsConfig;
use db::{Database, FileEntry, InsertTorrentResult};
use download::DownloadManager;
use seeding::SeedingManager;
use torrent_info::TorrentInfo;

const ROOT_INO: u64 = 1;
const METADATA_INO: u64 = 2;
const DATA_INO: u64 = 3;
const STATS_INO: u64 = 4;
const MAX_TORRENT_SIZE: usize = 10 * 1024 * 1024;
const DATA_TORRENT_INO_BASE: u64 = 1_000_000;
const DATA_DIR_INO_BASE: u64 = 2_000_000;
const DATA_FILE_INO_BASE: u64 = 3_000_000;
const SOURCE_PATH_DIR_INO_BASE: u64 = 4_000_000;

static NEXT_INO: AtomicU64 = AtomicU64::new(5);
static NEXT_FH: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
enum InodeData {
    Directory {
        parent: u64,
        name: String,
    },
    File {
        parent: u64,
        name: String,
        data: Vec<u8>,
    },
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
enum DataInode {
    SourcePathDir {
        path: String,
    },
    TorrentRoot {
        torrent_id: i64,
        source_path: String,
        name: String,
        filename: String,
    },
    TorrentDir {
        torrent_id: i64,
        dir_id: i64,
        name: String,
    },
    TorrentFile {
        torrent_id: i64,
        file_id: i64,
        name: String,
        size: i64,
    },
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
    #[arg(long, help = "Configuration file path (TOML)")]
    config: Option<PathBuf>,
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
    #[allow(dead_code)]
    cache_manager: Option<Arc<Mutex<CacheManager>>>,
}

impl TorrentFs {
    fn new_with_cache_path(cache_path: PathBuf, config: &TorrentfsConfig) -> Self {
        let mut inodes = HashMap::new();
        inodes.insert(
            ROOT_INO,
            InodeData::Directory {
                parent: 0,
                name: String::new(),
            },
        );
        inodes.insert(
            METADATA_INO,
            InodeData::Directory {
                parent: ROOT_INO,
                name: "metadata".to_string(),
            },
        );
        inodes.insert(
            DATA_INO,
            InodeData::Directory {
                parent: ROOT_INO,
                name: "data".to_string(),
            },
        );
        inodes.insert(
            STATS_INO,
            InodeData::File {
                parent: ROOT_INO,
                name: ".stats".to_string(),
                data: Vec::new(),
            },
        );

        if !cache_path.exists() {
            if let Err(e) = std::fs::create_dir_all(&cache_path) {
                warn!("Failed to create cache directory {:?}: {:?}", cache_path, e);
            }
        }

        let download_manager = DownloadManager::new(cache_path.as_path(), config).ok();
        let cache_manager = CacheManager::new(&cache_path, 1024 * 1024 * 1024).ok();

        // Register SeedingManager as eviction callback on the DownloadManager's CacheManager
        if let Some(ref dm) = download_manager {
            if let Ok(seeding) = SeedingManager::new(&cache_path, config) {
                let seeding = std::sync::Arc::new(seeding);
                dm.register_seeding_callback(seeding);
                info!("SeedingManager registered as CacheManager eviction callback");
            }
        }

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

    #[allow(dead_code)]
    fn new() -> Self {
        Self::new_with_cache_path(
            PathBuf::from("/tmp/torrentfs-cache"),
            &TorrentfsConfig::default_config(),
        )
    }

    #[allow(dead_code)]
    fn new_with_db(_db: Database) -> Self {
        Self::new()
    }

    fn new_with_db_and_cache(db: Database, cache_path: PathBuf, config: &TorrentfsConfig) -> Self {
        let mut fs = Self::new_with_cache_path(cache_path, config);

        // First, collect all data we need from the database
        let (dirs, torrents) = {
            let dirs = db.get_all_metadata_directories().unwrap_or_default();
            let torrents = db.get_all_torrents().unwrap_or_default();
            (dirs, torrents)
        };

        fs.db = Some(Arc::new(Mutex::new(db)));
        fs.restore_metadata_inodes(dirs, torrents);
        fs
    }

    /// Restore metadata/ subdirectory inodes from the database on startup.
    /// This is critical for extract_source_path() to work correctly after remount.
    fn restore_metadata_inodes(
        &mut self,
        dirs: Vec<(i64, Option<i64>, String, String)>,
        torrents: Vec<db::Torrent>,
    ) {
        // Build a mapping from metadata_directories id to inode number
        // We use NEXT_INO to create stable inodes for each directory

        // Sort directories by path depth to ensure parents are processed before children
        let mut sorted_dirs = dirs;
        sorted_dirs.sort_by(|a, b| {
            let depth_a = a.3.matches('/').count();
            let depth_b = b.3.matches('/').count();
            depth_a.cmp(&depth_b)
        });

        let mut dir_id_to_ino: HashMap<i64, u64> = HashMap::new();

        for (db_id, parent_db_id, name, path) in &sorted_dirs {
            let parent_ino = if let Some(pid) = parent_db_id {
                // Parent is another metadata subdirectory
                *dir_id_to_ino.get(pid).unwrap_or(&METADATA_INO)
            } else {
                // Parent is metadata/ root
                METADATA_INO
            };

            let new_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
            dir_id_to_ino.insert(*db_id, new_ino);

            self.inodes.insert(
                new_ino,
                InodeData::Directory {
                    parent: parent_ino,
                    name: name.clone(),
                },
            );

            info!(
                "Restored metadata inode {} for path '{}' (db_id={}, parent_ino={})",
                new_ino, path, db_id, parent_ino
            );
        }

        // Also restore torrent file inodes from the database
        for torrent in &torrents {
            // Find the parent directory inode for this torrent's source_path
            let parent_ino = if torrent.source_path.is_empty() {
                METADATA_INO
            } else {
                // Look up the source_path directory in our restored inodes
                // We need to find the inode whose full_path is "metadata/<source_path>"
                let full_source = format!("metadata/{}", torrent.source_path);
                self.find_ino_by_full_path(&full_source)
                    .unwrap_or(METADATA_INO)
            };

            let new_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
            // Use torrent.filename (actual stored filename) instead of torrent.name (internal name)
            // For backward compatibility, fall back to torrent.name if filename is empty (v4 migration)
            let filename = if !torrent.filename.is_empty() {
                // Ensure .torrent extension
                if torrent.filename.ends_with(".torrent") {
                    torrent.filename.clone()
                } else {
                    format!("{}.torrent", torrent.filename)
                }
            } else {
                // Backward compatibility: filename field may be empty (pre-v4 migration)
                if torrent.name.ends_with(".torrent") {
                    torrent.name.clone()
                } else {
                    format!("{}.torrent", torrent.name)
                }
            };
            self.inodes.insert(
                new_ino,
                InodeData::File {
                    parent: parent_ino,
                    name: filename,
                    data: torrent.torrent_data.clone().unwrap_or_default(),
                },
            );
        }
    }

    /// Find an inode number by its full path (e.g., "metadata/os/linux")
    fn find_ino_by_full_path(&self, target_path: &str) -> Option<u64> {
        for (ino, data) in &self.inodes {
            let full_path = match data {
                InodeData::Directory { .. } | InodeData::File { .. } => self.get_full_path(*ino),
            };
            if full_path == target_path {
                return Some(*ino);
            }
        }
        None
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

    fn make_source_path_dir_ino(path: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        SOURCE_PATH_DIR_INO_BASE + (hasher.finish() % 1_000_000)
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
            DataInode::SourcePathDir { path } => self.resolve_source_path_dir_lookup(path, name),
            DataInode::TorrentRoot { torrent_id, .. } => {
                self.resolve_torrent_root_lookup(*torrent_id, name)
            }
            DataInode::TorrentDir {
                torrent_id, dir_id, ..
            } => self.resolve_torrent_dir_lookup(*torrent_id, Some(*dir_id), name),
            DataInode::TorrentFile { .. } => None,
        }
    }

    fn resolve_data_root_lookup(&self, name: &str) -> Option<(u64, DataInode)> {
        let db = self.get_db().ok()?;
        let db_guard = db.lock().ok()?;

        let prefixes = db_guard.get_source_path_prefixes("").ok()?;
        if prefixes.contains(&name.to_string()) {
            let full_path = name.to_string();
            let ino = Self::make_source_path_dir_ino(&full_path);
            return Some((ino, DataInode::SourcePathDir { path: full_path }));
        }

        let root_torrents = db_guard.get_torrents_by_source_path("").ok()?;
        for torrent in root_torrents {
            if torrent.filename == name {
                let ino = Self::make_torrent_root_ino(torrent.id);
                return Some((
                    ino,
                    DataInode::TorrentRoot {
                        torrent_id: torrent.id,
                        source_path: torrent.source_path.clone(),
                        name: torrent.name.clone(),
                        filename: torrent.filename.clone(),
                    },
                ));
            }
        }

        None
    }

    fn resolve_source_path_dir_lookup(&self, prefix: &str, name: &str) -> Option<(u64, DataInode)> {
        let new_path = if prefix.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", prefix, name)
        };

        let db = self.get_db().ok()?;
        let db_guard = db.lock().ok()?;

        let prefixes = db_guard.get_source_path_prefixes(prefix).ok()?;
        if prefixes.contains(&name.to_string()) {
            let ino = Self::make_source_path_dir_ino(&new_path);
            return Some((ino, DataInode::SourcePathDir { path: new_path }));
        }

        let torrents = db_guard.get_torrents_by_source_path(prefix).ok()?;
        for torrent in torrents {
            if torrent.filename == name {
                let ino = Self::make_torrent_root_ino(torrent.id);
                return Some((
                    ino,
                    DataInode::TorrentRoot {
                        torrent_id: torrent.id,
                        source_path: torrent.source_path.clone(),
                        name: torrent.name.clone(),
                        filename: torrent.filename.clone(),
                    },
                ));
            }
        }

        None
    }

    fn resolve_torrent_root_lookup(&self, torrent_id: i64, name: &str) -> Option<(u64, DataInode)> {
        self.resolve_torrent_dir_lookup(torrent_id, None, name)
    }

    fn resolve_torrent_dir_lookup(
        &self,
        torrent_id: i64,
        parent_dir_id: Option<i64>,
        name: &str,
    ) -> Option<(u64, DataInode)> {
        let db = self.get_db().ok()?;
        let db_guard = db.lock().ok()?;

        if let Some(dir) = db_guard
            .get_torrent_directory(torrent_id, parent_dir_id, name)
            .ok()?
        {
            let ino = Self::make_torrent_dir_ino(dir.id);
            return Some((
                ino,
                DataInode::TorrentDir {
                    torrent_id,
                    dir_id: dir.id,
                    name: dir.name,
                },
            ));
        }

        let files = if let Some(pid) = parent_dir_id {
            db_guard.get_files_in_directory(pid).ok()?
        } else {
            db_guard.get_root_files(torrent_id).ok()?
        };

        for file in files {
            if file.name == name {
                let ino = Self::make_torrent_file_ino(file.id);
                return Some((
                    ino,
                    DataInode::TorrentFile {
                        torrent_id,
                        file_id: file.id,
                        name: file.name,
                        size: file.size,
                    },
                ));
            }
        }

        None
    }

    fn lookup_data_inode(
        &mut self,
        parent: u64,
        name: &str,
    ) -> Option<(u64, fuser::FileType, u64)> {
        let (ino, data_inode) = self.resolve_data_lookup(parent, name)?;

        self.data_inodes.insert(ino, data_inode.clone());

        match &data_inode {
            DataInode::SourcePathDir { .. }
            | DataInode::TorrentRoot { .. }
            | DataInode::TorrentDir { .. } => Some((ino, fuser::FileType::Directory, 0)),
            DataInode::TorrentFile { size, .. } => {
                Some((ino, fuser::FileType::RegularFile, *size as u64))
            }
        }
    }

    fn readdir_data(
        &mut self,
        ino: u64,
        offset: i64,
    ) -> Option<Vec<(u64, i64, fuser::FileType, String)>> {
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
                    let name = torrent.filename.clone();
                    cache_entries.push((
                        torrent_ino,
                        DataInode::TorrentRoot {
                            torrent_id: torrent.id,
                            source_path: torrent.source_path.clone(),
                            name: torrent.name.clone(),
                            filename: torrent.filename.clone(),
                        },
                    ));
                    entries.push((
                        torrent_ino,
                        offset_counter,
                        fuser::FileType::Directory,
                        name,
                    ));
                    offset_counter += 1;
                }

                let prefixes = db_guard.get_source_path_prefixes("").ok()?;

                for prefix in prefixes {
                    let child_ino = Self::make_source_path_dir_ino(&prefix);
                    cache_entries.push((
                        child_ino,
                        DataInode::SourcePathDir {
                            path: prefix.clone(),
                        },
                    ));
                    entries.push((
                        child_ino,
                        offset_counter,
                        fuser::FileType::Directory,
                        prefix,
                    ));
                    offset_counter += 1;
                }
            }

            for (cache_ino, cache_inode) in cache_entries {
                self.data_inodes.insert(cache_ino, cache_inode);
            }

            return Some(
                entries
                    .into_iter()
                    .filter(|(_, o, _, _)| *o > offset)
                    .collect(),
            );
        }

        let data_inode = self.data_inodes.get(&ino)?.clone();

        match data_inode {
            DataInode::SourcePathDir { path } => {
                entries.push((ino, 1, fuser::FileType::Directory, ".".to_string()));

                // Calculate the correct parent inode for nested directories
                let parent_ino = if path.is_empty() {
                    DATA_INO
                } else {
                    let path_parts: Vec<&str> = path.split('/').collect();
                    if path_parts.len() == 1 {
                        DATA_INO
                    } else {
                        let parent_path = path_parts[..path_parts.len() - 1].join("/");
                        Self::make_source_path_dir_ino(&parent_path)
                    }
                };
                entries.push((parent_ino, 2, fuser::FileType::Directory, "..".to_string()));

                {
                    let db = self.get_db().ok()?;
                    let db_guard = db.lock().ok()?;

                    let mut offset_counter = 3i64;

                    let sub_prefixes = db_guard.get_source_path_prefixes(&path).ok()?;
                    for sub in sub_prefixes {
                        let new_path = if path.is_empty() {
                            sub.clone()
                        } else {
                            format!("{}/{}", path, sub)
                        };
                        let child_ino = Self::make_source_path_dir_ino(&new_path);
                        cache_entries.push((
                            child_ino,
                            DataInode::SourcePathDir {
                                path: new_path.clone(),
                            },
                        ));
                        entries.push((child_ino, offset_counter, fuser::FileType::Directory, sub));
                        offset_counter += 1;
                    }

                    let direct_torrents = db_guard.get_torrents_by_source_path(&path).ok()?;
                    for torrent in direct_torrents {
                        let torrent_ino = Self::make_torrent_root_ino(torrent.id);
                        let name = torrent.filename.clone();
                        cache_entries.push((
                            torrent_ino,
                            DataInode::TorrentRoot {
                                torrent_id: torrent.id,
                                source_path: torrent.source_path.clone(),
                                name: torrent.name.clone(),
                                filename: torrent.filename.clone(),
                            },
                        ));
                        entries.push((
                            torrent_ino,
                            offset_counter,
                            fuser::FileType::Directory,
                            name,
                        ));
                        offset_counter += 1;
                    }
                }

                for (cache_ino, cache_inode) in cache_entries {
                    self.data_inodes.insert(cache_ino, cache_inode);
                }
            }
            DataInode::TorrentRoot {
                torrent_id,
                source_path,
                ..
            } => {
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

                        let torrents = db_guard
                            .get_torrents_by_source_path(&parent_path)
                            .ok()
                            .unwrap_or_default();
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

                    let root_dirs = db_guard
                        .get_torrent_directories_by_parent(None, torrent_id)
                        .ok()?;
                    for dir in root_dirs {
                        let dir_ino = Self::make_torrent_dir_ino(dir.id);
                        cache_entries.push((
                            dir_ino,
                            DataInode::TorrentDir {
                                torrent_id,
                                dir_id: dir.id,
                                name: dir.name.clone(),
                            },
                        ));
                        entries.push((
                            dir_ino,
                            offset_counter,
                            fuser::FileType::Directory,
                            dir.name,
                        ));
                        offset_counter += 1;
                    }

                    let root_files = db_guard.get_root_files(torrent_id).ok()?;
                    for file in root_files {
                        let file_ino = Self::make_torrent_file_ino(file.id);
                        cache_entries.push((
                            file_ino,
                            DataInode::TorrentFile {
                                torrent_id,
                                file_id: file.id,
                                name: file.name.clone(),
                                size: file.size,
                            },
                        ));
                        entries.push((
                            file_ino,
                            offset_counter,
                            fuser::FileType::RegularFile,
                            file.name,
                        ));
                        offset_counter += 1;
                    }
                }

                for (cache_ino, cache_inode) in cache_entries {
                    self.data_inodes.insert(cache_ino, cache_inode);
                }
            }
            DataInode::TorrentDir {
                torrent_id, dir_id, ..
            } => {
                entries.push((ino, 1, fuser::FileType::Directory, ".".to_string()));

                {
                    let db = self.get_db().ok()?;
                    let db_guard = db.lock().ok()?;

                    let parent_ino = db_guard
                        .get_torrent_directory_by_id(dir_id)
                        .ok()
                        .flatten()
                        .and_then(|d| d.parent_id)
                        .map(Self::make_torrent_dir_ino)
                        .unwrap_or_else(|| Self::make_torrent_root_ino(torrent_id));
                    entries.push((parent_ino, 2, fuser::FileType::Directory, "..".to_string()));

                    let mut offset_counter = 3i64;

                    let sub_dirs = db_guard
                        .get_torrent_directories_by_parent(Some(dir_id), torrent_id)
                        .ok()?;
                    for dir in sub_dirs {
                        let sub_dir_ino = Self::make_torrent_dir_ino(dir.id);
                        cache_entries.push((
                            sub_dir_ino,
                            DataInode::TorrentDir {
                                torrent_id,
                                dir_id: dir.id,
                                name: dir.name.clone(),
                            },
                        ));
                        entries.push((
                            sub_dir_ino,
                            offset_counter,
                            fuser::FileType::Directory,
                            dir.name,
                        ));
                        offset_counter += 1;
                    }

                    let dir_files = db_guard.get_files_in_directory(dir_id).ok()?;
                    for file in dir_files {
                        let file_ino = Self::make_torrent_file_ino(file.id);
                        cache_entries.push((
                            file_ino,
                            DataInode::TorrentFile {
                                torrent_id,
                                file_id: file.id,
                                name: file.name.clone(),
                                size: file.size,
                            },
                        ));
                        entries.push((
                            file_ino,
                            offset_counter,
                            fuser::FileType::RegularFile,
                            file.name,
                        ));
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

        Some(
            entries
                .into_iter()
                .filter(|(_, o, _, _)| *o > offset)
                .collect(),
        )
    }

    fn extract_source_path(&self, parent: u64) -> String {
        if parent == METADATA_INO {
            return String::new();
        }

        let full_path = self.get_full_path(parent);
        if let Some(stripped) = full_path.strip_prefix("metadata/") {
            stripped.to_string()
        } else {
            full_path
        }
    }

    fn process_torrent(&self, data: &[u8], source_path: &str, filename: &str) -> Result<(), i32> {
        let info = TorrentInfo::from_bytes(data.to_vec()).map_err(|e| {
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
                info!(
                    "Parsed torrent {} (no DB configured, skipping insert)",
                    metadata.name
                );
                return Ok(());
            }
        };

        let mut db_guard = db.lock().map_err(|_| {
            error!("Database lock poisoned");
            EIO
        })?;

        // Prepare files for atomic insert
        let files: Vec<FileEntry> = metadata
            .files
            .iter()
            .map(|f| FileEntry {
                path: f.path.clone(),
                size: f.size as i64,
            })
            .collect();

        // Use atomic insert to ensure torrent and files are inserted together
        let result = db_guard
            .insert_torrent_with_files(
                source_path,
                &metadata.name,
                filename,
                metadata.total_size as i64,
                &info_hash_hex,
                metadata.num_files as i64,
                &files,
            )
            .map_err(|e| {
                error!("Failed to insert torrent with files {}: {:?}", filename, e);
                EIO
            })?;

        match result {
            InsertTorrentResult::Inserted(torrent_id) => {
                db_guard.set_torrent_data(torrent_id, data).map_err(|e| {
                    error!("Failed to store torrent data for {}: {:?}", filename, e);
                    EIO
                })?;

                info!(
                    "Persisted torrent '{}' ({} files, {} bytes) from {}",
                    metadata.name,
                    metadata.num_files,
                    metadata.total_size,
                    if source_path.is_empty() {
                        "root"
                    } else {
                        source_path
                    }
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
            blocks: size.div_ceil(512),
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
                InodeData::File {
                    parent: p, name: n, ..
                } if *p == parent && n == name => {
                    return Some(*ino);
                }
                _ => {}
            }
        }
        None
    }

    fn read_torrent_file_data(
        &self,
        torrent_id: i64,
        file_id: i64,
        offset: usize,
        size: usize,
    ) -> Result<Vec<u8>, i32> {
        let db = self.get_db()?;
        let db_guard = db.lock().map_err(|_| {
            error!("Database lock poisoned");
            EIO
        })?;

        let torrent = db_guard
            .get_torrent_by_id(torrent_id)
            .map_err(|e| {
                error!("Failed to get torrent by id: {:?}", e);
                EIO
            })?
            .ok_or_else(|| {
                error!("Torrent not found: {}", torrent_id);
                ENOENT
            })?;

        let files = db_guard.get_files_by_torrent_id(torrent_id).map_err(|e| {
            error!("Failed to get files for torrent: {:?}", e);
            EIO
        })?;

        let _file = files.iter().find(|f| f.id == file_id).ok_or_else(|| {
            error!("File not found: {}", file_id);
            ENOENT
        })?;

        let file_index = files.iter().position(|f| f.id == file_id).ok_or_else(|| {
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
                    info!(
                        "Successfully read {} bytes from torrent file (torrent_id={}, file_id={})",
                        data.len(),
                        torrent_id,
                        file_id
                    );
                    Ok(data)
                }
                Err(e) => {
                    warn!(
                        "Failed to read from BitTorrent network: {}. \
                         The torrent may have no active peers/seeds. \
                         Check tracker health with `cat .stats`.",
                        e
                    );
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

        let torrent = db_guard
            .get_torrent_by_source_path(source_path)
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
        for data in self.inodes.values() {
            if let InodeData::File {
                name,
                data: file_data,
                ..
            } = data
            {
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

    fn generate_stats(&self) -> Vec<u8> {
        let mut output = String::new();

        // Header
        output.push_str("═══════════════════════════════════════════════════════════════\n");
        output.push_str("  torrentfs v0.1.0\n");
        output.push_str("═══════════════════════════════════════════════════════════════\n\n");

        // --- 概况 ---
        let now = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let uptime_secs = now.as_secs().saturating_sub(self.creation_time.as_secs());
        let uptime_h = uptime_secs / 3600;
        let uptime_m = (uptime_secs % 3600) / 60;
        let uptime_s = uptime_secs % 60;

        output.push_str("── 概况 ──\n");
        output.push_str(&format!(
            "  运行时间:    {}h {}m {}s\n",
            uptime_h, uptime_m, uptime_s
        ));
        output.push_str("  挂载点:      (dynamic)\n");

        // DB path
        let db_path = if self.db.is_some() {
            "(active)"
        } else {
            "(none)"
        };
        output.push_str(&format!("  数据库:      {}\n", db_path));

        // Cache info
        let (cache_total_size, cache_max_size, cache_dir_str) =
            if let Some(ref cm) = self.cache_manager {
                if let Ok(cm_guard) = cm.lock() {
                    (
                        cm_guard.current_size(),
                        cm_guard.max_cache_size(),
                        "(cache)",
                    )
                } else {
                    (0, 0, "(locked)")
                }
            } else {
                (0, 0, "(none)")
            };
        let cache_pct = if cache_max_size > 0 {
            (cache_total_size as f64 / cache_max_size as f64) * 100.0
        } else {
            0.0
        };
        output.push_str(&format!("  缓存目录:    {}\n", cache_dir_str));
        output.push_str(&format!(
            "  缓存总用量:  {} / {} ({:.1}%)\n",
            format_bytes(cache_total_size),
            format_bytes(cache_max_size),
            cache_pct
        ));

        // Session stats
        let session_stats = if let Some(ref dm) = self.download_manager {
            if let Ok(dm_guard) = dm.lock() {
                dm_guard.get_session_stats().ok()
            } else {
                None
            }
        } else {
            None
        };

        if let Some(ref ss) = session_stats {
            output.push_str(&format!("  监听地址:    0.0.0.0:6881\n"));
            output.push_str(&format!("  DHT 节点:    {}\n", ss.dht_nodes));
        } else {
            output.push_str("  监听地址:    (not available)\n");
            output.push_str("  DHT 节点:    —\n");
        }

        // --- 全局速率 ---
        output.push_str("\n── 全局速率 ──\n");
        if let Some(ref ss) = session_stats {
            output.push_str(&format!(
                "  下载速率:    {}/s\n",
                format_bytes(ss.download_rate as u64)
            ));
            output.push_str(&format!(
                "  上传速率:    {}/s\n",
                format_bytes(ss.upload_rate as u64)
            ));
            output.push_str(&format!(
                "  累计下载:    {}\n",
                format_bytes(ss.total_downloaded as u64)
            ));
            output.push_str(&format!(
                "  累计上传:    {}\n",
                format_bytes(ss.total_uploaded as u64)
            ));
        } else {
            output.push_str("  下载速率:    —\n");
            output.push_str("  上传速率:    —\n");
            output.push_str("  累计下载:    —\n");
            output.push_str("  累计上传:    —\n");
        }

        // --- 连接 ---
        output.push_str("\n── 连接 ──\n");
        if let Some(ref ss) = session_stats {
            output.push_str(&format!("  已连接 peers:   {}\n", ss.peers_connected));
            output.push_str(&format!(
                "  半开连接:        {}\n",
                ss.half_open_connections
            ));
            output.push_str("  累计尝试:      —\n");
        } else {
            output.push_str("  已连接 peers:   —\n");
            output.push_str("  半开连接:       —\n");
            output.push_str("  累计尝试:      —\n");
        }

        // --- 种子总览 ---
        output.push_str("\n── 种子总览 ──\n");
        let (pending, downloading, seeding, error, total_torrents) = if let Ok(db) = self.get_db() {
            if let Ok(db_guard) = db.lock() {
                db_guard
                    .get_torrent_counts_by_status()
                    .unwrap_or((0, 0, 0, 0, 0))
            } else {
                (0, 0, 0, 0, 0)
            }
        } else {
            (0, 0, 0, 0, 0)
        };

        // Count unique info_hashes
        let unique_info_hashes = if let Ok(db) = self.get_db() {
            if let Ok(db_guard) = db.lock() {
                if let Ok(torrents) = db_guard.get_all_torrents() {
                    let mut set: std::collections::HashSet<&str> = std::collections::HashSet::new();
                    for t in &torrents {
                        set.insert(t.info_hash.as_str());
                    }
                    set.len() as i64
                } else {
                    0
                }
            } else {
                0
            }
        } else {
            0
        };

        output.push_str(&format!(
            "  种子实例: {}    info_hash 去重: {}    等待: {}    下载: {}    做种: {}    错误: {}\n",
            total_torrents, unique_info_hashes, pending, downloading, seeding, error
        ));

        // --- 种子详情 ---
        output.push_str("\n── 种子详情 ──\n");

        if let Ok(db) = self.get_db() {
            if let Ok(db_guard) = db.lock() {
                if let Ok(torrents) = db_guard.get_all_torrents() {
                    let mut torrent_idx = 0usize;
                    for t in &torrents {
                        torrent_idx += 1;
                        output.push_str(&format!("  #{}  {}\n", torrent_idx, t.name));

                        let status_str = match t.status {
                            db::TorrentStatus::Pending => "等待",
                            db::TorrentStatus::Downloading => "下载",
                            db::TorrentStatus::Seeding => "做种",
                            db::TorrentStatus::Error => "错误",
                        };

                        // Try to get runtime status from DownloadManager
                        let (
                            dl_rate,
                            ul_rate,
                            num_peers,
                            num_seeds,
                            progress,
                            total_size,
                            total_done,
                            total_upload,
                            total_download,
                        ) = if let Some(ref dm) = self.download_manager {
                            if let Ok(dm_guard) = dm.lock() {
                                let handles = dm_guard.get_all_handles();
                                if let Some((_, handle)) =
                                    handles.iter().find(|(ih, _)| ih == &t.info_hash)
                                {
                                    if let Ok(h) = handle.lock() {
                                        if let Ok(status) = h.status() {
                                            (
                                                status.download_rate,
                                                status.upload_rate,
                                                status.num_peers,
                                                status.num_seeds,
                                                status.progress,
                                                status.total,
                                                status.total_done,
                                                status.total_upload,
                                                status.total_download,
                                            )
                                        } else {
                                            (0, 0, 0, 0, 0.0, 0, 0, 0, 0)
                                        }
                                    } else {
                                        (0, 0, 0, 0, 0.0, 0, 0, 0, 0)
                                    }
                                } else {
                                    (0, 0, 0, 0, 0.0, 0, 0, 0, 0)
                                }
                            } else {
                                (0, 0, 0, 0, 0.0, 0, 0, 0, 0)
                            }
                        } else {
                            (0, 0, 0, 0, 0.0, 0, 0, 0, 0)
                        };

                        let prog_pct = if total_size > 0 {
                            progress * 100.0
                        } else {
                            0.0
                        };

                        output.push_str(&format!(
                            "      状态: {}     进度: {:.1}%    大小: {}\n",
                            status_str,
                            prog_pct,
                            format_bytes(t.total_size as u64)
                        ));

                        let share = if total_download > 0 {
                            format!("{:.2}", total_upload as f64 / total_download as f64)
                        } else {
                            "—".to_string()
                        };

                        output.push_str(&format!(
                            "      下载: {}   上传: {}   分享率: {}\n",
                            format_bytes(total_done),
                            format_bytes(total_upload as u64),
                            share
                        ));

                        output.push_str(&format!(
                            "      速度: ↓ {}/s   ↑ {}/s  peers: {}   seeds: {}\n",
                            format_bytes(dl_rate as u64),
                            format_bytes(ul_rate as u64),
                            num_peers,
                            num_seeds
                        ));

                        // Health warning: 0 peers and 0 seeds
                        if num_peers == 0 && num_seeds == 0 {
                            output.push_str(
                                "      ⚠ 健康警告: 0 peers / 0 seeds — tracker 可能无响应或种子无活跃节点\n",
                            );
                        }

                        output.push_str(&format!(
                            "      source_path: \"{}\"                       info_hash: {}...\n",
                            if t.source_path.is_empty() {
                                ""
                            } else {
                                &t.source_path
                            },
                            &t.info_hash[..std::cmp::min(10, t.info_hash.len())]
                        ));

                        output.push('\n');
                    }
                }
            }
        }

        // --- 缓存详情 ---
        output.push_str("── 缓存详情 ──\n");
        let (global_hits, global_misses) = if let Some(ref cm) = self.cache_manager {
            if let Ok(cm_guard) = cm.lock() {
                (cm_guard.hit_count, cm_guard.miss_count)
            } else {
                (0, 0)
            }
        } else {
            (0, 0)
        };
        let global_total = global_hits + global_misses;
        let hit_rate = if global_total > 0 {
            (global_hits as f64 / global_total as f64) * 100.0
        } else {
            0.0
        };
        output.push_str(&format!(
            "  全局命中: {}    全局未命中: {}    全局命中率: {:.1}%    淘汰: 0\n\n",
            format_num(global_hits),
            format_num(global_misses),
            hit_rate
        ));

        // Per-info_hash cache stats
        if let Some(ref cm) = self.cache_manager {
            if let Ok(cm_guard) = cm.lock() {
                let infohashes = cm_guard.get_all_infohashes();
                for ih in &infohashes {
                    let stats = cm_guard.get_cache_stats_by_infohash(ih);
                    output.push_str(&format!(
                        "  [info_hash] {}...\n",
                        &ih[..std::cmp::min(10, ih.len())]
                    ));
                    output.push_str(&format!(
                        "    缓存:  {} pieces    {}    命中: {}    淘汰: 0\n",
                        stats.piece_count,
                        format_bytes(stats.total_size),
                        format_num(stats.hit_count)
                    ));
                    output.push_str("    种子:\n");

                    // Query DB for associated torrents
                    if let Ok(db) = self.get_db() {
                        if let Ok(db_guard) = db.lock() {
                            if let Ok(associated) = db_guard.get_torrents_by_infohash(ih) {
                                for (torrent_id, name, _filename, source_path) in &associated {
                                    // Find the torrent index from the all_torrents list
                                    let idx_str = if let Ok(all) = db_guard.get_all_torrents() {
                                        all.iter()
                                            .position(|t| t.id == *torrent_id)
                                            .map(|p| format!("#{}", p + 1))
                                            .unwrap_or_else(|| "#?".to_string())
                                    } else {
                                        "#?".to_string()
                                    };
                                    let sp = if source_path.is_empty() {
                                        ""
                                    } else {
                                        source_path
                                    };
                                    output.push_str(&format!(
                                        "      {}  {:<40}  source_path: \"{}\"\n",
                                        idx_str, name, sp
                                    ));
                                }
                            }
                        }
                    }
                    output.push('\n');
                }
            }
        }

        // --- 性能 ---
        output.push_str("── 性能 ──\n");
        output.push_str("  tick 间隔:      1000 ms\n");
        // Memory RSS - approximate
        output.push_str("  内存 (RSS):     —\n");

        output.push_str("\n═══════════════════════════════════════════════════════════════\n");

        output.into_bytes()
    }
}

impl Filesystem for TorrentFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = name.to_string_lossy();

        if parent == ROOT_INO {
            match name_str.as_ref() {
                "metadata" => {
                    reply.entry(
                        &Duration::from_secs(1),
                        &self.attr_for_dir(METADATA_INO, true),
                        0,
                    );
                }
                "data" => {
                    reply.entry(
                        &Duration::from_secs(1),
                        &self.attr_for_dir(DATA_INO, false),
                        0,
                    );
                }
                ".stats" => {
                    let stats_size = self.generate_stats().len() as u64;
                    reply.entry(
                        &Duration::from_secs(1),
                        &self.attr_for_file(STATS_INO, stats_size),
                        0,
                    );
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
                        reply.entry(
                            &Duration::from_secs(1),
                            &self.attr_for_dir(child_ino, true),
                            0,
                        );
                    }
                    InodeData::File {
                        data: file_data, ..
                    } => {
                        reply.entry(
                            &Duration::from_secs(1),
                            &self.attr_for_file(child_ino, file_data.len() as u64),
                            0,
                        );
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
            STATS_INO => {
                let stats_size = self.generate_stats().len() as u64;
                reply.attr(
                    &Duration::from_secs(1),
                    &self.attr_for_file(ino, stats_size),
                );
            }
            _ => {
                if Self::is_data_ino(ino) {
                    if let Some(data_inode) = self.data_inodes.get(&ino) {
                        match data_inode {
                            DataInode::SourcePathDir { .. }
                            | DataInode::TorrentRoot { .. }
                            | DataInode::TorrentDir { .. } => {
                                reply.attr(&Duration::from_secs(1), &self.attr_for_dir(ino, false));
                            }
                            DataInode::TorrentFile { size, .. } => {
                                reply.attr(
                                    &Duration::from_secs(1),
                                    &self.attr_for_file(ino, *size as u64),
                                );
                            }
                        }
                        return;
                    }

                    let torrent_id = (ino - DATA_TORRENT_INO_BASE) as i64;
                    if (DATA_TORRENT_INO_BASE..DATA_DIR_INO_BASE).contains(&ino) {
                        if let Ok(db) = self.get_db() {
                            if let Ok(db_guard) = db.lock() {
                                if db_guard
                                    .get_torrent_by_id(torrent_id)
                                    .ok()
                                    .flatten()
                                    .is_some()
                                {
                                    reply.attr(
                                        &Duration::from_secs(1),
                                        &self.attr_for_dir(ino, false),
                                    );
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
                            reply.attr(
                                &Duration::from_secs(1),
                                &self.attr_for_dir(ino, self.is_metadata_child(ino)),
                            );
                        }
                        InodeData::File {
                            data: file_data, ..
                        } => {
                            reply.attr(
                                &Duration::from_secs(1),
                                &self.attr_for_file(ino, file_data.len() as u64),
                            );
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

        let mut entries: Vec<(u64, i64, fuser::FileType, &str)> =
            vec![(ino, 1, fuser::FileType::Directory, ".")];

        if ino == ROOT_INO {
            entries.push((ROOT_INO, 2, fuser::FileType::Directory, ".."));
            entries.push((METADATA_INO, 3, fuser::FileType::Directory, "metadata"));
            entries.push((DATA_INO, 4, fuser::FileType::Directory, "data"));
            entries.push((STATS_INO, 5, fuser::FileType::RegularFile, ".stats"));
        } else if let Some(InodeData::Directory { parent, .. }) = self.inodes.get(&ino) {
            entries.push((*parent, 2, fuser::FileType::Directory, ".."));

            let mut offset_counter = entries.len() as i64 + 1;
            for (child_ino, data) in &self.inodes {
                match data {
                    InodeData::Directory { parent, name } if *parent == ino && !name.is_empty() => {
                        entries.push((
                            *child_ino,
                            offset_counter,
                            fuser::FileType::Directory,
                            name.as_str(),
                        ));
                        offset_counter += 1;
                    }
                    InodeData::File { parent, name, .. } if *parent == ino => {
                        entries.push((
                            *child_ino,
                            offset_counter,
                            fuser::FileType::RegularFile,
                            name.as_str(),
                        ));
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
            STATS_INO => {
                let fh = NEXT_FH.fetch_add(1, Ordering::SeqCst);
                self.open_files.insert(fh, ino);
                reply.opened(fh, 0);
            }
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

    fn flush(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: fuser::ReplyEmpty,
    ) {
        // Validate torrent data synchronously during flush.
        // This is called during close() and its error propagates to userspace,
        // unlike release() which is asynchronous.
        if let Some(InodeData::File { data, name, .. }) = self.inodes.get(&ino) {
            if name.ends_with(".torrent") {
                // Reject zero-byte torrent files
                if data.is_empty() {
                    warn!("Zero-byte torrent file {} rejected", name);
                    reply.error(EINVAL);
                    return;
                }

                // Check size limit
                if data.len() > MAX_TORRENT_SIZE {
                    warn!(
                        "Torrent file {} exceeds size limit ({} bytes)",
                        name,
                        data.len()
                    );
                    reply.error(EFBIG);
                    return;
                }

                // Validate torrent by parsing it
                match TorrentInfo::from_bytes(data.clone()) {
                    Ok(_) => {
                        info!("Torrent {} validated successfully", name);
                    }
                    Err(e) => {
                        warn!("Invalid torrent file {}: {:?}", name, e);
                        reply.error(EINVAL);
                        return;
                    }
                }
            }
        }
        reply.ok();
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
                if name.ends_with(".torrent") {
                    if data.is_empty() {
                        // Zero-byte torrent: flush already returned EINVAL, clean up ghost inode
                        warn!("Zero-byte torrent file {} removed", name);
                        self.inodes.remove(&ino);
                        reply.ok();
                        return;
                    }

                    // Size check is already done in flush, but keep for safety
                    if data.len() > MAX_TORRENT_SIZE {
                        self.inodes.remove(&ino);
                        reply.ok();
                        return;
                    }

                    // Validate torrent - if invalid, clean up and exit
                    // (flush already returned EINVAL to userspace)
                    if TorrentInfo::from_bytes(data.clone()).is_err() {
                        warn!("Torrent {} invalid, removing inode", name);
                        self.inodes.remove(&ino);
                        reply.ok();
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
                            // DB insert failed - keep inode for debugging
                            error!("Failed to process torrent {}: {}", name, e);
                            let mut processing = self.processing_torrents.lock().unwrap();
                            processing.remove(&source_path);
                            // Don't return error - flush already succeeded
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
            STATS_INO => {
                let offset = offset as usize;
                let stats = self.generate_stats();
                if offset >= stats.len() {
                    reply.data(&[]);
                } else {
                    let end = std::cmp::min(offset + size as usize, stats.len());
                    reply.data(&stats[offset..end]);
                }
            }
            _ => {
                if Self::is_data_ino(ino) {
                    if let Some(DataInode::TorrentFile {
                        torrent_id,
                        file_id,
                        name,
                        size: file_size,
                    }) = self.data_inodes.get(&ino)
                    {
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

                        match self.read_torrent_file_data(
                            *torrent_id,
                            *file_id,
                            offset,
                            result_size,
                        ) {
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

        self.inodes.insert(
            new_ino,
            InodeData::File {
                parent,
                name: name_str.to_string(),
                data: Vec::new(),
            },
        );

        info!(
            "Created file {} with inode {} in {}",
            name_str,
            new_ino,
            self.get_full_path(parent)
        );
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

        self.inodes.insert(
            new_ino,
            InodeData::File {
                parent,
                name: name_str.to_string(),
                data: Vec::new(),
            },
        );

        let fh = NEXT_FH.fetch_add(1, Ordering::SeqCst);
        self.open_files.insert(fh, new_ino);

        info!(
            "Created file {} with inode {} in {}",
            name_str,
            new_ino,
            self.get_full_path(parent)
        );
        reply.created(
            &Duration::from_secs(1),
            &self.attr_for_file(new_ino, 0),
            0,
            fh,
            0,
        );
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
            if let InodeData::File {
                data: ref mut file_data,
                name,
                ..
            } = inode_data
            {
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
        self.inodes.insert(
            new_ino,
            InodeData::Directory {
                parent,
                name: name_str.to_string(),
            },
        );

        // Persist the directory to the database so it appears in data/
        let source_path = if parent == METADATA_INO {
            name_str.to_string()
        } else {
            let parent_source_path = self.extract_source_path(parent);
            if parent_source_path.is_empty() {
                name_str.to_string()
            } else {
                format!("{}/{}", parent_source_path, name_str)
            }
        };

        if let Some(db) = &self.db {
            if let Ok(mut db_guard) = db.lock() {
                if let Err(e) = db_guard.ensure_metadata_directories(&source_path) {
                    warn!("Failed to persist directory to database: {:?}", e);
                }
            }
        }

        info!(
            "Created directory {} with inode {} in {}",
            name_str,
            new_ino,
            self.get_full_path(parent)
        );
        reply.entry(
            &Duration::from_secs(1),
            &self.attr_for_dir(new_ino, true),
            0,
        );
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
                    reply.attr(
                        &Duration::from_secs(1),
                        &self.attr_for_dir(ino, self.is_metadata_child(ino)),
                    );
                }
                InodeData::File {
                    data: file_data, ..
                } => {
                    reply.attr(
                        &Duration::from_secs(1),
                        &self.attr_for_file(ino, file_data.len() as u64),
                    );
                }
            }
        } else {
            reply.error(ENOENT);
        }
    }

    fn rename(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: fuser::ReplyEmpty,
    ) {
        let name_str = name.to_string_lossy();
        let newname_str = newname.to_string_lossy();

        // Only allow renaming within metadata/ directory
        if !self.is_metadata_child(parent) || !self.is_metadata_child(newparent) {
            error!("Rename only allowed within metadata/ directory");
            reply.error(EACCES);
            return;
        }

        // Find the source inode
        let source_ino = match self.find_child_by_name(parent, &name_str) {
            Some(ino) => ino,
            None => {
                error!("Source file not found: {}", name_str);
                reply.error(ENOENT);
                return;
            }
        };

        // Check if target already exists
        if self.find_child_by_name(newparent, &newname_str).is_some() {
            error!("Target file already exists: {}", newname_str);
            reply.error(EEXIST);
            return;
        }

        // Check if the source is a directory or file
        let is_directory = matches!(
            self.inodes.get(&source_ino),
            Some(InodeData::Directory { .. })
        );

        if is_directory {
            // --- Directory rename ---
            let old_source_path = self.extract_source_path(source_ino);
            let new_source_path = if newparent == METADATA_INO {
                newname_str.to_string()
            } else {
                let parent_path = self.extract_source_path(newparent);
                if parent_path.is_empty() {
                    newname_str.to_string()
                } else {
                    format!("{}/{}", parent_path, newname_str)
                }
            };

            // Update the directory inode's name
            self.inodes.insert(
                source_ino,
                InodeData::Directory {
                    parent: newparent,
                    name: newname_str.to_string(),
                },
            );

            // Update all descendant inodes that reference the old path prefix
            let old_prefix = format!("{}/", old_source_path);
            for (_, data) in self.inodes.iter_mut() {
                match data {
                    InodeData::Directory { name, .. } => {
                        // Directory names are just the leaf name, no path update needed
                        let _ = name;
                    }
                    InodeData::File { name, .. } => {
                        // File names are just the leaf name, no path update needed
                        let _ = name;
                    }
                }
            }

            // Update data_inodes cache: rename SourcePathDir entries
            let new_prefix = format!("{}/", new_source_path);
            for (_, data_inode) in self.data_inodes.iter_mut() {
                match data_inode {
                    DataInode::SourcePathDir { path } => {
                        if path == &old_source_path {
                            *path = new_source_path.clone();
                        } else if path.starts_with(&old_prefix) {
                            *path = format!("{}{}", new_prefix, &path[old_prefix.len()..]);
                        }
                    }
                    DataInode::TorrentRoot { source_path, .. } => {
                        if source_path == &old_source_path {
                            *source_path = new_source_path.clone();
                        } else if source_path.starts_with(&old_prefix) {
                            *source_path =
                                format!("{}{}", new_prefix, &source_path[old_prefix.len()..]);
                        }
                    }
                    _ => {}
                }
            }

            // Persist the directory rename to the database
            if let Some(db) = &self.db {
                match db.lock() {
                    Ok(mut db_guard) => {
                        if let Err(e) = db_guard.rename_metadata_directory(
                            &old_source_path,
                            &newname_str,
                            &new_source_path,
                        ) {
                            error!("Failed to rename metadata directory in database: {:?}", e);
                            reply.error(EIO);
                            return;
                        }
                        info!(
                            "Renamed metadata directory '{}' to '{}' (source_path: '{}' -> '{}')",
                            name_str, newname_str, old_source_path, new_source_path
                        );
                    }
                    Err(_) => {
                        error!("Database lock poisoned during directory rename");
                        reply.error(EIO);
                        return;
                    }
                }
            } else {
                info!(
                    "Renamed directory '{}' to '{}' (no database)",
                    name_str, newname_str
                );
            }

            reply.ok();
        } else {
            // --- File rename (.torrent files only) ---
            if !name_str.ends_with(".torrent") || !newname_str.ends_with(".torrent") {
                error!("Rename only allowed for .torrent files or directories");
                reply.error(EACCES);
                return;
            }

            // Get the source file data
            let (file_data, old_name) = match self.inodes.get(&source_ino) {
                Some(InodeData::File { data, name, .. }) => (data.clone(), name.clone()),
                None => {
                    error!("Source inode not found: {}", source_ino);
                    reply.error(ENOENT);
                    return;
                }
                _ => unreachable!(),
            };

            // Update the inode with the new name
            self.inodes.insert(
                source_ino,
                InodeData::File {
                    parent: newparent,
                    name: newname_str.to_string(),
                    data: file_data,
                },
            );

            // Update the database if available
            if let Some(db) = &self.db {
                let old_source_path = self.extract_source_path(parent);
                let new_source_path = self.extract_source_path(newparent);

                match db.lock() {
                    Ok(mut db_guard) => {
                        match db_guard
                            .get_torrent_by_filename_and_source_path(&old_name, &old_source_path)
                        {
                            Ok(Some(torrent)) => {
                                if let Err(e) = db_guard.rename_torrent(
                                    torrent.id,
                                    &torrent.name,
                                    &newname_str,
                                    &new_source_path,
                                ) {
                                    error!("Failed to rename torrent in database: {:?}", e);
                                    reply.error(EIO);
                                    return;
                                }
                                info!(
                                    "Renamed torrent '{}' to '{}' (id={}, source_path: '{}' -> '{}', name preserved: '{}')",
                                    old_name, newname_str, torrent.id, old_source_path, new_source_path, torrent.name
                                );
                            }
                            Ok(None) => {
                                info!(
                                    "Renamed file '{}' to '{}' (not yet in database)",
                                    old_name, newname_str
                                );
                            }
                            Err(e) => {
                                error!("Database error during rename: {:?}", e);
                                reply.error(EIO);
                                return;
                            }
                        }
                    }
                    Err(_) => {
                        error!("Database lock poisoned during rename");
                        reply.error(EIO);
                        return;
                    }
                }
            } else {
                info!(
                    "Renamed file '{}' to '{}' (no database)",
                    old_name, newname_str
                );
            }

            reply.ok();
        }
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        let name_str = name.to_string_lossy();

        // Only allow deleting files in metadata/ directory
        if !self.is_metadata_child(parent) {
            error!("Unlink only allowed within metadata/ directory");
            reply.error(EACCES);
            return;
        }

        // Only allow deleting torrent files (.torrent extension)
        if !name_str.ends_with(".torrent") {
            error!("Unlink only allowed for .torrent files");
            reply.error(EACCES);
            return;
        }

        // Find the inode
        let ino = match self.find_child_by_name(parent, &name_str) {
            Some(ino) => ino,
            None => {
                error!("File not found: {}", name_str);
                reply.error(ENOENT);
                return;
            }
        };

        // Verify it's a file, not a directory
        match self.inodes.get(&ino) {
            Some(InodeData::File {
                name,
                parent: file_parent,
                ..
            }) => {
                let filename = name.clone();
                let source_path = self.extract_source_path(*file_parent);

                // Delete from database FIRST (before modifying in-memory state)
                // This ensures atomicity: if DB delete fails, the inode remains
                if let Some(db) = &self.db {
                    match db.lock() {
                        Ok(mut db_guard) => {
                            // Find the torrent by filename and source_path
                            match db_guard
                                .get_torrent_by_filename_and_source_path(&filename, &source_path)
                            {
                                Ok(Some(torrent)) => {
                                    let torrent_id = torrent.id;

                                    // Delete from database (cascade will delete files and directories)
                                    if let Err(e) = db_guard.delete_torrent(torrent_id) {
                                        error!("Failed to delete torrent from database: {:?}", e);
                                        reply.error(EIO);
                                        return;
                                    }

                                    // DB delete succeeded - now safe to modify in-memory state

                                    // Remove the inode
                                    self.inodes.remove(&ino);

                                    // Close any open file handles for this inode
                                    self.open_files.retain(|_, &mut open_ino| open_ino != ino);

                                    // Clean up data_inodes cache for this torrent
                                    self.data_inodes.retain(|_, data_inode| match data_inode {
                                        DataInode::TorrentRoot {
                                            torrent_id: tid, ..
                                        } => *tid != torrent_id,
                                        DataInode::TorrentDir {
                                            torrent_id: tid, ..
                                        } => *tid != torrent_id,
                                        DataInode::TorrentFile {
                                            torrent_id: tid, ..
                                        } => *tid != torrent_id,
                                        _ => true,
                                    });

                                    // Remove from processing_torrents if present
                                    let mut processing = self.processing_torrents.lock().unwrap();
                                    processing.remove(&source_path);
                                    drop(processing);

                                    // Clear torrent data cache
                                    let mut cache = self.torrent_data_cache.lock().unwrap();
                                    cache.remove(&source_path);
                                    drop(cache);

                                    info!(
                                        "Deleted torrent '{}' (id={}, source_path='{}')",
                                        filename, torrent_id, source_path
                                    );
                                }
                                Ok(None) => {
                                    // Torrent not in database yet (not processed)
                                    // Safe to remove inode since there's no DB record
                                    self.inodes.remove(&ino);
                                    self.open_files.retain(|_, &mut open_ino| open_ino != ino);
                                    info!("Deleted file '{}' (not yet in database)", filename);
                                }
                                Err(e) => {
                                    error!("Database error during unlink: {:?}", e);
                                    reply.error(EIO);
                                    return;
                                }
                            }
                        }
                        Err(_) => {
                            error!("Database lock poisoned during unlink");
                            reply.error(EIO);
                            return;
                        }
                    }
                } else {
                    // No database - safe to remove inode directly
                    self.inodes.remove(&ino);
                    self.open_files.retain(|_, &mut open_ino| open_ino != ino);
                    info!("Deleted file '{}' (no database)", filename);
                }

                reply.ok();
            }
            Some(InodeData::Directory { .. }) => {
                error!("Cannot unlink directory: {}", name_str);
                reply.error(EISDIR);
            }
            None => {
                error!("Inode not found: {}", ino);
                reply.error(ENOENT);
            }
        }
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        let name_str = name.to_string_lossy();

        // Only allow deleting directories in metadata/ directory
        if !self.is_metadata_child(parent) {
            error!("Rmdir only allowed within metadata/ directory");
            reply.error(EACCES);
            return;
        }

        // Find the inode
        let ino = match self.find_child_by_name(parent, &name_str) {
            Some(ino) => ino,
            None => {
                error!("Directory not found: {}", name_str);
                reply.error(ENOENT);
                return;
            }
        };

        // Verify it's a directory
        match self.inodes.get(&ino) {
            Some(InodeData::Directory { .. }) => {
                // Check if directory is empty (no children)
                let has_children = self.inodes.iter().any(|(_, data)| match data {
                    InodeData::Directory { parent: p, .. } if *p == ino => true,
                    InodeData::File { parent: p, .. } if *p == ino => true,
                    _ => false,
                });

                if has_children {
                    error!("Directory not empty: {}", name_str);
                    reply.error(ENOTEMPTY);
                    return;
                }

                // Get the source_path for this directory before removing the inode
                let source_path = self.extract_source_path(ino);

                // Remove the inode
                self.inodes.remove(&ino);

                // Delete from database and clean up data_inodes cache
                if let Some(db) = &self.db {
                    match db.lock() {
                        Ok(mut db_guard) => {
                            // Delete the directory from metadata_directories table
                            if let Err(e) = db_guard.delete_metadata_directory(&source_path) {
                                warn!("Failed to delete metadata directory from database: {:?}", e);
                            }

                            // Clean up data_inodes cache for this directory and any child paths
                            self.data_inodes.retain(|_, data_inode| {
                                match data_inode {
                                    // Remove SourcePathDir entries that match or are children of the deleted path
                                    DataInode::SourcePathDir { path } => {
                                        if path == &source_path {
                                            false
                                        } else if source_path.is_empty() {
                                            // If source_path is empty, don't remove anything else
                                            true
                                        } else {
                                            // Check if this path is a child of the deleted directory
                                            !path.starts_with(&format!("{}/", source_path))
                                        }
                                    }
                                    _ => true,
                                }
                            });

                            info!(
                                "Deleted directory '{}' (source_path='{}')",
                                name_str, source_path
                            );
                        }
                        Err(_) => {
                            error!("Database lock poisoned during rmdir");
                            reply.error(EIO);
                            return;
                        }
                    }
                } else {
                    // No database - just clean up data_inodes cache
                    self.data_inodes.retain(|_, data_inode| match data_inode {
                        DataInode::SourcePathDir { path } => {
                            if path == &source_path {
                                false
                            } else if source_path.is_empty() {
                                true
                            } else {
                                !path.starts_with(&format!("{}/", source_path))
                            }
                        }
                        _ => true,
                    });

                    info!(
                        "Deleted directory '{}' (source_path='{}', no database)",
                        name_str, source_path
                    );
                }

                reply.ok();
            }
            Some(InodeData::File { .. }) => {
                error!("Cannot rmdir file: {}", name_str);
                reply.error(ENOTDIR);
            }
            None => {
                error!("Inode not found: {}", ino);
                reply.error(ENOENT);
            }
        }
    }
}

/// Format bytes into human-readable form.
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Format a number with thousand separators.
fn format_num(n: u64) -> String {
    if n == 0 {
        return "0".to_string();
    }
    let mut s = String::new();
    let mut remaining = n;
    let mut count = 0;
    while remaining > 0 {
        if count > 0 && count % 3 == 0 {
            s.push(',');
        }
        s.push(((remaining % 10) as u8 + b'0') as char);
        remaining /= 10;
        count += 1;
    }
    s.chars().rev().collect()
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

    let subscriber = FmtSubscriber::builder().with_max_level(log_level).finish();
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");

    let args = Args::parse();

    // Load configuration from TOML file if provided
    let config = match &args.config {
        Some(config_path) => match TorrentfsConfig::from_file(config_path) {
            Ok(cfg) => {
                info!("Loaded configuration from {:?}", config_path);
                cfg
            }
            Err(e) => {
                error!("Failed to load config from {:?}: {}", config_path, e);
                std::process::exit(1);
            }
        },
        None => TorrentfsConfig::default_config(),
    };

    if !args.mountpoint.exists() {
        std::fs::create_dir_all(&args.mountpoint).expect("Failed to create mountpoint");
    }

    let cache_path = args.cache.clone().unwrap_or_else(|| {
        dirs::data_local_dir()
            .unwrap_or_else(|| PathBuf::from("/tmp"))
            .join("torrentfs/cache")
    });

    if !cache_path.exists() {
        if let Err(e) = std::fs::create_dir_all(&cache_path) {
            warn!("Failed to create cache directory {:?}: {:?}", cache_path, e);
        }
    }

    let db_path = if let Some(db_path) = &args.db {
        db_path.clone()
    } else {
        dirs::data_local_dir()
            .unwrap_or_else(|| PathBuf::from("/tmp"))
            .join("torrentfs/db/metadata.db")
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
                warn!(
                    "Failed to open database at {:?}: {:?}, running without persistence",
                    db_path, e
                );
                None
            }
        };

        let fs = match db {
            Some(d) => TorrentFs::new_with_db_and_cache(d, cache_path.clone(), &config),
            None => TorrentFs::new_with_cache_path(cache_path.clone(), &config),
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
            warn!(
                "Failed to open database at {:?}: {:?}, running without persistence",
                db_path, e
            );
            None
        }
    };

    let fs = match db {
        Some(d) => TorrentFs::new_with_db_and_cache(d, cache_path.clone(), &config),
        None => TorrentFs::new_with_cache_path(cache_path.clone(), &config),
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
