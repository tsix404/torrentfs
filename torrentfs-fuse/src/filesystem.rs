use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, ReplyOpen,
    ReplyWrite, Request,
};
use libc::{ENOENT, ENOSYS, ENOTDIR};
use std::ffi::OsStr;
use std::time::{Duration, UNIX_EPOCH};

const TTL: Duration = Duration::from_secs(1);

pub const INO_ROOT: u64 = 1;
pub const INO_METADATA: u64 = 2;
pub const INO_DATA: u64 = 3;

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

pub fn attr_for_ino(ino: u64) -> Option<FileAttr> {
    match ino {
        INO_ROOT | INO_METADATA | INO_DATA => Some(dir_attr(ino)),
        _ => None,
    }
}

pub struct TorrentFsFilesystem;

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
        } else {
            reply.error(ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match attr_for_ino(ino) {
            Some(attr) => reply.attr(&TTL, &attr),
            None => reply.error(ENOENT),
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if ino != INO_ROOT {
            reply.error(ENOTDIR);
            return;
        }

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

    fn write(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        _data: &[u8],
        _size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
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
}
