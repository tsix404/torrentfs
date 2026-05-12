use torrentfs::{TorrentInfo, TorrentError, MockTorrentFs, FsError, FileType};

#[test]
fn test_parse_invalid_file_returns_error() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    use std::io::Write;
    write!(file, "this is not a valid torrent file").unwrap();
    
    let result = TorrentInfo::from_file(file.path());
    assert!(result.is_err());
    
    match result {
        Err(TorrentError::ParseError(_)) => (),
        Err(e) => panic!("Expected ParseError, got {:?}", e),
        Ok(_) => panic!("Expected error, got success"),
    }
}

#[test]
fn test_parse_nonexistent_file_returns_error() {
    let result = TorrentInfo::from_file("/nonexistent/path/to/file.torrent");
    assert!(result.is_err());
}

#[test]
fn test_parse_empty_file_returns_error() {
    let file = tempfile::NamedTempFile::new().unwrap();
    
    let result = TorrentInfo::from_file(file.path());
    assert!(result.is_err());
}

#[test]
fn test_mock_fs_initialization() {
    let fs = MockTorrentFs::new_in_memory();
    
    let root_attr = fs.getattr(1).unwrap();
    assert_eq!(root_attr.kind, FileType::Directory);
    
    let metadata_attr = fs.getattr(2).unwrap();
    assert_eq!(metadata_attr.kind, FileType::Directory);
    assert_eq!(metadata_attr.perm, 0o755);
    
    let data_attr = fs.getattr(3).unwrap();
    assert_eq!(data_attr.kind, FileType::Directory);
    assert_eq!(data_attr.perm, 0o555);
}

#[test]
fn test_mock_fs_root_directory_listing() {
    let mut fs = MockTorrentFs::new_in_memory();
    
    let entries = fs.readdir(1).unwrap();
    
    assert!(entries.iter().any(|e| e.name == "." && e.kind == FileType::Directory));
    assert!(entries.iter().any(|e| e.name == ".." && e.kind == FileType::Directory));
    assert!(entries.iter().any(|e| e.name == "metadata" && e.kind == FileType::Directory));
    assert!(entries.iter().any(|e| e.name == "data" && e.kind == FileType::Directory));
}

#[test]
fn test_mock_fs_metadata_directory_operations() {
    let mut fs = MockTorrentFs::new_in_memory();
    
    let dir_attr = fs.mkdir(2, "testdir").unwrap();
    assert_eq!(dir_attr.kind, FileType::Directory);
    
    let lookup_attr = fs.lookup(2, "testdir").unwrap();
    assert_eq!(lookup_attr.ino, dir_attr.ino);
    assert_eq!(lookup_attr.kind, FileType::Directory);
    
    let entries = fs.readdir(2).unwrap();
    assert!(entries.iter().any(|e| e.name == "testdir"));
}

#[test]
fn test_mock_fs_file_create_write_read() {
    let mut fs = MockTorrentFs::new_in_memory();
    
    let (attr, _fh) = fs.create(2, "test.torrent").unwrap();
    assert_eq!(attr.kind, FileType::RegularFile);
    assert_eq!(attr.size, 0);
    
    let test_data = b"test file data content";
    let written = fs.write(attr.ino, 0, test_data).unwrap();
    assert_eq!(written, test_data.len());
    
    let updated_attr = fs.getattr(attr.ino).unwrap();
    assert_eq!(updated_attr.size, test_data.len() as u64);
    
    let read_data = fs.read(attr.ino, 0, test_data.len()).unwrap();
    assert_eq!(read_data.as_slice(), test_data);
}

#[test]
fn test_mock_fs_file_offset_write() {
    let mut fs = MockTorrentFs::new_in_memory();
    
    let (attr, _fh) = fs.create(2, "test.torrent").unwrap();
    
    fs.write(attr.ino, 0, b"hello").unwrap();
    fs.write(attr.ino, 5, b" world").unwrap();
    
    let read_data = fs.read(attr.ino, 0, 11).unwrap();
    assert_eq!(read_data.as_slice(), b"hello world");
}

#[test]
fn test_mock_fs_file_read_with_offset() {
    let mut fs = MockTorrentFs::new_in_memory();
    
    let (attr, _fh) = fs.create(2, "test.torrent").unwrap();
    
    fs.write(attr.ino, 0, b"hello world").unwrap();
    
    let read_data = fs.read(attr.ino, 6, 5).unwrap();
    assert_eq!(read_data.as_slice(), b"world");
}

#[test]
fn test_mock_fs_permission_denied_for_non_torrent() {
    let mut fs = MockTorrentFs::new_in_memory();
    
    let result = fs.create(2, "test.txt");
    assert!(matches!(result, Err(FsError::PermissionDenied)));
    
    let result = fs.mknod(2, "test.bin");
    assert!(matches!(result, Err(FsError::PermissionDenied)));
}

#[test]
fn test_mock_fs_permission_denied_for_data_directory() {
    let mut fs = MockTorrentFs::new_in_memory();
    
    let result = fs.create(3, "test.torrent");
    assert!(matches!(result, Err(FsError::PermissionDenied)));
    
    let result = fs.mkdir(3, "testdir");
    assert!(matches!(result, Err(FsError::PermissionDenied)));
}

#[test]
fn test_mock_fs_file_exists_error() {
    let mut fs = MockTorrentFs::new_in_memory();
    
    fs.create(2, "test.torrent").unwrap();
    
    let result = fs.create(2, "test.torrent");
    assert!(matches!(result, Err(FsError::FileExists)));
    
    let result = fs.mknod(2, "test.torrent");
    assert!(matches!(result, Err(FsError::FileExists)));
}

#[test]
fn test_mock_fs_directory_exists_error() {
    let mut fs = MockTorrentFs::new_in_memory();
    
    fs.mkdir(2, "testdir").unwrap();
    
    let result = fs.mkdir(2, "testdir");
    assert!(matches!(result, Err(FsError::FileExists)));
}

#[test]
fn test_mock_fs_lookup_nonexistent() {
    let mut fs = MockTorrentFs::new_in_memory();
    
    let result = fs.lookup(1, "nonexistent");
    assert!(matches!(result, Err(FsError::NoSuchEntry)));
    
    let result = fs.lookup(2, "nonexistent.torrent");
    assert!(matches!(result, Err(FsError::NoSuchEntry)));
}

#[test]
fn test_mock_fs_getattr_nonexistent() {
    let fs = MockTorrentFs::new_in_memory();
    
    let result = fs.getattr(99999);
    assert!(matches!(result, Err(FsError::NoSuchEntry)));
}

#[test]
fn test_mock_fs_read_nonexistent() {
    let mut fs = MockTorrentFs::new_in_memory();
    
    let result = fs.read(99999, 0, 100);
    assert!(matches!(result, Err(FsError::NoSuchEntry)));
}

#[test]
fn test_mock_fs_write_nonexistent() {
    let mut fs = MockTorrentFs::new_in_memory();
    
    let result = fs.write(99999, 0, b"data");
    assert!(matches!(result, Err(FsError::NoSuchEntry)));
}

#[test]
fn test_mock_fs_write_to_directory() {
    let mut fs = MockTorrentFs::new_in_memory();
    
    let result = fs.write(2, 0, b"data");
    assert!(matches!(result, Err(FsError::IsDirectory)));
}

#[test]
fn test_mock_fs_read_directory() {
    let mut fs = MockTorrentFs::new_in_memory();
    
    let result = fs.read(2, 0, 100);
    assert!(matches!(result, Err(FsError::IsDirectory)));
}

#[test]
fn test_mock_fs_readdir_of_file() {
    let mut fs = MockTorrentFs::new_in_memory();
    
    let (attr, _fh) = fs.create(2, "test.torrent").unwrap();
    
    let result = fs.readdir(attr.ino);
    assert!(matches!(result, Err(FsError::NotDirectory)));
}

#[test]
fn test_mock_fs_nested_directories() {
    let mut fs = MockTorrentFs::new_in_memory();
    
    let dir1 = fs.mkdir(2, "dir1").unwrap();
    let dir2 = fs.mkdir(dir1.ino, "dir2").unwrap();
    let dir3 = fs.mkdir(dir2.ino, "dir3").unwrap();
    
    let (file_attr, _fh) = fs.create(dir3.ino, "deep.torrent").unwrap();
    
    let lookup = fs.lookup(dir3.ino, "deep.torrent").unwrap();
    assert_eq!(lookup.ino, file_attr.ino);
    
    let entries = fs.readdir(dir3.ino).unwrap();
    assert!(entries.iter().any(|e| e.name == "deep.torrent"));
}

#[test]
fn test_mock_fs_multiple_files_in_directory() {
    let mut fs = MockTorrentFs::new_in_memory();
    
    let (f1, _fh1) = fs.create(2, "file1.torrent").unwrap();
    let (f2, _fh2) = fs.create(2, "file2.torrent").unwrap();
    let (f3, _fh3) = fs.create(2, "file3.torrent").unwrap();
    
    assert_ne!(f1.ino, f2.ino);
    assert_ne!(f2.ino, f3.ino);
    assert_ne!(f1.ino, f3.ino);
    
    let entries = fs.readdir(2).unwrap();
    assert!(entries.iter().any(|e| e.name == "file1.torrent"));
    assert!(entries.iter().any(|e| e.name == "file2.torrent"));
    assert!(entries.iter().any(|e| e.name == "file3.torrent"));
}

#[test]
fn test_mock_fs_file_handle_tracking() {
    let mut fs = MockTorrentFs::new_in_memory();
    
    let (attr, fh1) = fs.create(2, "test.torrent").unwrap();
    let fh2 = fs.open(attr.ino).unwrap();
    
    assert_ne!(fh1, fh2);
}

#[test]
fn test_mock_fs_read_beyond_file_size() {
    let mut fs = MockTorrentFs::new_in_memory();
    
    let (attr, _fh) = fs.create(2, "test.torrent").unwrap();
    
    fs.write(attr.ino, 0, b"hello").unwrap();
    
    let read_data = fs.read(attr.ino, 10, 5).unwrap();
    assert!(read_data.is_empty());
}

#[test]
fn test_mock_fs_partial_read() {
    let mut fs = MockTorrentFs::new_in_memory();
    
    let (attr, _fh) = fs.create(2, "test.torrent").unwrap();
    
    fs.write(attr.ino, 0, b"hello world").unwrap();
    
    let read_data = fs.read(attr.ino, 0, 5).unwrap();
    assert_eq!(read_data.as_slice(), b"hello");
    
    let read_data = fs.read(attr.ino, 6, 5).unwrap();
    assert_eq!(read_data.as_slice(), b"world");
}

#[test]
fn test_mock_fs_write_with_gap() {
    let mut fs = MockTorrentFs::new_in_memory();
    
    let (attr, _fh) = fs.create(2, "test.torrent").unwrap();
    
    fs.write(attr.ino, 10, b"world").unwrap();
    
    let updated_attr = fs.getattr(attr.ino).unwrap();
    assert_eq!(updated_attr.size, 15);
    
    let read_data = fs.read(attr.ino, 0, 15).unwrap();
    assert_eq!(&read_data[0..10], &[0u8; 10]);
    assert_eq!(&read_data[10..15], b"world");
}

#[test]
fn test_mock_fs_data_directory_readonly() {
    let fs = MockTorrentFs::new_in_memory();
    
    let data_attr = fs.getattr(3).unwrap();
    assert_eq!(data_attr.perm, 0o555);
    
    let metadata_attr = fs.getattr(2).unwrap();
    assert_eq!(metadata_attr.perm, 0o755);
}

#[test]
fn test_mock_fs_concurrent_file_operations() {
    let mut fs = MockTorrentFs::new_in_memory();
    
    let dir1 = fs.mkdir(2, "dir1").unwrap();
    let dir2 = fs.mkdir(2, "dir2").unwrap();
    
    let (f1, _fh1) = fs.create(dir1.ino, "file1.torrent").unwrap();
    let (f2, _fh2) = fs.create(dir2.ino, "file2.torrent").unwrap();
    
    fs.write(f1.ino, 0, b"data1").unwrap();
    fs.write(f2.ino, 0, b"data2").unwrap();
    
    let read1 = fs.read(f1.ino, 0, 5).unwrap();
    let read2 = fs.read(f2.ino, 0, 5).unwrap();
    
    assert_eq!(read1.as_slice(), b"data1");
    assert_eq!(read2.as_slice(), b"data2");
}

#[test]
fn test_mock_fs_lookup_after_create() {
    let mut fs = MockTorrentFs::new_in_memory();
    
    let (attr, _fh) = fs.create(2, "test.torrent").unwrap();
    
    let lookup_attr = fs.lookup(2, "test.torrent").unwrap();
    assert_eq!(lookup_attr.ino, attr.ino);
    assert_eq!(lookup_attr.kind, FileType::RegularFile);
}

#[test]
fn test_mock_fs_directory_listing_after_operations() {
    let mut fs = MockTorrentFs::new_in_memory();
    
    let dir = fs.mkdir(2, "mydir").unwrap();
    let (_file, _fh) = fs.create(2, "file.torrent").unwrap();
    
    let entries = fs.readdir(2).unwrap();
    
    let dir_entry = entries.iter().find(|e| e.name == "mydir").unwrap();
    assert_eq!(dir_entry.kind, FileType::Directory);
    assert_eq!(dir_entry.ino, dir.ino);
    
    let file_entry = entries.iter().find(|e| e.name == "file.torrent").unwrap();
    assert_eq!(file_entry.kind, FileType::RegularFile);
}

#[test]
fn test_mock_fs_overwrite_data() {
    let mut fs = MockTorrentFs::new_in_memory();
    
    let (attr, _fh) = fs.create(2, "test.torrent").unwrap();
    
    fs.write(attr.ino, 0, b"original data").unwrap();
    fs.write(attr.ino, 0, b"new").unwrap();
    
    let read_data = fs.read(attr.ino, 0, 13).unwrap();
    assert_eq!(&read_data[0..3], b"new");
    assert_eq!(&read_data[3..13], b"ginal data");
}

#[test]
fn test_mock_fs_large_file_operations() {
    let mut fs = MockTorrentFs::new_in_memory();
    
    let (attr, _fh) = fs.create(2, "test.torrent").unwrap();
    
    let large_data = vec![0xABu8; 10000];
    fs.write(attr.ino, 0, &large_data).unwrap();
    
    let read_data = fs.read(attr.ino, 0, 10000).unwrap();
    assert_eq!(read_data.len(), 10000);
    assert!(read_data.iter().all(|&b| b == 0xAB));
}

#[test]
fn test_mock_fs_multiple_writes_same_file() {
    let mut fs = MockTorrentFs::new_in_memory();
    
    let (attr, _fh) = fs.create(2, "test.torrent").unwrap();
    
    fs.write(attr.ino, 0, b"part1").unwrap();
    fs.write(attr.ino, 5, b"part2").unwrap();
    fs.write(attr.ino, 10, b"part3").unwrap();
    
    let read_data = fs.read(attr.ino, 0, 15).unwrap();
    assert_eq!(read_data.as_slice(), b"part1part2part3");
}
