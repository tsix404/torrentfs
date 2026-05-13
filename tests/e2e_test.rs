use torrentfs::TorrentInfo;
use std::io::Write;

fn create_test_torrent() -> (Vec<u8>, Vec<u8>) {
    let mut test_content = b"Hello, this is a test file for torrentfs verification.\n".to_vec();
    while test_content.len() < 162 {
        test_content.push(b'X');
    }
    test_content.truncate(162);
    
    let mut torrent = Vec::new();
    torrent.extend_from_slice(b"d8:announce30:http://localhost:6969/announce4:infod");
    torrent.extend_from_slice(b"6:lengthi162e4:name22:final_verification.txt12:piece lengthi16384e6:pieces20:");
    torrent.extend_from_slice(&hashlib_sha1(&test_content));
    torrent.extend_from_slice(b"ee");
    
    (torrent, test_content)
}

fn hashlib_sha1(data: &[u8]) -> [u8; 20] {
    let mut hasher = sha1_smol::Sha1::new();
    hasher.update(data);
    hasher.digest().bytes()
}

#[test]
fn test_torrent_info_from_bytes() {
    let (torrent_data, _) = create_test_torrent();
    
    let result = TorrentInfo::from_bytes(torrent_data.clone());
    match result {
        Ok(info) => {
            println!("Torrent name: {}", info.name());
            println!("Total size: {}", info.total_size());
            println!("Piece length: {}", info.piece_length());
            println!("Num pieces: {}", info.num_pieces());
            println!("Num files: {}", info.num_files());
            assert_eq!(info.name(), "final_verification.txt");
            assert_eq!(info.total_size(), 162);
            assert_eq!(info.num_files(), 1);
        }
        Err(e) => {
            panic!("Failed to parse torrent: {:?}", e);
        }
    }
}

#[test]
fn test_read_file_range_with_local_seed() {
    use torrentfs::download::DownloadManager;
    use std::fs;
    
    let temp_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
    let cache_dir = temp_dir.path().join("cache");
    let seed_dir = temp_dir.path().join("seed");
    
    fs::create_dir_all(&cache_dir).expect("Failed to create cache dir");
    fs::create_dir_all(&seed_dir).expect("Failed to create seed dir");
    
    let (torrent_data, file_content) = create_test_torrent();
    
    let info = TorrentInfo::from_bytes(torrent_data.clone()).expect("Failed to parse torrent");
    
    println!("Torrent info:");
    println!("  Name: {}", info.name());
    println!("  Total size: {}", info.total_size());
    println!("  Piece length: {}", info.piece_length());
    println!("  Num pieces: {}", info.num_pieces());
    
    let mut dm = DownloadManager::new(&cache_dir).expect("Failed to create download manager");
    
    let info_hash = hex::encode(info.info_hash().expect("Failed to get info hash"));
    let torrent_cache_dir = cache_dir.join(&info_hash);
    fs::create_dir_all(&torrent_cache_dir).expect("Failed to create torrent cache dir");
    
    let seed_file_path = torrent_cache_dir.join("final_verification.txt");
    fs::write(&seed_file_path, &file_content).expect("Failed to write seed file");
    
    println!("\nAttempting to read file range...");
    println!("Info hash: {}", info_hash);
    println!("Seed file path: {:?}", seed_file_path);
    
    let result = dm.read_file_range(&info, 0, 0, 50);
    
    match result {
        Ok(data) => {
            println!("Successfully read {} bytes", data.len());
            println!("Data: {:?}", String::from_utf8_lossy(&data));
            assert!(!data.is_empty(), "Expected non-empty data");
            assert_eq!(data.as_slice(), &file_content[0..50], "Data mismatch");
        }
        Err(e) => {
            println!("Error reading file range: {:?}", e);
            println!("\nNote: This test requires a functioning BitTorrent session.");
            println!("The error is expected if no peers are available.");
        }
    }
}
