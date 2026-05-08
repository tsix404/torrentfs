//! Torrent parsing functionality.

use anyhow::{Result, bail};
use std::ffi::CStr;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

/// File entry in a torrent.
#[derive(Debug, Clone)]
pub struct FileEntry {
    /// Full file path (including subdirectories)
    pub path: String,
    /// File size in bytes
    pub size: u64,
    /// Byte offset of this file within the torrent
    pub offset: u64,
    /// Index of the first piece that contains data from this file
    pub first_piece: u32,
    /// Index of the last piece that contains data from this file
    pub last_piece: u32,
}

/// Tracker entry in a torrent.
#[derive(Debug, Clone)]
pub struct TrackerEntry {
    /// Tracker URL
    pub url: String,
    /// Tracker tier (0 for primary, higher for backup)
    pub tier: i32,
}

/// Cache entry for parsed torrent info.
struct CacheEntry {
    info: TorrentInfo,
}

/// Global cache for parsed torrent metadata.
static PARSE_CACHE: Mutex<Option<Arc<Mutex<HashMap<String, CacheEntry>>>>> = Mutex::new(None);

/// Gets or initializes the global parse cache.
fn get_cache() -> Arc<Mutex<HashMap<String, CacheEntry>>> {
    let mut cache_guard = PARSE_CACHE.lock().unwrap();
    if cache_guard.is_none() {
        *cache_guard = Some(Arc::new(Mutex::new(HashMap::new())));
    }
    cache_guard.as_ref().unwrap().clone()
}

/// Torrent information extracted from a .torrent file.
#[derive(Debug, Clone)]
pub struct TorrentInfo {
    /// Name of the torrent
    pub name: String,
    /// Info hash in hexadecimal format (40 characters)
    pub info_hash: String,
    /// Total size in bytes
    pub total_size: u64,
    /// Number of bytes per piece
    pub piece_size: u32,
    /// Number of files in the torrent
    pub file_count: u32,
    /// List of files in the torrent
    pub files: Vec<FileEntry>,
    /// List of trackers in the torrent
    pub trackers: Vec<TrackerEntry>,
    /// Torrent comment (optional)
    pub comment: Option<String>,
    /// Creator string (optional)
    pub created_by: Option<String>,
    /// Creation date as Unix timestamp (optional)
    pub creation_date: Option<u64>,
}

impl TorrentInfo {
    /// Creates a new TorrentInfo from FFI result.
    fn from_ffi(ffi_info: &libtorrent_sys::libtorrent_torrent_info_t) -> Result<Self> {
        if ffi_info.error_code != libtorrent_sys::libtorrent_error_t_LIBTORRENT_OK {
            let error_msg = if !ffi_info.error_message.is_null() {
                unsafe { CStr::from_ptr(ffi_info.error_message) }
                    .to_string_lossy()
                    .into_owned()
            } else {
                format!("Unknown error (code: {})", ffi_info.error_code)
            };
            bail!("Failed to parse torrent: {}", error_msg);
        }

        // Safety: FFI functions should have set these pointers if error_code is OK
        let name = unsafe {
            if ffi_info.name.is_null() {
                bail!("FFI returned null name pointer");
            }
            CStr::from_ptr(ffi_info.name)
                .to_string_lossy()
                .into_owned()
        };

        let info_hash = unsafe {
            if ffi_info.info_hash_hex.is_null() {
                bail!("FFI returned null info_hash_hex pointer");
            }
            CStr::from_ptr(ffi_info.info_hash_hex)
                .to_string_lossy()
                .into_owned()
        };

        // Validate info hash is 40 characters (SHA1 hex)
        if info_hash.len() != 40 {
            bail!("Invalid info hash length: expected 40 hex characters, got {}", info_hash.len());
        }

        let piece_size = ffi_info.piece_size;

        // Extract file list
        let mut files = Vec::new();
        if !ffi_info.files.is_null() && ffi_info.file_count > 0 {
            let files_slice = unsafe {
                std::slice::from_raw_parts(ffi_info.files, ffi_info.file_count as usize)
            };
            
            for file_entry in files_slice {
                let path = if !file_entry.path.is_null() {
                    unsafe { CStr::from_ptr(file_entry.path) }
                        .to_string_lossy()
                        .into_owned()
                } else {
                    String::new()
                };
                
                files.push(FileEntry {
                    path,
                    size: file_entry.size,
                    offset: file_entry.offset,
                    first_piece: file_entry.first_piece,
                    last_piece: file_entry.last_piece,
                });
            }
        }

        // Extract tracker list
        let mut trackers = Vec::new();
        if !ffi_info.trackers.is_null() && ffi_info.tracker_count > 0 {
            let trackers_slice = unsafe {
                std::slice::from_raw_parts(ffi_info.trackers, ffi_info.tracker_count as usize)
            };
            
            for tracker_entry in trackers_slice {
                let url = if !tracker_entry.url.is_null() {
                    unsafe { CStr::from_ptr(tracker_entry.url) }
                        .to_string_lossy()
                        .into_owned()
                } else {
                    String::new()
                };
                
                trackers.push(TrackerEntry {
                    url,
                    tier: tracker_entry.tier,
                });
            }
        }

        // Extract comment
        let comment = if !ffi_info.comment.is_null() {
            let c = unsafe { CStr::from_ptr(ffi_info.comment) }
                .to_string_lossy()
                .into_owned();
            if c.is_empty() { None } else { Some(c) }
        } else {
            None
        };

        // Extract created_by
        let created_by = if !ffi_info.created_by.is_null() {
            let c = unsafe { CStr::from_ptr(ffi_info.created_by) }
                .to_string_lossy()
                .into_owned();
            if c.is_empty() { None } else { Some(c) }
        } else {
            None
        };

        // Extract creation_date
        let creation_date = if ffi_info.creation_date > 0 {
            Some(ffi_info.creation_date)
        } else {
            None
        };

        Ok(TorrentInfo {
            name,
            info_hash,
            total_size: ffi_info.total_size,
            piece_size,
            file_count: ffi_info.file_count,
            files,
            trackers,
            comment,
            created_by,
            creation_date,
        })
    }

    /// Returns a list of files in the torrent.
    ///
    /// This is a convenience method that returns the file list.
    /// It handles subdirectory paths as they are provided by libtorrent.
    pub fn list_files(&self) -> Vec<FileEntry> {
        self.files.clone()
    }

    /// Returns a list of trackers in the torrent.
    ///
    /// Trackers are ordered by tier (primary trackers first).
    pub fn list_trackers(&self) -> Vec<TrackerEntry> {
        self.trackers.clone()
    }

    /// Returns primary trackers (tier 0).
    pub fn primary_trackers(&self) -> Vec<&str> {
        self.trackers
            .iter()
            .filter(|t| t.tier == 0)
            .map(|t| t.url.as_str())
            .collect()
    }

    /// Returns the number of pieces in the torrent.
    pub fn num_pieces(&self) -> u32 {
        if self.piece_size == 0 {
            return 0;
        }
        ((self.total_size + self.piece_size as u64 - 1) / self.piece_size as u64) as u32
    }
}

/// Returns a list of files in the torrent.
///
/// # Arguments
/// * `info` - Torrent information
///
/// # Returns
/// Vector of file entries with paths and sizes.
/// Handles subdirectory paths as they are provided by libtorrent.
pub fn list_files(info: &TorrentInfo) -> Vec<FileEntry> {
    info.files.clone()
}

/// Returns a list of trackers in the torrent.
///
/// # Arguments
/// * `info` - Torrent information
///
/// # Returns
/// Vector of tracker entries with URLs and tiers.
pub fn list_trackers(info: &TorrentInfo) -> Vec<TrackerEntry> {
    info.trackers.clone()
}

/// Parses torrent data from a buffer.
///
/// This function parses the bencoded torrent data and extracts metadata
/// including name, info hash, files, trackers, and other information.
///
/// # Arguments
/// * `data` - Buffer containing the .torrent file data
///
/// # Returns
/// * `Ok(TorrentInfo)` on success
/// * `Err` if parsing fails
///
/// # Example
/// ```no_run
/// use torrentfs_libtorrent::parse_torrent;
/// let data = std::fs::read("example.torrent").unwrap();
/// let info = parse_torrent(&data).unwrap();
/// println!("Name: {}", info.name);
/// println!("Info hash: {}", info.info_hash);
/// println!("Files: {:?}", info.files);
/// println!("Trackers: {:?}", info.trackers);
/// ```
pub fn parse_torrent(data: &[u8]) -> Result<TorrentInfo> {
    parse_torrent_impl(data, false)
}

/// Parses torrent data with caching enabled.
///
/// Subsequent calls with the same data will return cached results.
/// The cache is keyed by the info hash, not the raw data.
///
/// # Arguments
/// * `data` - Buffer containing the .torrent file data
///
/// # Returns
/// * `Ok(TorrentInfo)` on success (from cache or freshly parsed)
/// * `Err` if parsing fails
pub fn parse_torrent_cached(data: &[u8]) -> Result<TorrentInfo> {
    parse_torrent_impl(data, true)
}

/// Clears the parse cache.
///
/// This is useful for freeing memory or forcing re-parsing.
pub fn clear_parse_cache() {
    let cache = get_cache();
    cache.lock().unwrap().clear();
}

fn parse_torrent_impl(data: &[u8], use_cache: bool) -> Result<TorrentInfo> {
    if data.is_empty() {
        bail!("Empty torrent data");
    }

    // Check cache by computing a quick hash key from the raw data
    if use_cache {
        let data_hash = compute_data_hash(data);
        let cache = get_cache();
        let cache_guard = cache.lock().unwrap();
        if let Some(entry) = cache_guard.get(&data_hash) {
            return Ok(entry.info.clone());
        }
    }

    // Call FFI function (single parse)
    let ffi_info = unsafe {
        libtorrent_sys::libtorrent_parse_torrent(data.as_ptr(), data.len())
    };

    if ffi_info.is_null() {
        bail!("FFI returned null pointer");
    }

    // Convert FFI result to Rust struct
    let result = TorrentInfo::from_ffi(unsafe { &*ffi_info });

    // Always free the FFI structure
    unsafe {
        libtorrent_sys::libtorrent_free_torrent_info(ffi_info);
    }

    // Cache the result if enabled and successful
    if use_cache {
        if let Ok(ref info) = result {
            let data_hash = compute_data_hash(data);
            let cache = get_cache();
            cache.lock().unwrap().insert(
                data_hash,
                CacheEntry {
                    info: info.clone(),
                },
            );
        }
    }

    result
}

fn compute_data_hash(data: &[u8]) -> String {
    use std::fmt::Write;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    data.hash(&mut hasher);
    let hash = hasher.finish();
    let mut s = String::with_capacity(16);
    write!(s, "{:016x}", hash).unwrap();
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn test_torrent_dir() -> std::path::PathBuf {
        let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        manifest_dir.join("../test_data") // torrentfs-libtorrent/../test_data
    }

    fn first_torrent_file() -> Option<std::path::PathBuf> {
        let dir = test_torrent_dir();
        std::fs::read_dir(&dir).ok()?.filter_map(|e| {
            let e = e.ok()?;
            if e.file_name().to_string_lossy().ends_with(".torrent") {
                Some(e.path())
            } else {
                None
            }
        }).next()
    }

    #[test]
    fn test_parse_valid_torrent() {
        let test_file = first_torrent_file().expect("No .torrent file found in repo root");
        
        // Read the torrent file
        let data = fs::read(test_file).expect("Failed to read test torrent file");
        assert!(!data.is_empty(), "Torrent file should not be empty");
        
        // Parse the torrent
        let result = parse_torrent(&data);
        assert!(result.is_ok(), "Failed to parse valid torrent: {:?}", result.err());
        
        let info = result.unwrap();
        
        // Check fields
        assert!(!info.name.is_empty(), "Torrent name should not be empty");
        assert_eq!(info.info_hash.len(), 40, "Info hash should be 40 hex characters");
        assert!(info.total_size > 0, "Total size should be positive");
        assert!(info.file_count > 0, "File count should be positive");
        
        println!("Test passed!");
        println!("Name: {}", info.name);
        println!("Info hash: {}", info.info_hash);
        println!("Total size: {} bytes", info.total_size);
        println!("File count: {}", info.file_count);
        if let Some(ref comment) = info.comment {
            println!("Comment: {}", comment);
        }
        if let Some(ref created_by) = info.created_by {
            println!("Created by: {}", created_by);
        }
        if let Some(creation_date) = info.creation_date {
            println!("Creation date: {}", creation_date);
        }
        println!("Trackers: {:?}", info.trackers);
    }
    
    #[test]
    fn test_parse_invalid_data() {
        // Test with invalid data
        let invalid_data = b"not a valid torrent file";
        
        let result = parse_torrent(invalid_data);
        assert!(result.is_err(), "Should fail to parse invalid torrent data");
        
        println!("Invalid data test passed: {}", result.err().unwrap());
    }
    
    #[test]
    fn test_parse_empty_data() {
        // Test with empty data
        let empty_data = b"";
        
        let result = parse_torrent(empty_data);
        assert!(result.is_err(), "Should fail to parse empty data");
        
        println!("Empty data test passed: {}", result.err().unwrap());
    }

    #[test]
    fn test_file_list_functionality() {
        let test_file = first_torrent_file().expect("No .torrent file found in repo root");
        
        // Read the torrent file
        let data = fs::read(test_file).expect("Failed to read test torrent file");
        assert!(!data.is_empty(), "Torrent file should not be empty");
        
        // Parse the torrent
        let result = parse_torrent(&data);
        assert!(result.is_ok(), "Failed to parse valid torrent: {:?}", result.err());
        
        let info = result.unwrap();
        
        // Test list_files method
        let files_from_method = info.list_files();
        
        // Test standalone list_files function
        let files_from_function = list_files(&info);
        
        // Both should return the same file list
        assert_eq!(files_from_method.len(), files_from_function.len());
        
        // File count should match the number of files in the list
        assert_eq!(info.file_count as usize, info.files.len());
        assert_eq!(info.file_count as usize, files_from_method.len());
        
        // Verify piece_size is positive
        assert!(info.piece_size > 0, "Piece size should be positive");

        // Check that files have valid data
        for (i, file) in info.files.iter().enumerate() {
            assert!(!file.path.is_empty(), "File {} should have a non-empty path", i);
            assert!(file.size > 0, "File {} should have a positive size", i);

            // first_piece should be <= last_piece
            assert!(file.first_piece <= file.last_piece,
                "File {}: first_piece ({}) should be <= last_piece ({})", i, file.first_piece, file.last_piece);

            // Verify piece range is consistent with offset and size
            let piece_len = info.piece_size as u64;
            let expected_first = file.offset / piece_len;
            assert_eq!(file.first_piece as u64, expected_first,
                "File {}: first_piece mismatch", i);

            let expected_last = if file.size > 0 {
                (file.offset + file.size - 1) / piece_len
            } else {
                expected_first
            };
            assert_eq!(file.last_piece as u64, expected_last,
                "File {}: last_piece mismatch", i);
            
            // Check that file path is properly formatted (may contain subdirectories)
            println!("File {}: {} ({} bytes, offset={}, pieces={}-{})",
                i, file.path, file.size, file.offset, file.first_piece, file.last_piece);
        }
        
        println!("File list test passed!");
        println!("Total files: {}", info.file_count);
        println!("Total size: {} bytes", info.total_size);
        
        // Verify total size matches sum of file sizes
        let total_from_files: u64 = info.files.iter().map(|f| f.size).sum();
        assert_eq!(info.total_size, total_from_files, "Total size should match sum of file sizes");
    }

    #[test]
    fn test_tracker_list_functionality() {
        let test_file = first_torrent_file().expect("No .torrent file found in repo root");
        
        // Read the torrent file
        let data = fs::read(test_file).expect("Failed to read test torrent file");
        
        // Parse the torrent
        let info = parse_torrent(&data).expect("Failed to parse torrent");
        
        // Test list_trackers method
        let trackers = info.list_trackers();
        assert_eq!(trackers.len(), info.trackers.len());
        
        // Test standalone list_trackers function
        let trackers_func = list_trackers(&info);
        assert_eq!(trackers_func.len(), info.trackers.len());
        
        // Test primary_trackers method
        let primary = info.primary_trackers();
        let expected_primary: Vec<&str> = info.trackers
            .iter()
            .filter(|t| t.tier == 0)
            .map(|t| t.url.as_str())
            .collect();
        assert_eq!(primary, expected_primary);
        
        // Verify tracker structure
        for tracker in &info.trackers {
            assert!(!tracker.url.is_empty(), "Tracker URL should not be empty");
            assert!(tracker.tier >= 0, "Tracker tier should be non-negative");
            println!("Tracker: {} (tier {})", tracker.url, tracker.tier);
        }
        
        println!("Tracker list test passed!");
        println!("Total trackers: {}", info.trackers.len());
        println!("Primary trackers: {}", primary.len());
    }

    #[test]
    fn test_num_pieces() {
        let test_file = first_torrent_file().expect("No .torrent file found in repo root");
        let data = fs::read(test_file).expect("Failed to read test torrent file");
        let info = parse_torrent(&data).expect("Failed to parse torrent");
        
        let num_pieces = info.num_pieces();
        assert!(num_pieces > 0, "Number of pieces should be positive");
        
        // Verify calculation
        let expected = ((info.total_size + info.piece_size as u64 - 1) / info.piece_size as u64) as u32;
        assert_eq!(num_pieces, expected, "num_pieces calculation mismatch");
        
        println!("Number of pieces: {}", num_pieces);
    }

    #[test]
    fn test_parse_cache() {
        clear_parse_cache();
        
        let test_file = first_torrent_file().expect("No .torrent file found in repo root");
        let data = fs::read(test_file).expect("Failed to read test torrent file");
        
        // Parse with cache
        let info1 = parse_torrent_cached(&data).expect("Failed to parse torrent");
        let info2 = parse_torrent_cached(&data).expect("Failed to parse cached torrent");
        
        // Both should return the same info
        assert_eq!(info1.info_hash, info2.info_hash);
        assert_eq!(info1.name, info2.name);
        
        // Parse without cache should also work
        let info3 = parse_torrent(&data).expect("Failed to parse torrent without cache");
        assert_eq!(info3.info_hash, info1.info_hash);
        
        clear_parse_cache();
        println!("Cache test passed!");
    }

    #[test]
    fn test_metadata_fields() {
        let test_file = first_torrent_file().expect("No .torrent file found in repo root");
        let data = fs::read(test_file).expect("Failed to read test torrent file");
        let info = parse_torrent(&data).expect("Failed to parse torrent");
        
        // Print all metadata for debugging
        println!("Metadata for {}", info.name);
        println!("  Info hash: {}", info.info_hash);
        println!("  Total size: {} bytes", info.total_size);
        println!("  Piece size: {} bytes", info.piece_size);
        println!("  File count: {}", info.file_count);
        println!("  Tracker count: {}", info.trackers.len());
        
        if let Some(ref comment) = info.comment {
            println!("  Comment: {}", comment);
        }
        if let Some(ref created_by) = info.created_by {
            println!("  Created by: {}", created_by);
        }
        if let Some(creation_date) = info.creation_date {
            println!("  Creation date: {} (timestamp)", creation_date);
        }
        
        // Test that metadata fields are correctly optional
        // These should not panic even if they are None
        let _ = info.comment.as_ref();
        let _ = info.created_by.as_ref();
        let _ = info.creation_date;
    }
}