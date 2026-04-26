//! Torrent parsing functionality.

use anyhow::{Result, bail};
use std::ffi::CStr;

/// File entry in a torrent.
#[derive(Debug, Clone)]
pub struct FileEntry {
    /// Full file path (including subdirectories)
    pub path: String,
    /// File size in bytes
    pub size: u64,
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
    /// Number of files in the torrent
    pub file_count: u32,
    /// List of files in the torrent
    pub files: Vec<FileEntry>,
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
                });
            }
        }

        Ok(TorrentInfo {
            name,
            info_hash,
            total_size: ffi_info.total_size,
            file_count: ffi_info.file_count,
            files,
        })
    }

    /// Returns a list of files in the torrent.
    ///
    /// This is a convenience method that returns the file list.
    /// It handles subdirectory paths as they are provided by libtorrent.
    pub fn list_files(&self) -> Vec<FileEntry> {
        self.files.clone()
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

/// Parses torrent data from a buffer.
///
/// # Arguments
/// * `data` - Buffer containing the .torrent file data
///
/// # Returns
/// * `Ok(TorrentInfo)` on success
/// * `Err` if parsing fails
pub fn parse_torrent(data: &[u8]) -> Result<TorrentInfo> {
    if data.is_empty() {
        bail!("Empty torrent data");
    }

    // Call FFI function
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

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn test_torrent_dir() -> std::path::PathBuf {
        let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        manifest_dir.join("../") // torrentfs-libtorrent/../ = repo root
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
        
        // Check that files have valid data
        for (i, file) in info.files.iter().enumerate() {
            assert!(!file.path.is_empty(), "File {} should have a non-empty path", i);
            assert!(file.size > 0, "File {} should have a positive size", i);
            
            // Check that file path is properly formatted (may contain subdirectories)
            println!("File {}: {} ({} bytes)", i, file.path, file.size);
        }
        
        println!("File list test passed!");
        println!("Total files: {}", info.file_count);
        println!("Total size: {} bytes", info.total_size);
        
        // Verify total size matches sum of file sizes
        let total_from_files: u64 = info.files.iter().map(|f| f.size).sum();
        assert_eq!(info.total_size, total_from_files, "Total size should match sum of file sizes");
    }
}