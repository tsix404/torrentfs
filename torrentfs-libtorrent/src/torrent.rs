//! Torrent parsing functionality.

use anyhow::{Result, bail};
use std::ffi::CStr;

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

        Ok(TorrentInfo {
            name,
            info_hash,
            total_size: ffi_info.total_size,
            file_count: ffi_info.file_count,
        })
    }
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

    #[test]
    fn test_parse_valid_torrent() {
        // Test with the provided torrent file
        let test_file = "/workspace/torrentfs/77c8dd8e37d712522b49a3f2e62757d90e233c84.torrent";
        
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
}