//! Torrent file validation and detection module.
//!
//! Provides bencode format validation and BitTorrent file detection
//! before passing data to libtorrent FFI.

use anyhow::Result;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum ValidationError {
    EmptyData,
    InvalidBencode(String),
    MissingInfoDict,
    MissingInfoName,
    MissingInfoPieceLength,
    MissingInfoPieces,
    InvalidInfoName,
    InvalidPieceLength,
    InvalidPiecesLength,
    InvalidAnnounce,
    InvalidEncoding,
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationError::EmptyData => write!(f, "Empty torrent data"),
            ValidationError::InvalidBencode(msg) => write!(f, "Invalid bencode: {}", msg),
            ValidationError::MissingInfoDict => write!(f, "Missing 'info' dictionary"),
            ValidationError::MissingInfoName => write!(f, "Missing 'info.name' field"),
            ValidationError::MissingInfoPieceLength => write!(f, "Missing 'info.piece length' field"),
            ValidationError::MissingInfoPieces => write!(f, "Missing 'info.pieces' field"),
            ValidationError::InvalidInfoName => write!(f, "Invalid 'info.name' value"),
            ValidationError::InvalidPieceLength => write!(f, "Invalid 'info.piece length' value"),
            ValidationError::InvalidPiecesLength => write!(f, "Invalid 'info.pieces' length (must be multiple of 20)"),
            ValidationError::InvalidAnnounce => write!(f, "Invalid 'announce' field"),
            ValidationError::InvalidEncoding => write!(f, "Invalid 'encoding' field"),
        }
    }
}

impl std::error::Error for ValidationError {}

#[derive(Debug, Clone)]
pub struct TorrentMetadata {
    pub name: String,
    pub piece_length: u64,
    pub pieces_hash_count: usize,
    pub is_private: bool,
    pub announce: Option<String>,
    pub announce_list: Option<Vec<Vec<String>>>,
    pub encoding: Option<String>,
    pub has_files: bool,
    pub total_files: usize,
}

pub struct TorrentValidator;

impl TorrentValidator {
    pub fn new() -> Self {
        Self
    }

    pub fn validate(data: &[u8]) -> Result<TorrentMetadata, ValidationError> {
        if data.is_empty() {
            return Err(ValidationError::EmptyData);
        }

        let (value, _) = Self::parse_bencode_value(data)
            .map_err(|e| ValidationError::InvalidBencode(e))?;

        let dict = match value {
            BencodeValue::Dict(d) => d,
            _ => return Err(ValidationError::InvalidBencode("Root must be a dictionary".to_string())),
        };

        let info = dict.get(b"info".as_slice())
            .ok_or(ValidationError::MissingInfoDict)?;

        let info_dict = match info {
            BencodeValue::Dict(d) => d,
            _ => return Err(ValidationError::MissingInfoDict),
        };

        let name = Self::extract_string(info_dict.get(b"name".as_slice()))
            .ok_or(ValidationError::MissingInfoName)?;

        if name.is_empty() {
            return Err(ValidationError::InvalidInfoName);
        }

        let piece_length = Self::extract_integer(info_dict.get(b"piece length".as_slice()))
            .ok_or(ValidationError::MissingInfoPieceLength)?;

        if piece_length == 0 {
            return Err(ValidationError::InvalidPieceLength);
        }

        let pieces = Self::extract_bytes(info_dict.get(b"pieces".as_slice()))
            .ok_or(ValidationError::MissingInfoPieces)?;

        if pieces.len() % 20 != 0 {
            return Err(ValidationError::InvalidPiecesLength);
        }

        let is_private = Self::extract_integer(info_dict.get(b"private".as_slice()))
            .map(|v| v == 1)
            .unwrap_or(false);

        let announce = Self::extract_string(dict.get(b"announce".as_slice()));

        let announce_list = dict.get(b"announce-list".as_slice()).and_then(|v| {
            match v {
                BencodeValue::List(tiers) => {
                    let parsed_tiers: Option<Vec<Vec<String>>> = tiers.iter()
                        .map(|tier| {
                            match tier {
                                BencodeValue::List(urls) => {
                                    urls.iter()
                                        .map(|url| Self::extract_bytes(Some(url))
                                            .map(|b| String::from_utf8_lossy(b).into_owned()))
                                        .collect::<Option<Vec<String>>>()
                                }
                                _ => None,
                            }
                        })
                        .collect();
                    parsed_tiers
                }
                _ => None,
            }
        });

        let encoding = Self::extract_string(dict.get(b"encoding".as_slice()));

        let (has_files, total_files) = if info_dict.contains_key(b"files".as_slice()) {
            let files_list = info_dict.get(b"files".as_slice());
            match files_list {
                Some(BencodeValue::List(files)) => (true, files.len()),
                _ => (false, 1),
            }
        } else {
            (false, 1)
        };

        Ok(TorrentMetadata {
            name,
            piece_length,
            pieces_hash_count: pieces.len() / 20,
            is_private,
            announce,
            announce_list,
            encoding,
            has_files,
            total_files,
        })
    }

    pub fn is_valid_torrent(data: &[u8]) -> bool {
        Self::validate(data).is_ok()
    }

    pub fn detect_torrent_file(data: &[u8]) -> Result<bool> {
        if data.is_empty() {
            return Ok(false);
        }

        if data[0] != b'd' {
            return Ok(false);
        }

        match Self::validate(data) {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    fn extract_string(value: Option<&BencodeValue>) -> Option<String> {
        match value {
            Some(BencodeValue::Bytes(b)) => Some(String::from_utf8_lossy(b).into_owned()),
            _ => None,
        }
    }

    fn extract_bytes(value: Option<&BencodeValue>) -> Option<&[u8]> {
        match value {
            Some(BencodeValue::Bytes(b)) => Some(b),
            _ => None,
        }
    }

    fn extract_integer(value: Option<&BencodeValue>) -> Option<u64> {
        match value {
            Some(BencodeValue::Integer(i)) => Some(*i as u64),
            _ => None,
        }
    }

    fn parse_bencode_value(data: &[u8]) -> Result<(BencodeValue, usize), String> {
        if data.is_empty() {
            return Err("Empty data".to_string());
        }

        match data[0] {
            b'd' => Self::parse_dict(data),
            b'l' => Self::parse_list(data),
            b'i' => Self::parse_integer(data),
            b'0'..=b'9' => Self::parse_string(data),
            _ => Err(format!("Invalid bencode prefix: {}", data[0] as char)),
        }
    }

    fn parse_integer(data: &[u8]) -> Result<(BencodeValue, usize), String> {
        if data.is_empty() || data[0] != b'i' {
            return Err("Integer must start with 'i'".to_string());
        }

        let end = data.iter()
            .position(|&b| b == b'e')
            .ok_or("Missing integer terminator 'e'".to_string())?;

        if end == 1 {
            return Err("Empty integer value".to_string());
        }

        let num_str = String::from_utf8_lossy(&data[1..end]);
        let num: i64 = num_str.parse()
            .map_err(|_| format!("Invalid integer: {}", num_str))?;

        Ok((BencodeValue::Integer(num), end + 1))
    }

    fn parse_string(data: &[u8]) -> Result<(BencodeValue, usize), String> {
        let colon_pos = data.iter()
            .position(|&b| b == b':')
            .ok_or("Missing string length separator ':'".to_string())?;

        let len_str = String::from_utf8_lossy(&data[..colon_pos]);
        let len: usize = len_str.parse()
            .map_err(|_| format!("Invalid string length: {}", len_str))?;

        let start = colon_pos + 1;
        let end = start + len;

        if end > data.len() {
            return Err(format!("String data exceeds bounds: {} > {}", end, data.len()));
        }

        let bytes = data[start..end].to_vec();
        Ok((BencodeValue::Bytes(bytes), end))
    }

    fn parse_list(data: &[u8]) -> Result<(BencodeValue, usize), String> {
        if data.is_empty() || data[0] != b'l' {
            return Err("List must start with 'l'".to_string());
        }

        let mut pos = 1;
        let mut items = Vec::new();

        while pos < data.len() && data[pos] != b'e' {
            let (value, consumed) = Self::parse_bencode_value(&data[pos..])?;
            items.push(value);
            pos += consumed;
        }

        if pos >= data.len() {
            return Err("Missing list terminator 'e'".to_string());
        }

        Ok((BencodeValue::List(items), pos + 1))
    }

    fn parse_dict(data: &[u8]) -> Result<(BencodeValue, usize), String> {
        if data.is_empty() || data[0] != b'd' {
            return Err("Dictionary must start with 'd'".to_string());
        }

        let mut pos = 1;
        let mut dict: HashMap<Vec<u8>, BencodeValue> = HashMap::new();

        while pos < data.len() && data[pos] != b'e' {
            if data[pos] < b'0' || data[pos] > b'9' {
                return Err("Dictionary key must be a string".to_string());
            }

            let (key, key_consumed) = Self::parse_string(&data[pos..])?;
            pos += key_consumed;

            let key_bytes = match key {
                BencodeValue::Bytes(b) => b,
                _ => return Err("Dictionary key must be bytes".to_string()),
            };

            if pos >= data.len() {
                return Err("Missing value after key".to_string());
            }

            let (value, val_consumed) = Self::parse_bencode_value(&data[pos..])?;
            pos += val_consumed;

            dict.insert(key_bytes, value);
        }

        if pos >= data.len() {
            return Err("Missing dictionary terminator 'e'".to_string());
        }

        Ok((BencodeValue::Dict(dict), pos + 1))
    }
}

#[derive(Debug, Clone)]
enum BencodeValue {
    Integer(i64),
    Bytes(Vec<u8>),
    List(Vec<BencodeValue>),
    Dict(HashMap<Vec<u8>, BencodeValue>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn test_torrent_dir() -> std::path::PathBuf {
        let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        manifest_dir.join("../test_data")
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
    fn test_validate_valid_torrent() {
        let test_file = first_torrent_file().expect("No .torrent file found");
        let data = fs::read(&test_file).expect("Failed to read test torrent");

        let result = TorrentValidator::validate(&data);
        assert!(result.is_ok(), "Failed to validate valid torrent: {:?}", result.err());

        let meta = result.unwrap();
        assert!(!meta.name.is_empty());
        assert!(meta.piece_length > 0);
        assert!(meta.pieces_hash_count > 0);
    }

    #[test]
    fn test_validate_empty_data() {
        let result = TorrentValidator::validate(b"");
        assert!(matches!(result, Err(ValidationError::EmptyData)));
    }

    #[test]
    fn test_validate_not_a_dict() {
        let result = TorrentValidator::validate(b"4:test");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_missing_info() {
        let data = b"d4:name4:teste";
        let result = TorrentValidator::validate(data);
        assert!(matches!(result, Err(ValidationError::MissingInfoDict)));
    }

    #[test]
    fn test_validate_invalid_bencode() {
        let result = TorrentValidator::validate(b"invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_is_valid_torrent() {
        let test_file = first_torrent_file().expect("No .torrent file found");
        let data = fs::read(&test_file).expect("Failed to read test torrent");

        assert!(TorrentValidator::is_valid_torrent(&data));
        assert!(!TorrentValidator::is_valid_torrent(b""));
        assert!(!TorrentValidator::is_valid_torrent(b"not a torrent"));
    }

    #[test]
    fn test_detect_torrent_file() {
        let test_file = first_torrent_file().expect("No .torrent file found");
        let data = fs::read(&test_file).expect("Failed to read test torrent");

        let result = TorrentValidator::detect_torrent_file(&data);
        assert!(result.is_ok());
        assert!(result.unwrap());

        let result = TorrentValidator::detect_torrent_file(b"not a torrent");
        assert!(result.is_ok());
        assert!(!result.unwrap());

        let result = TorrentValidator::detect_torrent_file(b"");
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_all_test_torrents() {
        let dir = test_torrent_dir();
        let entries = fs::read_dir(&dir).expect("Failed to read test_data directory");

        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().map(|e| e == "torrent").unwrap_or(false) {
                let data = fs::read(&path).expect("Failed to read torrent file");
                let result = TorrentValidator::validate(&data);
                assert!(result.is_ok(), 
                    "Failed to validate {}: {:?}", 
                    path.display(), 
                    result.err()
                );
            }
        }
    }

    #[test]
    fn test_parse_integer() {
        let data = b"i42e";
        let result = TorrentValidator::parse_bencode_value(data);
        assert!(result.is_ok());
        
        let (value, consumed) = result.unwrap();
        assert_eq!(consumed, 4);
        match value {
            BencodeValue::Integer(i) => assert_eq!(i, 42),
            _ => panic!("Expected integer"),
        }
    }

    #[test]
    fn test_parse_negative_integer() {
        let data = b"i-42e";
        let result = TorrentValidator::parse_bencode_value(data);
        assert!(result.is_ok());
        
        let (value, _) = result.unwrap();
        match value {
            BencodeValue::Integer(i) => assert_eq!(i, -42),
            _ => panic!("Expected integer"),
        }
    }

    #[test]
    fn test_parse_string() {
        let data = b"4:test";
        let result = TorrentValidator::parse_bencode_value(data);
        assert!(result.is_ok());
        
        let (value, consumed) = result.unwrap();
        assert_eq!(consumed, 6);
        match value {
            BencodeValue::Bytes(b) => assert_eq!(b, b"test"),
            _ => panic!("Expected bytes"),
        }
    }

    #[test]
    fn test_parse_list() {
        let data = b"l4:testi42ee";
        let result = TorrentValidator::parse_bencode_value(data);
        assert!(result.is_ok());
        
        let (value, _) = result.unwrap();
        match value {
            BencodeValue::List(items) => {
                assert_eq!(items.len(), 2);
            }
            _ => panic!("Expected list"),
        }
    }

    #[test]
    fn test_parse_dict() {
        let data = b"d4:name4:test4:sizei42ee";
        let result = TorrentValidator::parse_bencode_value(data);
        assert!(result.is_ok());
        
        let (value, _) = result.unwrap();
        match value {
            BencodeValue::Dict(dict) => {
                assert!(dict.contains_key(b"name".as_slice()));
                assert!(dict.contains_key(b"size".as_slice()));
            }
            _ => panic!("Expected dict"),
        }
    }

    #[test]
    fn test_nested_structures() {
        let data = b"d4:infod4:name4:test12:piece lengthi16384e6:pieces20:aaaaaaaaaaaaaaaaaaaaee";
        let result = TorrentValidator::validate(data);
        assert!(result.is_ok());
        
        let meta = result.unwrap();
        assert_eq!(meta.name, "test");
        assert_eq!(meta.piece_length, 16384);
        assert_eq!(meta.pieces_hash_count, 1);
    }

    #[test]
    fn test_invalid_pieces_length() {
        let data = b"d4:infod4:name4:test12:piece lengthi16384e6:pieces10:aaaaaaaaaaee";
        let result = TorrentValidator::validate(data);
        assert!(matches!(result, Err(ValidationError::InvalidPiecesLength)));
    }

    #[test]
    fn test_missing_pieces() {
        let data = b"d4:infod4:name4:test12:piece lengthi16384eee";
        let result = TorrentValidator::validate(data);
        assert!(matches!(result, Err(ValidationError::MissingInfoPieces)));
    }

    #[test]
    fn test_zero_piece_length() {
        let data = b"d4:infod4:name4:test12:piece lengthi0e6:pieces20:aaaaaaaaaaaaaaaaaaaaee";
        let result = TorrentValidator::validate(data);
        assert!(matches!(result, Err(ValidationError::InvalidPieceLength)));
    }

    #[test]
    fn test_empty_name() {
        let data = b"d4:infod4:name0:12:piece lengthi16384e6:pieces20:aaaaaaaaaaaaaaaaaaaaee";
        let result = TorrentValidator::validate(data);
        assert!(matches!(result, Err(ValidationError::InvalidInfoName)));
    }

    #[test]
    fn test_truncated_data() {
        let data = b"d4:infod4:name4:test12:piece lengthi16384e6:pieces";
        let result = TorrentValidator::validate(data);
        assert!(result.is_err());
    }
}
