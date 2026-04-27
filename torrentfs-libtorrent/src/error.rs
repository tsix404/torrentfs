use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LibtorrentErrorCode {
    Ok = 0,
    InvalidData = 1,
    ParseFailed = 2,
    AllocationFailed = 3,
    Timeout = 4,
    Cancelled = 5,
    Unknown = 99,
}

impl LibtorrentErrorCode {
    pub fn from_ffi(code: i32) -> Self {
        match code {
            0 => LibtorrentErrorCode::Ok,
            1 => LibtorrentErrorCode::InvalidData,
            2 => LibtorrentErrorCode::ParseFailed,
            3 => LibtorrentErrorCode::AllocationFailed,
            4 => LibtorrentErrorCode::Timeout,
            5 => LibtorrentErrorCode::Cancelled,
            _ => LibtorrentErrorCode::Unknown,
        }
    }

    pub fn is_permanent(&self) -> bool {
        matches!(self, 
            LibtorrentErrorCode::InvalidData | 
            LibtorrentErrorCode::ParseFailed | 
            LibtorrentErrorCode::AllocationFailed
        )
    }

    pub fn is_transient(&self) -> bool {
        matches!(self, 
            LibtorrentErrorCode::Timeout | 
            LibtorrentErrorCode::Unknown
        )
    }
}

impl fmt::Display for LibtorrentErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LibtorrentErrorCode::Ok => write!(f, "OK"),
            LibtorrentErrorCode::InvalidData => write!(f, "Invalid data"),
            LibtorrentErrorCode::ParseFailed => write!(f, "Parse failed"),
            LibtorrentErrorCode::AllocationFailed => write!(f, "Allocation failed"),
            LibtorrentErrorCode::Timeout => write!(f, "Timeout"),
            LibtorrentErrorCode::Cancelled => write!(f, "Cancelled"),
            LibtorrentErrorCode::Unknown => write!(f, "Unknown error"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LibtorrentError {
    pub code: LibtorrentErrorCode,
    pub message: String,
}

impl LibtorrentError {
    pub fn new(code: LibtorrentErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn is_permanent(&self) -> bool {
        self.code.is_permanent()
    }

    pub fn is_transient(&self) -> bool {
        self.code.is_transient()
    }
}

impl fmt::Display for LibtorrentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for LibtorrentError {}
