# torrentfs

A Rust library for parsing torrent files using libtorrent via FFI.

## Features

- Parse .torrent files and extract metadata
- Extract torrent name, files, piece layout, and total size
- Support for both single-file and multi-file torrents
- Proper error handling for invalid or corrupted torrent files

## Requirements

- Rust 1.70 or later
- libtorrent-rasterbar 2.0.x
- OpenSSL
- C++17 compiler
- FUSE 3.x (libfuse3-dev)

## FUSE Configuration

TorrentFS uses FUSE to mount the virtual filesystem. To allow non-root users to access the mount point, you need to configure FUSE:

1. Edit `/etc/fuse.conf`:
   ```bash
   sudo sed -i 's/#user_allow_other/user_allow_other/' /etc/fuse.conf
   ```

   Or manually uncomment the `user_allow_other` line in `/etc/fuse.conf`:
   ```
   user_allow_other
   ```

2. Ensure your user is in the `fuse` group:
   ```bash
   sudo usermod -aG fuse $USER
   ```
   
   (Log out and back in for group changes to take effect)

3. On some systems, you may need to install FUSE development headers:
   ```bash
   # Ubuntu/Debian
   sudo apt-get install libfuse3-dev
   
   # Fedora
   sudo dnf install fuse3-devel
   
   # Arch Linux
   sudo pacman -S fuse3
   ```

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
torrentfs = "0.1"
```

## Usage

```rust
use torrentfs::{TorrentInfo, TorrentError};

fn main() -> Result<(), TorrentError> {
    let info = TorrentInfo::from_file("example.torrent")?;
    
    println!("Torrent: {}", info.name());
    println!("Total Size: {} bytes", info.total_size());
    println!("Piece Length: {} bytes", info.piece_length());
    println!("Number of Pieces: {}", info.num_pieces());
    println!("Number of Files: {}", info.num_files());
    
    let files = info.files()?;
    for file in files {
        println!("  {} ({} bytes)", file.path, file.size);
    }
    
    let hash = info.info_hash()?;
    print!("Info Hash: ");
    for byte in &hash {
        print!("{:02x}", byte);
    }
    println!();
    
    Ok(())
}
```

## API

### `TorrentInfo`

The main type for working with torrent metadata.

#### Methods

- `from_file(path)` - Load a torrent file from disk
- `name()` - Get the torrent name
- `total_size()` - Get total size in bytes
- `piece_length()` - Get piece length in bytes
- `num_pieces()` - Get number of pieces
- `num_files()` - Get number of files
- `files()` - Get list of files with paths and sizes
- `info_hash()` - Get the 20-byte SHA1 info hash
- `metadata()` - Get all metadata in one struct

### Error Handling

The library uses `thiserror` for error handling:

```rust
pub enum TorrentError {
    InvalidFile(String),
    ParseError(String),
    IoError(std::io::Error),
    NullPointer,
    Unknown { code: i32, message: String },
}
```

## Building from Source

```bash
# Ensure libtorrent is installed
# On Ubuntu/Debian:
sudo apt-get install libtorrent-rasterbar-dev

# Build
cargo build

# Run tests
cargo test

# Run example
cargo run --example torrent_info -- /path/to/file.torrent
```

## Docker

### Pull from GitHub Container Registry

Pre-built images are available at [ghcr.io/tsix404/torrentfs](https://github.com/tsix404/torrentfs/pkgs/container/torrentfs).

```bash
# Pull the latest stable release
docker pull ghcr.io/tsix404/torrentfs:latest

# Pull the development build (latest main branch)
docker pull ghcr.io/tsix404/torrentfs:dev

# Pull a specific version
docker pull ghcr.io/tsix404/torrentfs:v0.1.0
```

### Image Tags

| Tag      | Description                                    |
|----------|------------------------------------------------|
| `dev`    | Latest push to `main` branch                   |
| `latest` | Latest Git tag starting with `v*` (stable release) |
| `vX.Y.Z` | Specific semver release                        |
| `vX.Y`   | Minor version track (e.g., `v0.1`)            |

### Build Locally

```bash
docker build -t torrentfs .
```

### Run with FUSE

TorrentFS requires FUSE kernel access inside the container. Use the following flags when running:

```bash
# Create a mount point on the host
mkdir -p /tmp/torrentfs

# Run with FUSE support
docker run --rm \
  --cap-add SYS_ADMIN \
  --device /dev/fuse \
  -v /tmp/torrentfs:/mnt:shared \
  ghcr.io/tsix404/torrentfs:latest
```

**Required flags:**

- `--cap-add SYS_ADMIN` — grants the container capability to mount FUSE filesystems
- `--device /dev/fuse` — passes the FUSE device into the container
- `-v /host/path:/mnt:shared` — mounts a host directory for the virtual filesystem (use `:shared` for bidirectional access)

> **Note:** On systems with AppArmor or SELinux, you may need additional security profiles. See your distribution's documentation for details.

## License

MIT
