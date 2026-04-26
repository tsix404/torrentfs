//! TorrentFS CLI tool for parsing torrent metadata.
//!
//! Usage: cargo run -- <path/to/file.torrent>
//!
//! Outputs:
//! - name
//! - info_hash
//! - total_size
//! - file_count
//! - file list

use anyhow::{Result, Context};
use clap::Parser;
use std::fs;
use std::path::PathBuf;
use torrentfs_libtorrent::torrent;

#[derive(Parser)]
#[clap(version, about)]
struct Args {
    /// Path to the .torrent file
    torrent_file: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();
    
    // Read the torrent file
    let data = fs::read(&args.torrent_file)
        .with_context(|| format!("Failed to read torrent file: {:?}", args.torrent_file))?;
    
    // Parse the torrent data
    let info = torrent::parse_torrent(&data)
        .with_context(|| format!("Failed to parse torrent file: {:?}", args.torrent_file))?;
    
    // Output the metadata
    print_metadata(&info);
    
    Ok(())
}

fn print_metadata(info: &torrent::TorrentInfo) {
    println!("Name: {}", info.name);
    println!("Info hash: {}", info.info_hash);
    println!("Total size: {} bytes", info.total_size);
    println!("File count: {}", info.file_count);
    println!("\nFile list:");
    
    for (i, file) in info.files.iter().enumerate() {
        println!("  {}. {} ({} bytes)", i + 1, file.path, file.size);
    }
}