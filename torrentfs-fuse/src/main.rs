//! TorrentFS FUSE mount binary.

use anyhow::Result;
use clap::Parser;
use torrentfs_fuse::mount;

#[derive(Parser)]
#[clap(version, about)]
struct Args {
    /// Mount point
    mount_point: String,

    /// State directory
    #[clap(long, default_value = "~/.local/share/torrentfs")]
    state_dir: String,

    /// Log level
    #[clap(long, default_value = "info")]
    log_level: String,
}

fn main() -> Result<()> {
    let args = Args::parse();
    // Stub
    mount::mount(&args.mount_point)?;
    Ok(())
}