use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use torrentfs_fuse::mount;

#[derive(Parser)]
#[clap(version, about)]
struct Args {
    mount_point: String,

    #[clap(long, default_value = "~/.local/share/torrentfs")]
    state_dir: String,

    #[clap(long, default_value = "info")]
    log_level: String,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let state_dir = if args.state_dir.starts_with("~/") {
        dirs::home_dir()
            .map(|h| h.join(&args.state_dir[2..]))
            .unwrap_or_else(|| PathBuf::from(&args.state_dir))
    } else {
        PathBuf::from(&args.state_dir)
    };

    tracing_subscriber::fmt()
        .with_env_filter(args.log_level)
        .init();
    tracing::info!("Mounting torrentfs at {}", args.mount_point);
    mount::mount(&args.mount_point, &state_dir)?;
    Ok(())
}
