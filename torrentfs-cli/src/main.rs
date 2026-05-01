use anyhow::{bail, Context, Result};
use clap::Parser;
use std::fs;
use std::path::PathBuf;
use torrentfs::database::Database;
use torrentfs::repo::{FileEntry, InsertResult, TorrentRepo};
use torrentfs_libtorrent::torrent;

#[derive(Parser)]
#[clap(version, about)]
struct Args {
    /// Path to the .torrent file
    torrent_file: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let data = fs::read(&args.torrent_file)
        .with_context(|| format!("Failed to read torrent file: {:?}", args.torrent_file))?;

    let info = torrent::parse_torrent(&data)
        .with_context(|| format!("Failed to parse torrent file: {:?}", args.torrent_file))?;

    print_metadata(&info);

    let db = Database::new().await?;
    db.migrate().await?;

    let repo = TorrentRepo::new(db.pool().clone());
    let info_hash_bytes = hex_to_bytes(&info.info_hash)?;

    let files: Vec<FileEntry> = info
        .files
        .iter()
        .map(|f| FileEntry {
            id: 0,
            torrent_id: 0,
            path: f.path.clone(),
            size: f.size as i64,
            first_piece: f.first_piece as i64,
            last_piece: f.last_piece as i64,
            offset: f.offset as i64,
        })
        .collect();

    match repo
        .insert_if_not_exists(
            &info_hash_bytes,
            &info.name,
            info.total_size as i64,
            info.file_count as i64,
            "",
            Some(data.as_slice()),
            files,
        )
        .await?
    {
        InsertResult::Inserted(_) => println!("Saved to DB"),
        InsertResult::AlreadyExists(_) => println!("Already exists"),
    }

    Ok(())
}

fn print_metadata(info: &torrent::TorrentInfo) {
    println!("Name: {}", info.name);
    println!("Info hash: {}", info.info_hash);
    println!("Total size: {} bytes", info.total_size);
    println!("Piece size: {} bytes", info.piece_size);
    println!("File count: {}", info.file_count);
    println!("\nFile list:");

    for (i, file) in info.files.iter().enumerate() {
        println!("  {}. {} ({} bytes, offset={}, pieces={}-{})",
            i + 1, file.path, file.size, file.offset, file.first_piece, file.last_piece);
    }
}

fn hex_to_bytes(hex: &str) -> Result<Vec<u8>> {
    if hex.len() % 2 != 0 {
        bail!("Invalid hex string length: {}", hex.len());
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&hex[i..i + 2], 16)
                .with_context(|| format!("Invalid hex character at position {}", i))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::SqlitePool;
    use sqlx::sqlite::SqliteConnectOptions;
    use std::str::FromStr;
    use tempfile::TempDir;

    async fn setup_test_db() -> (TempDir, Database) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let options = SqliteConnectOptions::from_str(&db_path.to_string_lossy())
            .unwrap()
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(options).await.unwrap();
        let db = Database::with_pool(pool);
        db.migrate().await.unwrap();
        (temp_dir, db)
    }

    #[test]
    fn test_hex_to_bytes() {
        assert_eq!(hex_to_bytes("0a1b").unwrap(), vec![0x0a, 0x1b]);
        assert_eq!(
            hex_to_bytes("77c8dd8e37d712522b49a3f2e62757d90e233c84").unwrap().len(),
            20
        );
        assert!(hex_to_bytes("zz").is_err());
        assert!(hex_to_bytes("abc").is_err());
    }

    #[tokio::test]
    async fn test_idempotent_insert() {
        let (_temp_dir, db) = setup_test_db().await;
        let repo = TorrentRepo::new(db.pool().clone());

        let info_hash = hex_to_bytes("aabbccdd11223344aabbccdd11223344aabbccdd").unwrap();
        let files = vec![FileEntry {
            id: 0,
            torrent_id: 0,
            path: "dir/file1.txt".to_string(),
            size: 1024,
            first_piece: 0,
            last_piece: 0,
            offset: 0,
        }];

        let result = repo
            .insert_if_not_exists(&info_hash, "test.torrent", 1024, 1, "", None::<&[u8]>, files.clone())
            .await
            .unwrap();
        assert!(
            matches!(result, InsertResult::Inserted(_)),
            "First insert should return Inserted"
        );

        let all = repo.list_all().await.unwrap();
        assert_eq!(all.len(), 1, "DB should have exactly 1 record after first insert");

        let saved_files = repo.get_files(all[0].id).await.unwrap();
        assert_eq!(saved_files.len(), 1);
        assert_eq!(saved_files[0].path, "dir/file1.txt");

        let result2 = repo
            .insert_if_not_exists(&info_hash, "test.torrent", 1024, 1, "", None::<&[u8]>, files)
            .await
            .unwrap();
        assert!(
            matches!(result2, InsertResult::AlreadyExists(_)),
            "Second insert should return AlreadyExists"
        );

        let all = repo.list_all().await.unwrap();
        assert_eq!(
            all.len(),
            1,
            "DB should still have exactly 1 record after duplicate insert"
        );
    }
}
