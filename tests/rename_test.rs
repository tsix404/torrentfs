use tempfile::TempDir;
use torrentfs::db::Database;

/// Test that renaming a torrent file in metadata/ updates the database correctly
#[test]
fn test_rename_updates_database() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    let mut db = Database::open(&db_path).expect("Failed to open database");

    // Insert a torrent
    let torrent_id = match db
        .insert_torrent(
            "",
            "ubuntu-25.10-desktop-amd64.iso",
            "ubuntu-25.10-desktop-amd64.iso.torrent",
            1024,
            "hash123",
            1,
        )
        .expect("Failed to insert torrent")
    {
        torrentfs::db::InsertTorrentResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    // Verify initial state
    let torrent = db
        .get_torrent_by_filename_and_source_path("ubuntu-25.10-desktop-amd64.iso.torrent", "")
        .expect("Failed to get torrent")
        .expect("Torrent not found");
    assert_eq!(torrent.name, "ubuntu-25.10-desktop-amd64.iso");
    assert_eq!(torrent.filename, "ubuntu-25.10-desktop-amd64.iso.torrent");

    // Rename the torrent
    db.rename_torrent(torrent_id, "ubuntu-25.10", "ubuntu-25.10.torrent", "")
        .expect("Failed to rename torrent");

    // Verify the rename
    let torrent = db
        .get_torrent_by_id(torrent_id)
        .expect("Failed to get torrent")
        .expect("Torrent not found");
    assert_eq!(torrent.name, "ubuntu-25.10");
    assert_eq!(torrent.filename, "ubuntu-25.10.torrent");

    // Old filename lookup should return None
    let old_lookup = db
        .get_torrent_by_filename_and_source_path("ubuntu-25.10-desktop-amd64.iso.torrent", "")
        .expect("Failed to lookup");
    assert!(old_lookup.is_none());

    // New filename lookup should work
    let new_lookup = db
        .get_torrent_by_filename_and_source_path("ubuntu-25.10.torrent", "")
        .expect("Failed to lookup");
    assert!(new_lookup.is_some());
}

/// Test that renaming a torrent file with source_path updates correctly
#[test]
fn test_rename_with_source_path() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    let mut db = Database::open(&db_path).expect("Failed to open database");

    // Insert a torrent with source_path
    let torrent_id = match db
        .insert_torrent(
            "os/linux",
            "ubuntu-25.10",
            "ubuntu-25.10.torrent",
            1024,
            "hash456",
            1,
        )
        .expect("Failed to insert torrent")
    {
        torrentfs::db::InsertTorrentResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    // Verify initial state
    let torrent = db
        .get_torrent_by_filename_and_source_path("ubuntu-25.10.torrent", "os/linux")
        .expect("Failed to get torrent")
        .expect("Torrent not found");
    assert_eq!(torrent.name, "ubuntu-25.10");
    assert_eq!(torrent.filename, "ubuntu-25.10.torrent");

    // Rename the torrent
    db.rename_torrent(
        torrent_id,
        "ubuntu-26.04",
        "ubuntu-26.04.torrent",
        "os/linux",
    )
    .expect("Failed to rename torrent");

    // Verify the rename
    let torrent = db
        .get_torrent_by_id(torrent_id)
        .expect("Failed to get torrent")
        .expect("Torrent not found");
    assert_eq!(torrent.name, "ubuntu-26.04");
    assert_eq!(torrent.filename, "ubuntu-26.04.torrent");

    // Old filename lookup should return None
    let old_lookup = db
        .get_torrent_by_filename_and_source_path("ubuntu-25.10.torrent", "os/linux")
        .expect("Failed to lookup");
    assert!(old_lookup.is_none());

    // New filename lookup should work
    let new_lookup = db
        .get_torrent_by_filename_and_source_path("ubuntu-26.04.torrent", "os/linux")
        .expect("Failed to lookup");
    assert!(new_lookup.is_some());
}

/// Test that renaming persists across database reopen
#[test]
fn test_rename_persists() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    // Insert and rename
    {
        let mut db = Database::open(&db_path).expect("Failed to open database");

        let torrent_id = match db
            .insert_torrent("", "old-name", "old-name.torrent", 1024, "hash789", 1)
            .expect("Failed to insert torrent")
        {
            torrentfs::db::InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        };

        db.rename_torrent(torrent_id, "new-name", "new-name.torrent", "")
            .expect("Failed to rename torrent");
    }

    // Reopen and verify
    {
        let db = Database::open(&db_path).expect("Failed to open database");

        let torrent = db
            .get_torrent_by_filename_and_source_path("new-name.torrent", "")
            .expect("Failed to get torrent")
            .expect("Torrent not found");
        assert_eq!(torrent.name, "new-name");
        assert_eq!(torrent.filename, "new-name.torrent");

        let old_lookup = db
            .get_torrent_by_filename_and_source_path("old-name.torrent", "")
            .expect("Failed to lookup");
        assert!(old_lookup.is_none());
    }
}
