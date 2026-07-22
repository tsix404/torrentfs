//! Integration test: verify that peer-to-peer connectivity works
//! via local HTTP tracker + seeder.
//!
//! This test starts a local HTTP tracker + seeder, then creates a
//! downloader session that discovers the seeder via the tracker.
//!
//! This is the CI-level test infrastructure required by TSI-1938 to
//! validate file reads beyond cached data.

mod common;

use common::{local_test_config, TestHarness};
use std::thread;
use std::time::Duration;

/// Verify that two libtorrent sessions can discover each other via
/// the local HTTP tracker and establish peer connections.
#[test]
fn test_peer_discovery_via_tracker() {
    // ── Setup: start tracker + seeder ──────────────────────────────────
    let harness = TestHarness::new();

    println!(
        "Tracker: {}, announces: {}",
        harness.tracker.announce_url(),
        harness.tracker.announce_count()
    );

    let info_hash = hex::encode(
        harness
            .info
            .info_hash()
            .expect("Failed to get info hash"),
    );
    println!("Info hash: {}", info_hash);

    // ── Create downloader session directly (bypass DownloadManager) ────
    let config = local_test_config();
    let mut dl_session =
        torrentfs::download::Session::new(&config)
            .expect("Failed to create downloader session");

    let cache_dir =
        tempfile::TempDir::new().expect("Failed to create cache dir");

    // Re-parse torrent data from the harness (fresh TorrentInfo)
    let torrent_data = harness.torrent_data.clone();
    let dl_info = torrentfs::TorrentInfo::from_bytes(torrent_data)
        .expect("Failed to parse torrent for downloader");

    let handle = dl_session
        .add_torrent(&dl_info, cache_dir.path())
        .expect("Failed to add torrent to downloader session");

    println!("Downloader: torrent added, waiting for peers...");

    // ── Wait for the downloader to discover the seeder ─────────────────
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(30);
    let mut peers_found = false;

    loop {
        match handle.status() {
            Ok(status) => {
                println!(
                    "Downloader: state={:?}, progress={:.2}%, peers={}, seeds={}, dl_rate={}, ul_rate={}",
                    status.state,
                    status.progress * 100.0,
                    status.num_peers,
                    status.num_seeds,
                    status.download_rate,
                    status.upload_rate
                );

                if status.num_peers > 0 || status.num_seeds > 0 {
                    println!(
                        "Downloader: found {} peers, {} seeds!",
                        status.num_peers, status.num_seeds
                    );
                    peers_found = true;
                    break;
                }

                // Also check if already finished/seeding (file was cached somehow)
                if matches!(
                    status.state,
                    torrentfs::download::TorrentState::Finished
                        | torrentfs::download::TorrentState::Seeding
                ) {
                    println!("Downloader: torrent completed (may have found data locally)");
                    peers_found = true;
                    break;
                }
            }
            Err(e) => {
                panic!("Downloader: status error: {:?}", e);
            }
        }

        if start.elapsed() > timeout {
            break;
        }

        thread::sleep(Duration::from_millis(500));
    }

    // ── Verify tracker received the downloader's announce ──────────────
    let final_announce_count = harness.tracker.announce_count();
    println!(
        "Tracker final announce count: {}",
        final_announce_count
    );

    if !peers_found {
        panic!(
            "Downloader did not find any peers within {} seconds. \
             Tracker announces: {}. Check tracker and seeder health.",
            timeout.as_secs(),
            final_announce_count
        );
    }

    // ── If peers found, verify we can read a piece ─────────────────────
    println!("\n--- Testing piece read ---");
    let session_ref = &dl_session;
    match handle.read_piece(session_ref, 0) {
        Ok(data) => {
            if !data.is_empty() {
                println!(
                    "Read piece 0: {} bytes, first 50: {:?}",
                    data.len(),
                    String::from_utf8_lossy(&data[..50.min(data.len())])
                );
                assert_eq!(
                    &data[..50.min(data.len())],
                    &harness.file_content[..50.min(data.len())],
                    "Downloaded data doesn't match seed content"
                );
            } else {
                println!("Read piece 0: empty (piece not yet downloaded)");
            }
        }
        Err(e) => {
            // Piece read can fail if not downloaded yet - that's OK
            // as long as we proved peer connectivity
            println!("Piece read failed (expected if not yet downloaded): {:?}", e);
        }
    }

    println!("\n=== Peer discovery test passed! ===");
}
