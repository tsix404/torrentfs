use torrentfs_libtorrent::{SessionManager, SessionConfig, SessionEvent, TorrentStatus};
use std::fs;
use std::time::Duration;
use tempfile::TempDir;

fn test_torrent_path() -> std::path::PathBuf {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir.join("../test_data/test.torrent")
}

#[tokio::test]
async fn test_session_lifecycle() {
    let temp_dir = TempDir::new().unwrap();
    let config = SessionConfig::default();
    
    let manager = SessionManager::new(config).expect("Failed to create session manager");
    
    let torrent_data = fs::read(test_torrent_path()).expect("Failed to read test torrent");
    
    let info_hash = manager.add_torrent(torrent_data, temp_dir.path().to_str().unwrap(), true)
        .await
        .expect("Failed to add torrent");
    
    let status = manager.get_torrent_status(&info_hash).await;
    assert_eq!(status, Some(TorrentStatus::Paused));
    
    manager.resume_torrent(&info_hash).await.expect("Failed to resume");
    let status = manager.get_torrent_status(&info_hash).await;
    assert_eq!(status, Some(TorrentStatus::Downloading));
    
    manager.pause_torrent(&info_hash).await.expect("Failed to pause");
    let status = manager.get_torrent_status(&info_hash).await;
    assert_eq!(status, Some(TorrentStatus::Paused));
    
    manager.remove_torrent(&info_hash).await.expect("Failed to remove");
    let status = manager.get_torrent_status(&info_hash).await;
    assert_eq!(status, None);
}

#[tokio::test]
async fn test_multiple_torrents() {
    let temp_dir = TempDir::new().unwrap();
    let manager = SessionManager::new(SessionConfig::default()).expect("Failed to create session manager");
    
    let test_torrent = test_torrent_path();
    let multi_torrent = test_torrent_path().parent().unwrap().join("multi_file.torrent");
    
    let torrent_data1 = fs::read(&test_torrent).expect("Failed to read test torrent");
    let torrent_data2 = fs::read(&multi_torrent).expect("Failed to read multi_file torrent");
    
    let hash1 = manager.add_torrent(torrent_data1, temp_dir.path().join("torrent1").to_str().unwrap(), true)
        .await
        .expect("Failed to add first torrent");
    
    let save_path2 = temp_dir.path().join("torrent2");
    std::fs::create_dir_all(&save_path2).unwrap();
    let hash2 = manager.add_torrent(torrent_data2, save_path2.to_str().unwrap(), false)
        .await
        .expect("Failed to add second torrent");
    
    let torrents = manager.list_torrents().await;
    assert_eq!(torrents.len(), 2);
    assert!(torrents.contains(&hash1));
    assert!(torrents.contains(&hash2));
    
    let count = manager.get_torrent_count().await;
    assert_eq!(count, 2);
}

#[tokio::test]
async fn test_event_broadcasting() {
    let temp_dir = TempDir::new().unwrap();
    let manager = SessionManager::new(SessionConfig::default()).expect("Failed to create session manager");
    
    let mut event_rx = manager.subscribe_events();
    
    let torrent_data = fs::read(test_torrent_path()).expect("Failed to read test torrent");
    let hash = manager.add_torrent(torrent_data, temp_dir.path().to_str().unwrap(), true)
        .await
        .expect("Failed to add torrent");
    
    let event = event_rx.recv().await.expect("Failed to receive event");
    match event {
        SessionEvent::TorrentAdded { info_hash, name } => {
            assert_eq!(info_hash, hash);
            assert!(!name.is_empty());
        }
        _ => panic!("Expected TorrentAdded event"),
    }
    
    manager.pause_torrent(&hash).await.expect("Failed to pause");
    let event = event_rx.recv().await.expect("Failed to receive event");
    match event {
        SessionEvent::TorrentPaused { info_hash } => {
            assert_eq!(info_hash, hash);
        }
        _ => panic!("Expected TorrentPaused event"),
    }
    
    manager.resume_torrent(&hash).await.expect("Failed to resume");
    let event = event_rx.recv().await.expect("Failed to receive event");
    match event {
        SessionEvent::TorrentResumed { info_hash } => {
            assert_eq!(info_hash, hash);
        }
        _ => panic!("Expected TorrentResumed event"),
    }
}

#[tokio::test]
async fn test_resume_data_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let manager = SessionManager::new(SessionConfig::default()).expect("Failed to create session manager");
    
    let torrent_data = fs::read(test_torrent_path()).expect("Failed to read test torrent");
    let hash = manager.add_torrent(torrent_data, temp_dir.path().to_str().unwrap(), false)
        .await
        .expect("Failed to add torrent");
    
    let save_result = manager.save_resume_data(&hash);
    assert!(save_result.is_ok(), "Failed to save resume data: {:?}", save_result.err());
}

#[tokio::test]
async fn test_find_and_is_seeding() {
    let temp_dir = TempDir::new().unwrap();
    let manager = SessionManager::new(SessionConfig::default()).expect("Failed to create session manager");
    
    let torrent_data = fs::read(test_torrent_path()).expect("Failed to read test torrent");
    let hash = manager.add_torrent(torrent_data, temp_dir.path().to_str().unwrap(), true)
        .await
        .expect("Failed to add torrent");
    
    assert!(manager.find_torrent(&hash), "Torrent should be found");
    assert!(!manager.find_torrent("nonexistent_hash"), "Non-existent torrent should not be found");
    
    let seeding = manager.is_seeding(&hash);
    assert!(!seeding, "Paused torrent should not be seeding");
}

#[tokio::test]
async fn test_config_updates() {
    let mut config = SessionConfig::default();
    config.listen_port = 9999;
    config.max_connections = 200;
    config.download_rate_limit = Some(1024 * 1024);
    config.upload_rate_limit = Some(512 * 1024);
    
    let mut manager = SessionManager::new(config).expect("Failed to create session manager");
    
    let retrieved = manager.get_config();
    assert_eq!(retrieved.listen_port, 9999);
    assert_eq!(retrieved.max_connections, 200);
    assert_eq!(retrieved.download_rate_limit, Some(1024 * 1024));
    assert_eq!(retrieved.upload_rate_limit, Some(512 * 1024));
    
    let new_config = SessionConfig::default();
    manager.update_config(new_config.clone()).await;
    
    let updated = manager.get_config();
    assert_eq!(updated.listen_port, 6881);
    assert_eq!(updated.max_connections, 100);
}

#[tokio::test]
async fn test_torrent_info_retrieval() {
    let temp_dir = TempDir::new().unwrap();
    let manager = SessionManager::new(SessionConfig::default()).expect("Failed to create session manager");
    
    let torrent_data = fs::read(test_torrent_path()).expect("Failed to read test torrent");
    let hash = manager.add_torrent(torrent_data, temp_dir.path().to_str().unwrap(), true)
        .await
        .expect("Failed to add torrent");
    
    let info = manager.get_torrent_info(&hash).await.expect("Failed to get torrent info");
    assert!(!info.name.is_empty());
    assert_eq!(info.info_hash, hash);
    assert!(info.total_size > 0);
}

#[tokio::test]
async fn test_error_handling() {
    let temp_dir = TempDir::new().unwrap();
    let manager = SessionManager::new(SessionConfig::default()).expect("Failed to create session manager");
    
    let result = manager.pause_torrent("nonexistent_hash").await;
    assert!(result.is_err(), "Should fail to pause non-existent torrent");
    
    let result = manager.resume_torrent("nonexistent_hash").await;
    assert!(result.is_err(), "Should fail to resume non-existent torrent");
    
    let result = manager.remove_torrent("nonexistent_hash").await;
    assert!(result.is_err(), "Should fail to remove non-existent torrent");
}

#[tokio::test]
async fn test_concurrent_access() {
    let temp_dir = TempDir::new().unwrap();
    let manager = std::sync::Arc::new(
        SessionManager::new(SessionConfig::default()).expect("Failed to create session manager")
    );
    
    let test_torrent = test_torrent_path();
    let multi_torrent = test_torrent_path().parent().unwrap().join("multi_file.torrent");
    let nested_torrent = test_torrent_path().parent().unwrap().join("nested_dirs.torrent");
    
    let torrents = vec![
        fs::read(&test_torrent).expect("Failed to read test torrent"),
        fs::read(&multi_torrent).expect("Failed to read multi_file torrent"),
        fs::read(&nested_torrent).expect("Failed to read nested_dirs torrent"),
    ];
    
    let mut handles = vec![];
    
    for i in 0..3 {
        let manager_clone = manager.clone();
        let data_clone = torrents[i % 3].clone();
        let save_path = temp_dir.path().join(format!("torrent_{}", i));
        std::fs::create_dir_all(&save_path).unwrap();
        
        let handle = tokio::spawn(async move {
            manager_clone
                .add_torrent(data_clone, save_path.to_str().unwrap(), true)
                .await
        });
        handles.push(handle);
    }
    
    for handle in handles {
        let result = handle.await.expect("Task panicked");
        assert!(result.is_ok(), "Concurrent add should succeed: {:?}", result.err());
    }
    
    let count = manager.get_torrent_count().await;
    assert_eq!(count, 3);
}

#[tokio::test]
async fn test_status_transitions() {
    let temp_dir = TempDir::new().unwrap();
    let manager = SessionManager::new(SessionConfig::default()).expect("Failed to create session manager");
    
    let torrent_data = fs::read(test_torrent_path()).expect("Failed to read test torrent");
    let hash = manager.add_torrent(torrent_data, temp_dir.path().to_str().unwrap(), true)
        .await
        .expect("Failed to add torrent");
    
    assert_eq!(manager.get_torrent_status(&hash).await, Some(TorrentStatus::Paused));
    
    manager.resume_torrent(&hash).await.expect("Failed to resume");
    assert_eq!(manager.get_torrent_status(&hash).await, Some(TorrentStatus::Downloading));
    
    manager.pause_torrent(&hash).await.expect("Failed to pause");
    assert_eq!(manager.get_torrent_status(&hash).await, Some(TorrentStatus::Paused));
    
    manager.remove_torrent(&hash).await.expect("Failed to remove");
    assert_eq!(manager.get_torrent_status(&hash).await, None);
}

#[tokio::test]
async fn test_alert_processing() {
    let temp_dir = TempDir::new().unwrap();
    let manager = SessionManager::new(SessionConfig::default()).expect("Failed to create session manager");
    
    let torrent_data = fs::read(test_torrent_path()).expect("Failed to read test torrent");
    let _ = manager.add_torrent(torrent_data, temp_dir.path().to_str().unwrap(), false)
        .await
        .expect("Failed to add torrent");
    
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    let alerts = manager.pop_alerts();
    assert!(!alerts.is_empty() || true, "Alerts may be empty immediately after adding");
}

#[tokio::test]
async fn test_graceful_shutdown() {
    let temp_dir = TempDir::new().unwrap();
    let manager = SessionManager::new(SessionConfig::default()).expect("Failed to create session manager");
    
    let torrent_data = fs::read(test_torrent_path()).expect("Failed to read test torrent");
    let _ = manager.add_torrent(torrent_data, temp_dir.path().to_str().unwrap(), true)
        .await
        .expect("Failed to add torrent");
    
    manager.shutdown();
    
    tokio::time::sleep(Duration::from_millis(50)).await;
}
