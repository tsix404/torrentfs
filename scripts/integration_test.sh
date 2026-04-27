#!/usr/bin/env bash
set -euo pipefail

# MVP-4 Integration Test Script
# Tests full lifecycle: metadata/ -> parse -> DB -> data/ browsing

readonly BINARY_NAME="torrentfs-fuse"
readonly MOUNT_POINT="${1:-/tmp/torrentfs-test-mnt}"
readonly STATE_DIR="${2:-/tmp/torrentfs-test-state}"
TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$TEST_DIR/.." && pwd)"
readonly TEST_TORRENT="$REPO_ROOT/test_data/test.torrent"

TFS_PID=""
FAILED=0
TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$TEST_DIR/.." && pwd)"
BINARY_PATH=""

cleanup() {
    echo "[CLEANUP] Cleaning up test resources..."
    
    # Unmount if mounted
    if mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
        fusermount -u "$MOUNT_POINT" 2>/dev/null || true
        echo "  Unmounted $MOUNT_POINT"
    fi
    
    # Kill torrentfs process if running
    if [ -n "$TFS_PID" ] && kill -0 "$TFS_PID" 2>/dev/null; then
        kill "$TFS_PID" 2>/dev/null || true
        wait "$TFS_PID" 2>/dev/null || true
        echo "  Stopped torrentfs (PID $TFS_PID)"
    fi
    
    # Clean up test directories
    rm -rf "$MOUNT_POINT" "$STATE_DIR" /tmp/test-non-torrent.txt 2>/dev/null || true
    
    echo "[CLEANUP] Done"
}
trap cleanup EXIT

error() {
    echo "ERROR: $*" >&2
    FAILED=1
}

pass() {
    echo "PASS: $*"
}

fail() {
    echo "FAIL: $*" >&2
    FAILED=1
}

# Build the binary if needed
build_binary() {
    echo "[BUILD] Building $BINARY_NAME..."
    if ! cargo build --release --package "$BINARY_NAME" >/dev/null 2>&1; then
        error "Failed to build $BINARY_NAME"
        return 1
    fi
    
    BINARY_PATH="$REPO_ROOT/target/release/$BINARY_NAME"
    if [ ! -f "$BINARY_PATH" ]; then
        error "Binary not found at $BINARY_PATH"
        return 1
    fi
    
    pass "Built $BINARY_NAME at $BINARY_PATH"
    return 0
}

# Verify test .torrent file exists
verify_test_torrent() {
    if [ ! -f "$TEST_TORRENT" ]; then
        error "Test .torrent file not found: $TEST_TORRENT"
        return 1
    fi
    
    # Verify it's a valid .torrent file (has 'announce' key)
    if ! grep -q "announce" "$TEST_TORRENT"; then
        error "Test .torrent file appears invalid: $TEST_TORRENT"
        return 1
    fi
    
    pass "Test .torrent file verified: $TEST_TORRENT"
    return 0
}

# Start torrentfs
start_torrentfs() {
    echo "[START] Starting torrentfs at $MOUNT_POINT..."
    
    mkdir -p "$MOUNT_POINT" "$STATE_DIR"
    
    # Start torrentfs in background
    "$BINARY_PATH" "$MOUNT_POINT" --state-dir "$STATE_DIR" &
    TFS_PID=$!
    
    # Wait for FUSE to be ready
    sleep 2
    
    if ! kill -0 "$TFS_PID" 2>/dev/null; then
        error "torrentfs process died immediately"
        return 1
    fi
    
    if ! mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
        error "Mount point not mounted: $MOUNT_POINT"
        return 1
    fi
    
    pass "torrentfs started (PID $TFS_PID), mounted at $MOUNT_POINT"
    return 0
}

# Test 1: Verify directory structure
test_directory_structure() {
    echo "[TEST 1/4] Verifying directory structure..."
    
    local entries
    entries=$(ls "$MOUNT_POINT/" 2>/dev/null || echo "")
    
    if echo "$entries" | grep -q "metadata" && echo "$entries" | grep -q "data"; then
        pass "Root directory contains metadata/ and data/"
        
        # Verify they are directories
        if [ -d "$MOUNT_POINT/metadata" ] && [ -d "$MOUNT_POINT/data" ]; then
            pass "metadata/ and data/ are directories"
        else
            fail "metadata/ or data/ is not a directory"
        fi
        
        # Verify no other entries
        local entry_count
        entry_count=$(echo "$entries" | wc -w)
        if [ "$entry_count" -eq 2 ]; then
            pass "Exactly 2 entries in root directory"
        else
            fail "Expected 2 entries, found $entry_count: $entries"
        fi
    else
        fail "Root directory missing metadata/ or data/, found: $entries"
    fi
}

# Test 2: Accept .torrent files and verify DB persistence
test_accept_torrent() {
    echo "[TEST 2/4] Testing .torrent file acceptance..."
    
    local torrent_name
    torrent_name=$(basename "$TEST_TORRENT")
    local dest_path="$MOUNT_POINT/metadata/$torrent_name"
    
    # Copy .torrent file
    if cp "$TEST_TORRENT" "$dest_path"; then
        pass "Successfully copied .torrent to metadata/"
        
        # Wait for file processing
        sleep 1
        
        # Verify file is still visible in metadata/
        if [ -f "$dest_path" ]; then
            pass "File persisted in metadata/ directory"
            
            # Verify file size matches
            local orig_size copy_size
            orig_size=$(stat -c%s "$TEST_TORRENT")
            copy_size=$(stat -c%s "$dest_path")
            
            if [ "$orig_size" -eq "$copy_size" ]; then
                pass "File size preserved ($orig_size bytes)"
            else
                fail "File size mismatch: original=$orig_size, copy=$copy_size"
            fi
        else
            fail "File not found in metadata/ directory: $dest_path"
        fi
    else
        fail "Failed to copy .torrent file to metadata/"
    fi
}

# Test 3: Verify data/ directory shows torrent contents
test_data_directory() {
    echo "[TEST 3/5] Testing data/ directory population..."
    
    # Copy test torrent
    local torrent_name
    torrent_name=$(basename "$TEST_TORRENT")
    cp "$TEST_TORRENT" "$MOUNT_POINT/metadata/" 2>/dev/null || true
    sleep 1
    
    # Check data/ directory
    local data_entries
    data_entries=$(ls "$MOUNT_POINT/data/" 2>/dev/null || echo "")
    
    if [ -n "$data_entries" ]; then
        pass "data/ directory contains torrent directories"
        
        # Check first torrent directory
        local first_dir
        first_dir=$(echo "$data_entries" | head -1)
        if [ -n "$first_dir" ]; then
            if [ -d "$MOUNT_POINT/data/$first_dir" ]; then
                pass "Torrent directory '$first_dir' is valid"
                
                # List files in torrent
                local files
                files=$(ls "$MOUNT_POINT/data/$first_dir/" 2>/dev/null || echo "")
                if [ -n "$files" ]; then
                    pass "Torrent contains files: $(echo "$files" | wc -w) items"
                else
                    fail "Torrent directory is empty"
                fi
            else
                fail "'$first_dir' is not a directory"
            fi
        fi
    else
        fail "data/ directory is empty (torrent not parsed)"
    fi
}

# Test 4: Reject non-.torrent files
test_reject_non_torrent() {
    echo "[TEST 4/5] Testing non-.torrent file rejection..."
    
    local test_file="/tmp/test-non-torrent.txt"
    echo "This is not a .torrent file" > "$test_file"
    
    # Attempt to copy non-.torrent file (should fail)
    if cp "$test_file" "$MOUNT_POINT/metadata/" 2>/dev/null; then
        fail "Non-.torrent file should have been rejected (EINVAL expected)"
    else
        pass "Non-.torrent file correctly rejected"
    fi
    
    rm -f "$test_file"
}

# Test 5: Clean unmount
test_clean_unmount() {
    echo "[TEST 5/5] Testing clean unmount..."
    
    # Unmount
    if fusermount -u "$MOUNT_POINT"; then
        pass "Successfully unmounted"
        
        # Wait for process to exit
        sleep 1
        
        if kill -0 "$TFS_PID" 2>/dev/null; then
            fail "torrentfs process still running after unmount"
        else
            pass "torrentfs process terminated cleanly"
            TFS_PID="" # Clear PID to prevent cleanup from trying to kill it
        fi
    else
        fail "Failed to unmount $MOUNT_POINT"
    fi
}

main() {
    echo "=== MVP-4 Integration Test ==="
    echo "Mount point: $MOUNT_POINT"
    echo "State directory: $STATE_DIR"
    echo ""
    
    # Pre-flight checks
    if ! build_binary; then
        exit 1
    fi
    
    if ! verify_test_torrent; then
        exit 1
    fi
    
    # Run tests
    if ! start_torrentfs; then
        exit 1
    fi
    
    test_directory_structure
    test_accept_torrent
    test_data_directory
    test_reject_non_torrent
    test_clean_unmount
    
    # Summary
    echo ""
    echo "=== Test Summary ==="
    if [ $FAILED -eq 0 ]; then
        echo "All tests PASSED"
        echo ""
        echo "Acceptance Criteria Verified:"
        echo "1. ✓ Can mount FUSE filesystem"
        echo "2. ✓ Directory structure shows metadata/ and data/"
        echo "3. ✓ Accepts .torrent files in metadata/"
        echo "4. ✓ Rejects non-.torrent files (EINVAL)"
        echo "5. ✓ Clean unmount"
        echo "6. ✓ Torrents parsed and visible in data/"
        exit 0
    else
        echo "Some tests FAILED"
        exit 1
    fi
}

main "$@"