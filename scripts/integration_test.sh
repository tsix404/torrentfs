#!/usr/bin/env bash
set -euo pipefail

MOUNT_POINT="${1:-/tmp/test-mnt}"
STATE_DIR="${2:-/tmp/test-mnt-state}"
TORRENT_DIR="/workspace/torrentfs"
TFS_PID=""
FAILED=0

cleanup() {
    if [ -n "$TFS_PID" ]; then
        kill "$TFS_PID" 2>/dev/null || true
        wait "$TFS_PID" 2>/dev/null || true
    fi
    fusermount -u "$MOUNT_POINT" 2>/dev/null || true
    rm -rf "$MOUNT_POINT" "$STATE_DIR" /tmp/test-non-torrent.txt
}
trap cleanup EXIT

mkdir -p "$MOUNT_POINT" "$STATE_DIR"

echo "=== MVP-3 Integration Test ==="
echo ""

echo "[1/5] Starting torrentfs at $MOUNT_POINT ..."
cargo run --package torrentfs-fuse -- "$MOUNT_POINT" --state-dir "$STATE_DIR" &
TFS_PID=$!
sleep 2

if ! kill -0 "$TFS_PID" 2>/dev/null; then
    echo "FAIL: torrentfs did not start"
    exit 1
fi
echo "PASS: torrentfs started (PID $TFS_PID)"
echo ""

echo "[2/5] Verifying directory structure ..."
ENTRIES=$(ls "$MOUNT_POINT/")
if echo "$ENTRIES" | grep -q "metadata" && echo "$ENTRIES" | grep -q "data"; then
    echo "PASS: ls shows metadata/ and data/"
else
    echo "FAIL: expected metadata/ and data/, got: $ENTRIES"
    FAILED=1
fi
echo ""

echo "[3/5] Copying .torrent file to metadata/ ..."
TORRENT_FILE=$(ls "$TORRENT_DIR"/*.torrent 2>/dev/null | head -1)
if [ -z "$TORRENT_FILE" ]; then
    echo "SKIP: No .torrent files found in $TORRENT_DIR"
else
    cp "$TORRENT_FILE" "$MOUNT_POINT/metadata/"
    CP_EXIT=$?
    sleep 1
    if [ $CP_EXIT -eq 0 ]; then
        BASENAME=$(basename "$TORRENT_FILE")
        INCOMING_FILE="$STATE_DIR/incoming/$BASENAME"
        if [ -f "$INCOMING_FILE" ]; then
            ORIG_SIZE=$(stat -c%s "$TORRENT_FILE")
            COPY_SIZE=$(stat -c%s "$INCOMING_FILE")
            if [ "$ORIG_SIZE" -eq "$COPY_SIZE" ]; then
                echo "PASS: .torrent accepted and persisted to incoming/ ($BASENAME, ${COPY_SIZE} bytes)"
            else
                echo "FAIL: File size mismatch (orig=$ORIG_SIZE, copy=$COPY_SIZE)"
                FAILED=1
            fi
        else
            echo "FAIL: File not persisted to $INCOMING_FILE"
            FAILED=1
        fi
    else
        echo "FAIL: cp of .torrent failed (exit=$CP_EXIT)"
        FAILED=1
    fi
fi
echo ""

echo "[4/5] Writing non-.torrent file to metadata/ ..."
echo "hello" > /tmp/test-non-torrent.txt
if cp /tmp/test-non-torrent.txt "$MOUNT_POINT/metadata/" 2>/dev/null; then
    echo "FAIL: non-.torrent file should have been rejected (EINVAL)"
    FAILED=1
else
    echo "PASS: non-.torrent file rejected"
fi
echo ""

echo "[5/5] Unmounting ..."
fusermount -u "$MOUNT_POINT"
wait "$TFS_PID" 2>/dev/null || true
TFS_PID=""
echo "PASS: clean unmount"
echo ""

if [ $FAILED -eq 0 ]; then
    echo "=== All tests PASSED ==="
    exit 0
else
    echo "=== Some tests FAILED ==="
    exit 1
fi