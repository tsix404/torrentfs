#!/bin/bash
set -euo pipefail

echo "=== Running integration test for all torrent files ==="
echo

count=0
for f in /workspace/torrentfs/*.torrent; do
  echo "=== Testing: $(basename "$f") ==="
  cargo run --package torrentfs-cli -- "$f" || exit 1
  echo
  count=$((count + 1))
done

echo "All $count torrents parsed successfully"