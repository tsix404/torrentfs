# Integration Testing for MVP-3

This directory contains integration test scripts for MVP-3 of TorrentFS.

## Overview

Two integration test approaches are available:

1. **Rust Integration Tests** (`torrentfs-fuse/tests/integration_test.rs`) - Primary, recommended approach
2. **Shell Script** (`integration_test.sh`) - Alternative for manual testing

## Rust Integration Tests (Recommended)

The Rust integration tests are the primary and recommended way to test MVP-3 functionality. They are:

- **More robust**: Use temp directories, proper error handling, and automatic cleanup
- **Self-contained**: Don't require external .torrent files
- **Comprehensive**: Cover all MVP-3 acceptance criteria
- **Integrated**: Run as part of `cargo test`

### Running Rust Integration Tests

```bash
# Run all tests
cargo test

# Run only integration tests
cargo test --test integration_test

# Run with verbose output
cargo test --test integration_test -- --nocapture
```

### Test Coverage

The Rust integration tests verify:

1. **Mount and directory structure** - FUSE mount shows `metadata/` and `data/` directories
2. **.torrent file acceptance** - Can copy .torrent files to `metadata/` directory
3. **Non-.torrent file rejection** - Non-.torrent files are rejected with EINVAL
4. **Memory management** - Files are not listed in `metadata/` after release (no memory leaks)

## Shell Integration Test Script

The shell script (`integration_test.sh`) is provided as an alternative for manual testing or CI integration.

### Changes from Original Version

The script has been updated to address C&R review feedback:

1. **Pre-built binary**: Uses `cargo build --release` once, then tests the binary
2. **Self-contained**: Includes test .torrent file (no external dependencies)
3. **No SKIP conditions**: Always validates all acceptance criteria
4. **Robust error handling**: Proper cleanup and resource management

### Running the Shell Script

```bash
# Make script executable
chmod +x scripts/integration_test.sh

# Run with default parameters
./scripts/integration_test.sh

# Run with custom mount point and state directory
./scripts/integration_test.sh /tmp/my-mount /tmp/my-state
```

### Script Features

- **Automatic cleanup**: Uses traps to clean up resources on exit
- **Detailed output**: Shows progress and results for each test
- **Error handling**: Continues testing even if individual tests fail
- **Resource management**: Properly manages FUSE mount and process lifecycle

## Test .torrent File

A minimal test .torrent file is included in `test_data/test.torrent`. This file:

- Contains valid bencoded torrent metadata
- Is small (102 bytes) for fast testing
- Has no external dependencies (no real tracker or files)

## Acceptance Criteria Verification

Both test approaches verify all MVP-3 acceptance criteria:

1. ✓ **Mount**: Can mount FUSE filesystem at specified mount point
2. ✓ **Directory structure**: `ls` shows `metadata/` and `data/` directories
3. ✓ **.torrent acceptance**: `.torrent` files can be copied to `metadata/` directory
4. ✓ **Non-.torrent rejection**: Non-.torrent files are rejected with EINVAL
5. ✓ **Clean unmount**: Filesystem can be unmounted cleanly

## Recommendations

- Use **Rust integration tests** for automated testing and CI/CD
- Use **shell script** for manual testing or integration with external systems
- The Rust tests are more thorough and should be considered the authoritative test suite