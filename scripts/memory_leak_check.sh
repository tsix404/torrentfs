#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== TorrentFS Memory Leak Detection ==="
echo

check_tool() {
    local tool=$1
    if ! command -v "$tool" &> /dev/null; then
        echo "Error: $tool is not installed"
        echo "Install with: sudo apt-get install $tool"
        return 1
    fi
    return 0
}

run_valgrind() {
    echo "Running Valgrind memory check..."
    
    if ! check_tool valgrind; then
        return 1
    fi
    
    cd "$PROJECT_ROOT"
    
    cargo build --release 2>/dev/null
    
    local test_binary="target/release/deps/stability_test"
    if [[ ! -f "$test_binary" ]]; then
        cargo test --test stability_test --no-run --release
    fi
    
    valgrind \
        --leak-check=full \
        --show-leak-kinds=all \
        --track-origins=yes \
        --verbose \
        --log-file=valgrind_report.log \
        "$test_binary" \
        test_file_descriptor_leak_detection \
        --test-threads=1 \
        2>/dev/null || true
    
    echo
    echo "Valgrind report saved to: valgrind_report.log"
    
    if grep -q "ERROR SUMMARY: 0 errors" valgrind_report.log; then
        echo "✓ No memory errors detected by Valgrind"
    else
        echo "⚠ Potential memory errors detected. Check valgrind_report.log"
    fi
}

run_heaptrack() {
    echo "Running Heaptrack memory analysis..."
    
    if ! check_tool heaptrack; then
        return 1
    fi
    
    cd "$PROJECT_ROOT"
    
    cargo build --release 2>/dev/null
    
    local test_binary="target/release/deps/stability_test"
    if [[ ! -f "$test_binary" ]]; then
        cargo test --test stability_test --no-run --release
    fi
    
    heaptrack -o heaptrack_report \
        "$test_binary" \
        test_long_running_endurance \
        --test-threads=1 \
        2>/dev/null || true
    
    echo
    if [[ -f heaptrack_report.heaptrack.out ]]; then
        heaptrack_print heaptrack_report.heaptrack.out
        echo
        echo "Heaptrack report saved to: heaptrack_report.heaptrack.out"
    else
        echo "Heaptrack output not found"
    fi
}

run_asan() {
    echo "Running AddressSanitizer (ASan) check..."
    
    cd "$PROJECT_ROOT"
    
    RUSTFLAGS="-Z sanitizer=address" \
    cargo test \
        --test stability_test \
        --target x86_64-unknown-linux-gnu \
        -- \
        --test-threads=1 \
        2>&1 | tee asan_report.log || true
    
    echo
    echo "ASan report saved to: asan_report.log"
    
    if grep -q "ERROR: AddressSanitizer" asan_report.log; then
        echo "⚠ Memory errors detected by ASan"
    else
        echo "✓ No memory errors detected by ASan"
    fi
}

run_leak_sanitizer() {
    echo "Running LeakSanitizer (LSan) check..."
    
    cd "$PROJECT_ROOT"
    
    RUSTFLAGS="-Z sanitizer=leak" \
    cargo test \
        --test stability_test \
        --target x86_64-unknown-linux-gnu \
        -- \
        --test-threads=1 \
        2>&1 | tee lsan_report.log || true
    
    echo
    echo "LSan report saved to: lsan_report.log"
    
    if grep -q "ERROR: LeakSanitizer" lsan_report.log; then
        echo "⚠ Memory leaks detected by LSan"
    else
        echo "✓ No memory leaks detected by LSan"
    fi
}

usage() {
    echo "Usage: $0 [command]"
    echo
    echo "Commands:"
    echo "  valgrind    Run Valgrind memory check"
    echo "  heaptrack   Run Heaptrack memory analysis"
    echo "  asan        Run AddressSanitizer check (requires nightly Rust)"
    echo "  lsan        Run LeakSanitizer check (requires nightly Rust)"
    echo "  all         Run all available checks"
    echo "  help        Show this help message"
    echo
    echo "Examples:"
    echo "  $0 valgrind"
    echo "  $0 heaptrack"
    echo "  $0 all"
}

main() {
    local command=${1:-help}
    
    case "$command" in
        valgrind)
            run_valgrind
            ;;
        heaptrack)
            run_heaptrack
            ;;
        asan)
            run_asan
            ;;
        lsan)
            run_lsan
            ;;
        all)
            run_valgrind || true
            echo
            run_heaptrack || true
            ;;
        help|--help|-h)
            usage
            ;;
        *)
            echo "Unknown command: $command"
            usage
            exit 1
            ;;
    esac
}

main "$@"
