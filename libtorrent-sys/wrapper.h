#ifndef LIBTORRENT_FFI_H
#define LIBTORRENT_FFI_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Error codes
typedef enum {
    LIBTORRENT_OK = 0,
    LIBTORRENT_ERROR_INVALID_DATA = 1,
    LIBTORRENT_ERROR_PARSE_FAILED = 2,
    LIBTORRENT_ERROR_ALLOCATION_FAILED = 3,
    LIBTORRENT_ERROR_UNKNOWN = 99
} libtorrent_error_t;

// File entry structure
typedef struct {
    char* path;     // Full file path (including subdirectories)
    uint64_t size;  // File size in bytes
} libtorrent_file_entry_t;

// Torrent info structure
typedef struct {
    char* name;
    char* info_hash_hex;  // 40 characters for SHA1 hex + null terminator
    uint64_t total_size;
    uint32_t file_count;
    libtorrent_file_entry_t* files;  // Array of file entries
    libtorrent_error_t error_code;
    char* error_message;
} libtorrent_torrent_info_t;

// Parse torrent from buffer
libtorrent_torrent_info_t* libtorrent_parse_torrent(const uint8_t* data, size_t size);

// Free torrent info
void libtorrent_free_torrent_info(libtorrent_torrent_info_t* info);

#ifdef __cplusplus
}
#endif

#endif // LIBTORRENT_FFI_H