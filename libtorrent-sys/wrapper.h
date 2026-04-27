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
    char* path;        // Full file path (including subdirectories)
    uint64_t size;     // File size in bytes
    uint64_t offset;   // Byte offset of this file within the torrent
    uint32_t first_piece;  // First piece index containing this file
    uint32_t last_piece;   // Last piece index containing this file
} libtorrent_file_entry_t;

// Torrent info structure
typedef struct {
    char* name;
    char* info_hash_hex;  // 40 characters for SHA1 hex + null terminator
    uint64_t total_size;
    uint32_t piece_size;  // Number of bytes per piece
    uint32_t file_count;
    libtorrent_file_entry_t* files;  // Array of file entries
    libtorrent_error_t error_code;
    char* error_message;
} libtorrent_torrent_info_t;

// Parse torrent from buffer
libtorrent_torrent_info_t* libtorrent_parse_torrent(const uint8_t* data, size_t size);

// Free torrent info
void libtorrent_free_torrent_info(libtorrent_torrent_info_t* info);

// Opaque session handle
typedef struct libtorrent_session_t libtorrent_session_t;

// Add torrent params
typedef struct {
    const uint8_t* torrent_data;
    size_t torrent_size;
} libtorrent_add_torrent_params_t;

// Create a new libtorrent session
libtorrent_session_t* libtorrent_create_session();

// Add a torrent to the session (paused)
libtorrent_error_t libtorrent_add_torrent(libtorrent_session_t* session, const libtorrent_add_torrent_params_t* params, char** error_message);

// Add a torrent to the session (paused) with custom save path
libtorrent_error_t libtorrent_add_torrent_ex(libtorrent_session_t* session, const libtorrent_add_torrent_params_t* params, const char* save_path, size_t save_path_len, char** error_message);

// Alert types
typedef enum {
    LIBTORRENT_ALERT_UNKNOWN = 0,
    LIBTORRENT_ALERT_PIECE_FINISHED,
    LIBTORRENT_ALERT_TORRENT_FINISHED,
    LIBTORRENT_ALERT_SAVE_RESUME_DATA,
    LIBTORRENT_ALERT_ADD_TORRENT,
    LIBTORRENT_ALERT_METADATA_RECEIVED,
    LIBTORRENT_ALERT_PIECE_READ,
} libtorrent_alert_type_t;

// Alert structure
typedef struct {
    libtorrent_alert_type_t type;
    char* alert_type_name;     // C++ type name for debugging
    char* message;             // Alert message
    char* info_hash_hex;       // Info hash of the torrent (40 chars + null)
    uint32_t piece_index;      // For piece-related alerts
    int error_code;            // For error alerts
} libtorrent_alert_t;

// Alert list structure
typedef struct {
    libtorrent_alert_t* alerts;
    size_t count;
} libtorrent_alert_list_t;

// Pop all pending alerts from session
libtorrent_alert_list_t libtorrent_pop_alerts(libtorrent_session_t* session);

// Wait for an alert (returns after timeout_ms or when alert is available)
// Returns 1 if alert is available, 0 on timeout
int libtorrent_wait_for_alert(libtorrent_session_t* session, int timeout_ms);

// Free alert list
void libtorrent_free_alert_list(libtorrent_alert_list_t* list);

// Dynamically configure which alert categories are received
void libtorrent_set_alert_mask(libtorrent_session_t* session, uint64_t mask);

// Destroy a libtorrent session
void libtorrent_destroy_session(libtorrent_session_t* session);

#ifdef __cplusplus
}
#endif

#endif // LIBTORRENT_FFI_H