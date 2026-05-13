#ifndef LIBTORRENT_WRAPPER_H
#define LIBTORRENT_WRAPPER_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void* lt_session_t;
typedef void* lt_torrent_handle_t;
typedef void* lt_torrent_info_t;
typedef void* lt_file_storage_t;

typedef struct {
    const char* path;
    uint64_t size;
} lt_file_entry_t;

typedef struct {
    const char* name;
    uint64_t total_size;
    uint32_t piece_length;
    uint32_t num_pieces;
    uint32_t num_files;
    lt_file_entry_t* files;
    const uint8_t* info_hash;
} lt_torrent_metadata_t;

typedef struct {
    const char* message;
    int code;
} lt_error_t;

lt_torrent_info_t lt_torrent_info_create(const char* filepath, lt_error_t* error);
lt_torrent_info_t lt_torrent_info_create_from_buffer(const uint8_t* data, size_t size, lt_error_t* error);
void lt_torrent_info_destroy(lt_torrent_info_t info);

lt_torrent_metadata_t* lt_torrent_info_get_metadata(lt_torrent_info_t info);
void lt_torrent_metadata_destroy(lt_torrent_metadata_t* metadata);

const char* lt_torrent_info_name(lt_torrent_info_t info);
uint64_t lt_torrent_info_total_size(lt_torrent_info_t info);
uint32_t lt_torrent_info_piece_length(lt_torrent_info_t info);
uint32_t lt_torrent_info_num_pieces(lt_torrent_info_t info);
uint32_t lt_torrent_info_num_files(lt_torrent_info_t info);

int lt_torrent_info_get_files(lt_torrent_info_t info, lt_file_entry_t** files, uint32_t* count);
void lt_files_free(lt_file_entry_t* files);

int lt_torrent_info_get_info_hash(lt_torrent_info_t info, uint8_t* hash_out);

lt_session_t lt_session_create(const char* listen_interface, lt_error_t* error);
void lt_session_destroy(lt_session_t session);
lt_torrent_handle_t lt_session_add_torrent(lt_session_t session, lt_torrent_info_t info, const char* save_path, lt_error_t* error);
void lt_session_remove_torrent(lt_session_t session, lt_torrent_handle_t handle, int remove_files);
void lt_torrent_handle_destroy(lt_torrent_handle_t handle);

int lt_torrent_handle_is_valid(lt_torrent_handle_t handle);
int lt_torrent_handle_status(lt_torrent_handle_t handle, int* state, float* progress, uint64_t* total_done, uint64_t* total);
int lt_torrent_handle_read_piece(lt_session_t session, lt_torrent_handle_t handle, int piece_index, uint8_t** data_out, size_t* size_out, lt_error_t* error);
void lt_piece_data_free(uint8_t* data);
int lt_torrent_handle_get_piece_info(lt_torrent_handle_t handle, int file_index, int64_t* first_piece, int64_t* num_pieces, int64_t* file_offset);
int lt_torrent_handle_get_torrent_info(lt_torrent_handle_t handle, int64_t* piece_length, int64_t* num_pieces);

#ifdef __cplusplus
}
#endif

#endif
