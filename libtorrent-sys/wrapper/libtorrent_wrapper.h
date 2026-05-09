#ifndef LIBTORRENT_WRAPPER_H
#define LIBTORRENT_WRAPPER_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

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

#ifdef __cplusplus
}
#endif

#endif
