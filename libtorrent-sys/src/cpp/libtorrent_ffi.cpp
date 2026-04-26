#include "libtorrent_ffi.h"
#include <libtorrent/bdecode.hpp>
#include <libtorrent/entry.hpp>
#include <libtorrent/torrent_info.hpp>
#include <libtorrent/hex.hpp>
#include <cstring>
#include <string>
#include <vector>

extern "C" {

libtorrent_torrent_info_t* libtorrent_parse_torrent(const uint8_t* data, size_t size) {
    auto* info = new libtorrent_torrent_info_t();
    memset(info, 0, sizeof(libtorrent_torrent_info_t));
    
    try {
        // Create a bdecode node from the buffer
        libtorrent::error_code ec;
        libtorrent::span<const char> buffer(reinterpret_cast<const char*>(data), size);
        libtorrent::bdecode_node node = libtorrent::bdecode(buffer, ec);
        
        if (ec) {
            info->error_code = LIBTORRENT_ERROR_PARSE_FAILED;
            info->error_message = strdup(ec.message().c_str());
            return info;
        }
        
        // Create torrent_info from the bdecode node
        libtorrent::torrent_info ti(node, ec);
        if (ec) {
            info->error_code = LIBTORRENT_ERROR_PARSE_FAILED;
            info->error_message = strdup(ec.message().c_str());
            return info;
        }
        
        // Get name
        std::string name = ti.name();
        info->name = strdup(name.c_str());
        
        // Get info hash as hex
        libtorrent::sha1_hash hash = ti.info_hash();
        std::string hash_str = hash.to_string();
        std::string hash_hex = libtorrent::aux::to_hex(libtorrent::span<const char>(hash_str.data(), hash_str.size()));
        info->info_hash_hex = strdup(hash_hex.c_str());
        
        // Get total size
        info->total_size = ti.total_size();
        
        // Get file count
        info->file_count = ti.files().num_files();
        
        info->error_code = LIBTORRENT_OK;
        
    } catch (const std::exception& e) {
        info->error_code = LIBTORRENT_ERROR_UNKNOWN;
        info->error_message = strdup(e.what());
    } catch (...) {
        info->error_code = LIBTORRENT_ERROR_UNKNOWN;
        info->error_message = strdup("Unknown exception");
    }
    
    return info;
}

void libtorrent_free_torrent_info(libtorrent_torrent_info_t* info) {
    if (!info) return;
    
    if (info->name) free(info->name);
    if (info->info_hash_hex) free(info->info_hash_hex);
    if (info->error_message) free(info->error_message);
    
    delete info;
}

} // extern "C"