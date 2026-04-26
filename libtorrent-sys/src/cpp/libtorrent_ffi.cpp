#include "libtorrent_ffi.h"
#include <libtorrent/bdecode.hpp>
#include <libtorrent/entry.hpp>
#include <libtorrent/torrent_info.hpp>
#include <libtorrent/hex.hpp>
#include <libtorrent/file_storage.hpp>
#include <libtorrent/session.hpp>
#include <libtorrent/add_torrent_params.hpp>
#include <cstring>
#include <string>
#include <vector>
#include <memory>

extern "C" {

struct libtorrent_session_t {
    libtorrent::session session;
};

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
        
        // Get file list
        if (info->file_count > 0) {
            info->files = static_cast<libtorrent_file_entry_t*>(malloc(info->file_count * sizeof(libtorrent_file_entry_t)));
            if (!info->files) {
                throw std::bad_alloc();
            }
            memset(info->files, 0, info->file_count * sizeof(libtorrent_file_entry_t));
            
            const auto& files = ti.files();
            for (uint32_t i = 0; i < info->file_count; i++) {
                // Get file path
                std::string file_path = files.file_path(i);
                info->files[i].path = strdup(file_path.c_str());
                
                // Get file size
                info->files[i].size = files.file_size(i);
            }
        } else {
            info->files = nullptr;
        }
        
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
    
    // Free file list
    if (info->files) {
        for (uint32_t i = 0; i < info->file_count; i++) {
            if (info->files[i].path) {
                free(info->files[i].path);
            }
        }
        free(info->files);
    }
    
    if (info->error_message) free(info->error_message);
    
    delete info;
}

libtorrent_session_t* libtorrent_create_session() {
    auto* s = new libtorrent_session_t();
    try {
        libtorrent::settings_pack pack;
        pack.set_int(libtorrent::settings_pack::alert_mask, 0);
        s->session.apply_settings(pack);
    } catch (...) {
        delete s;
        return nullptr;
    }
    return s;
}

libtorrent_error_t libtorrent_add_torrent(
    libtorrent_session_t* session,
    const libtorrent_add_torrent_params_t* params,
    char** error_message)
{
    return libtorrent_add_torrent_ex(session, params, "/tmp/torrentfs", 12, error_message);
}

libtorrent_error_t libtorrent_add_torrent_ex(
    libtorrent_session_t* session,
    const libtorrent_add_torrent_params_t* params,
    const char* save_path,
    size_t save_path_len,
    char** error_message)
{
    if (!session || !params) {
        if (error_message) *error_message = strdup("Null pointer");
        return LIBTORRENT_ERROR_INVALID_DATA;
    }

    try {
        libtorrent::error_code ec;
        libtorrent::span<const char> buf(
            reinterpret_cast<const char*>(params->torrent_data),
            params->torrent_size);
        libtorrent::bdecode_node node = libtorrent::bdecode(buf, ec);
        if (ec) {
            if (error_message) *error_message = strdup(ec.message().c_str());
            return LIBTORRENT_ERROR_PARSE_FAILED;
        }

        libtorrent::torrent_info ti(node, ec);
        if (ec) {
            if (error_message) *error_message = strdup(ec.message().c_str());
            return LIBTORRENT_ERROR_PARSE_FAILED;
        }

        libtorrent::add_torrent_params atp;
        atp.ti = std::make_shared<libtorrent::torrent_info>(ti);
        atp.save_path = std::string(save_path, save_path_len);
        atp.flags &= ~libtorrent::torrent_flags::auto_managed;
        atp.flags |= libtorrent::torrent_flags::paused;
        atp.flags |= libtorrent::torrent_flags::upload_mode;

        libtorrent::torrent_handle handle = session->session.add_torrent(atp, ec);
        if (ec) {
            if (error_message) *error_message = strdup(ec.message().c_str());
            return LIBTORRENT_ERROR_PARSE_FAILED;
        }

        return LIBTORRENT_OK;
    } catch (const std::exception& e) {
        if (error_message) *error_message = strdup(e.what());
        return LIBTORRENT_ERROR_UNKNOWN;
    } catch (...) {
        if (error_message) *error_message = strdup("Unknown exception");
        return LIBTORRENT_ERROR_UNKNOWN;
    }
}

void libtorrent_destroy_session(libtorrent_session_t* session) {
    if (!session) return;
    try {
        session->session.abort();
    } catch (...) {
        // Ignore abort errors
    }
    delete session;
}

} // extern "C"