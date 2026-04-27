#include "libtorrent_ffi.h"
#include <libtorrent/bdecode.hpp>
#include <libtorrent/entry.hpp>
#include <libtorrent/torrent_info.hpp>
#include <libtorrent/hex.hpp>
#include <libtorrent/file_storage.hpp>
#include <libtorrent/session.hpp>
#include <libtorrent/add_torrent_params.hpp>
#include <libtorrent/alert_types.hpp>
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
        
        // Get piece size
        info->piece_size = ti.files().piece_length();
        
        // Get file count
        info->file_count = ti.files().num_files();
        
        // Get file list
        if (info->file_count > 0) {
            const auto& fs = ti.files();
            uint32_t piece_len = fs.piece_length();

            info->files = static_cast<libtorrent_file_entry_t*>(malloc(info->file_count * sizeof(libtorrent_file_entry_t)));
            if (!info->files) {
                throw std::bad_alloc();
            }
            memset(info->files, 0, info->file_count * sizeof(libtorrent_file_entry_t));
            
            for (uint32_t i = 0; i < info->file_count; i++) {
                // Get file path
                std::string file_path = fs.file_path(i);
                info->files[i].path = strdup(file_path.c_str());
                
                // Get file size
                info->files[i].size = fs.file_size(i);
                
                // Get file offset within the torrent
                info->files[i].offset = fs.file_offset(i);
                
                // Compute piece range from offset and size
                uint64_t off = fs.file_offset(i);
                uint64_t sz = fs.file_size(i);
                if (piece_len > 0) {
                    info->files[i].first_piece = static_cast<uint32_t>(off / piece_len);
                    info->files[i].last_piece = (sz > 0)
                        ? static_cast<uint32_t>((off + sz - 1) / piece_len)
                        : info->files[i].first_piece;
                } else {
                    info->files[i].first_piece = 0;
                    info->files[i].last_piece = 0;
                }
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
        pack.set_int(libtorrent::settings_pack::alert_mask,
            libtorrent::alert_category::error
            | libtorrent::alert_category::status
            | libtorrent::alert_category::piece_progress
            | libtorrent::alert_category::storage);
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

static libtorrent_alert_type_t get_alert_type(const libtorrent::alert* a) {
    using namespace libtorrent;
    
    if (alert_cast<piece_finished_alert>(a)) {
        return LIBTORRENT_ALERT_PIECE_FINISHED;
    }
    if (alert_cast<torrent_finished_alert>(a)) {
        return LIBTORRENT_ALERT_TORRENT_FINISHED;
    }
    if (alert_cast<save_resume_data_alert>(a)) {
        return LIBTORRENT_ALERT_SAVE_RESUME_DATA;
    }
    if (alert_cast<add_torrent_alert>(a)) {
        return LIBTORRENT_ALERT_ADD_TORRENT;
    }
    if (alert_cast<metadata_received_alert>(a)) {
        return LIBTORRENT_ALERT_METADATA_RECEIVED;
    }
    if (alert_cast<read_piece_alert>(a)) {
        return LIBTORRENT_ALERT_PIECE_READ;
    }
    return LIBTORRENT_ALERT_UNKNOWN;
}

static char* get_info_hash_hex(const libtorrent::alert* a) {
    using namespace libtorrent;
    
    const torrent_alert* ta = alert_cast<torrent_alert>(a);
    if (!ta) {
        return nullptr;
    }
    
    try {
        std::string hash_str = ta->handle.info_hash().to_string();
        std::string hash_hex = aux::to_hex(libtorrent::span<const char>(hash_str.data(), hash_str.size()));
        return strdup(hash_hex.c_str());
    } catch (...) {
        return nullptr;
    }
}

static uint32_t get_piece_index(const libtorrent::alert* a) {
    using namespace libtorrent;
    
    if (auto* pfa = alert_cast<piece_finished_alert>(a)) {
        return pfa->piece_index;
    }
    if (auto* pra = alert_cast<read_piece_alert>(a)) {
        return pra->piece;
    }
    return 0;
}

libtorrent_alert_list_t libtorrent_pop_alerts(libtorrent_session_t* session) {
    libtorrent_alert_list_t result = {nullptr, 0};
    
    if (!session) {
        return result;
    }
    
    try {
        std::vector<libtorrent::alert*> alerts;
        session->session.pop_alerts(&alerts);
        
        if (alerts.empty()) {
            return result;
        }
        
        result.alerts = static_cast<libtorrent_alert_t*>(
            malloc(alerts.size() * sizeof(libtorrent_alert_t)));
        if (!result.alerts) {
            return result;
        }
        memset(result.alerts, 0, alerts.size() * sizeof(libtorrent_alert_t));
        result.count = alerts.size();
        
        for (size_t i = 0; i < alerts.size(); i++) {
            const libtorrent::alert* a = alerts[i];
            result.alerts[i].type = get_alert_type(a);
            result.alerts[i].alert_type_name = strdup(a->what());
            result.alerts[i].message = strdup(a->message().c_str());
            result.alerts[i].info_hash_hex = get_info_hash_hex(a);
            result.alerts[i].piece_index = get_piece_index(a);
        }
    } catch (...) {
        // Return empty list on error
    }
    
    return result;
}

int libtorrent_wait_for_alert(libtorrent_session_t* session, int timeout_ms) {
    if (!session) {
        return 0;
    }
    
    try {
        libtorrent::time_duration timeout = libtorrent::milliseconds(timeout_ms);
        libtorrent::alert const* a = session->session.wait_for_alert(timeout);
        return (a != nullptr) ? 1 : 0;
    } catch (...) {
        return 0;
    }
}

void libtorrent_free_alert_list(libtorrent_alert_list_t* list) {
    if (!list || !list->alerts) return;
    
    for (size_t i = 0; i < list->count; i++) {
        if (list->alerts[i].alert_type_name) {
            free(list->alerts[i].alert_type_name);
        }
        if (list->alerts[i].message) {
            free(list->alerts[i].message);
        }
        if (list->alerts[i].info_hash_hex) {
            free(list->alerts[i].info_hash_hex);
        }
    }
    free(list->alerts);
    list->alerts = nullptr;
    list->count = 0;
}

} // extern "C"