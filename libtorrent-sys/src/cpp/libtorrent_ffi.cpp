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
#include <cstdio>
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
    } catch (const std::exception& e) {
        fprintf(stderr, "libtorrent_create_session: %s\n", e.what());
        delete s;
        return nullptr;
    } catch (...) {
        fprintf(stderr, "libtorrent_create_session: unknown exception\n");
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

libtorrent_error_t libtorrent_add_torrent_with_resume(
    libtorrent_session_t* session,
    const libtorrent_add_torrent_params_t* params,
    const char* save_path,
    size_t save_path_len,
    const uint8_t* resume_data,
    size_t resume_data_size,
    char** error_message
) {
    if (!session || !params || !params->torrent_data || params->torrent_size == 0) {
        if (error_message) *error_message = strdup("Invalid parameters");
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
        
        if (resume_data && resume_data_size > 0) {
            std::vector<char> resume_buf(resume_data_size);
            std::memcpy(resume_buf.data(), resume_data, resume_data_size);
            atp.resume_data = std::move(resume_buf);
        }

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
    } catch (const std::exception& e) {
        fprintf(stderr, "libtorrent_destroy_session: %s\n", e.what());
    } catch (...) {
        fprintf(stderr, "libtorrent_destroy_session: unknown exception\n");
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
    } catch (const std::exception& e) {
        fprintf(stderr, "get_info_hash_hex: %s\n", e.what());
        return nullptr;
    } catch (...) {
        fprintf(stderr, "get_info_hash_hex: unknown exception\n");
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
            
            char* type_name = strdup(a->what());
            if (!type_name) {
                fprintf(stderr, "libtorrent_pop_alerts: strdup failed for alert_type_name at index %zu\n", i);
                libtorrent_free_alert_list(&result);
                return {nullptr, 0};
            }
            result.alerts[i].alert_type_name = type_name;
            
            char* msg = strdup(a->message().c_str());
            if (!msg) {
                fprintf(stderr, "libtorrent_pop_alerts: strdup failed for message at index %zu\n", i);
                result.alerts[i].alert_type_name = nullptr;
                free(type_name);
                libtorrent_free_alert_list(&result);
                return {nullptr, 0};
            }
            result.alerts[i].message = msg;
            
            result.alerts[i].info_hash_hex = get_info_hash_hex(a);
            result.alerts[i].piece_index = get_piece_index(a);
        }
    } catch (const std::exception& e) {
        fprintf(stderr, "libtorrent_pop_alerts: %s\n", e.what());
        if (result.alerts) {
            libtorrent_free_alert_list(&result);
        }
        return {nullptr, 0};
    } catch (...) {
        fprintf(stderr, "libtorrent_pop_alerts: unknown exception\n");
        if (result.alerts) {
            libtorrent_free_alert_list(&result);
        }
        return {nullptr, 0};
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
    } catch (const std::exception& e) {
        fprintf(stderr, "libtorrent_wait_for_alert: %s\n", e.what());
        return 0;
    } catch (...) {
        fprintf(stderr, "libtorrent_wait_for_alert: unknown exception\n");
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

void libtorrent_set_alert_mask(libtorrent_session_t* session, uint64_t mask) {
    if (!session) return;
    
    try {
        libtorrent::settings_pack pack;
        pack.set_int(libtorrent::settings_pack::alert_mask, static_cast<int>(mask));
        session->session.apply_settings(pack);
    } catch (const std::exception& e) {
        fprintf(stderr, "libtorrent_set_alert_mask: %s\n", e.what());
    } catch (...) {
        fprintf(stderr, "libtorrent_set_alert_mask: unknown exception\n");
    }
}

static bool is_valid_hex_char(char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
}

static libtorrent::torrent_handle find_torrent_by_hash(libtorrent_session_t* session, const char* info_hash_hex) {
    if (!session || !info_hash_hex) {
        return libtorrent::torrent_handle();
    }
    
    try {
        std::string hex_str(info_hash_hex);
        if (hex_str.length() != 40) {
            return libtorrent::torrent_handle();
        }
        
        for (size_t i = 0; i < 40; i++) {
            if (!is_valid_hex_char(hex_str[i])) {
                fprintf(stderr, "find_torrent_by_hash: invalid hex character at position %zu\n", i);
                return libtorrent::torrent_handle();
            }
        }
        
        std::vector<char> hash_bytes;
        for (size_t i = 0; i < 40; i += 2) {
            int byte;
            sscanf(hex_str.c_str() + i, "%02x", &byte);
            hash_bytes.push_back(static_cast<char>(byte));
        }
        
        libtorrent::sha1_hash target_hash;
        std::memcpy(&target_hash, hash_bytes.data(), 20);
        
        std::vector<libtorrent::torrent_handle> handles = session->session.get_torrents();
        for (const auto& h : handles) {
            if (h.is_valid() && h.info_hash() == target_hash) {
                return h;
            }
        }
    } catch (const std::exception& e) {
        fprintf(stderr, "find_torrent_by_hash: %s\n", e.what());
    } catch (...) {
        fprintf(stderr, "find_torrent_by_hash: unknown exception\n");
    }
    
    return libtorrent::torrent_handle();
}

int libtorrent_find_torrent(libtorrent_session_t* session, const char* info_hash_hex) {
    auto handle = find_torrent_by_hash(session, info_hash_hex);
    return handle.is_valid() ? 1 : 0;
}

libtorrent_error_t libtorrent_resume_torrent(libtorrent_session_t* session, const char* info_hash_hex) {
    auto handle = find_torrent_by_hash(session, info_hash_hex);
    if (!handle.is_valid()) {
        return LIBTORRENT_ERROR_INVALID_DATA;
    }
    
    try {
        handle.unset_flags(libtorrent::torrent_flags::paused);
        return LIBTORRENT_OK;
    } catch (const std::exception& e) {
        fprintf(stderr, "libtorrent_resume_torrent: %s\n", e.what());
        return LIBTORRENT_ERROR_UNKNOWN;
    } catch (...) {
        fprintf(stderr, "libtorrent_resume_torrent: unknown exception\n");
        return LIBTORRENT_ERROR_UNKNOWN;
    }
}

libtorrent_error_t libtorrent_set_piece_deadline(libtorrent_session_t* session, const char* info_hash_hex, uint32_t piece_index, int deadline_ms) {
    auto handle = find_torrent_by_hash(session, info_hash_hex);
    if (!handle.is_valid()) {
        return LIBTORRENT_ERROR_INVALID_DATA;
    }
    
    try {
        handle.set_piece_deadline(piece_index, deadline_ms, libtorrent::torrent_handle::alert_when_available);
        return LIBTORRENT_OK;
    } catch (const std::exception& e) {
        fprintf(stderr, "libtorrent_set_piece_deadline: %s\n", e.what());
        return LIBTORRENT_ERROR_UNKNOWN;
    } catch (...) {
        fprintf(stderr, "libtorrent_set_piece_deadline: unknown exception\n");
        return LIBTORRENT_ERROR_UNKNOWN;
    }
}

int libtorrent_is_seeding(libtorrent_session_t* session, const char* info_hash_hex) {
    auto handle = find_torrent_by_hash(session, info_hash_hex);
    if (!handle.is_valid()) {
        return 0;
    }
    
    try {
        libtorrent::torrent_status status = handle.status();
        return status.is_seeding ? 1 : 0;
    } catch (const std::exception& e) {
        fprintf(stderr, "libtorrent_is_seeding: %s\n", e.what());
        return 0;
    } catch (...) {
        fprintf(stderr, "libtorrent_is_seeding: unknown exception\n");
        return 0;
    }
}

libtorrent_read_piece_result_t libtorrent_read_piece(libtorrent_session_t* session, const char* info_hash_hex, uint32_t piece_index) {
    libtorrent_read_piece_result_t result = {nullptr, 0, LIBTORRENT_OK, nullptr};
    
    auto handle = find_torrent_by_hash(session, info_hash_hex);
    if (!handle.is_valid()) {
        result.error_code = LIBTORRENT_ERROR_INVALID_DATA;
        result.error_message = strdup("Torrent not found");
        return result;
    }
    
    try {
        handle.read_piece(piece_index);
        
        // Use deadline-based waiting instead of busy-wait
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        
        while (std::chrono::steady_clock::now() < deadline) {
            auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - std::chrono::steady_clock::now()
            );
            if (remaining.count() <= 0) break;
            
            // Pop all alerts at once instead of waiting repeatedly
            std::vector<libtorrent::alert*> alerts;
            session->session.pop_alerts(&alerts);
            
            for (const auto* a : alerts) {
                if (auto* pra = libtorrent::alert_cast<libtorrent::read_piece_alert>(a)) {
                    if (pra->piece == static_cast<int>(piece_index)) {
                        if (pra->error) {
                            if (pra->error == libtorrent::errors::timed_out) {
                                result.error_code = LIBTORRENT_ERROR_TIMEOUT;
                            } else {
                                result.error_code = LIBTORRENT_ERROR_UNKNOWN;
                            }
                            result.error_message = strdup(pra->error.message().c_str());
                            return result;
                        }
                        
                        result.size = pra->size;
                        result.data = static_cast<uint8_t*>(malloc(result.size));
                        if (!result.data) {
                            result.error_code = LIBTORRENT_ERROR_ALLOCATION_FAILED;
                            result.error_message = strdup("Failed to allocate memory");
                            return result;
                        }
                        std::memcpy(result.data, pra->buffer.get(), result.size);
                        return result;
                    }
                }
            }
            
            // Wait for new alerts with remaining time
            auto timeout_ms = static_cast<int>(std::min(remaining.count(), (std::chrono::milliseconds::rep)1000));
            session->session.wait_for_alert(libtorrent::milliseconds(timeout_ms));
        }
        
        // Timeout - note: libtorrent will clean up the pending read_piece when the torrent is closed
        // There's no explicit cancel API for individual read_piece requests
        result.error_code = LIBTORRENT_ERROR_TIMEOUT;
        result.error_message = strdup("Timeout waiting for piece data");
        return result;
        
    } catch (const std::exception& e) {
        result.error_code = LIBTORRENT_ERROR_UNKNOWN;
        result.error_message = strdup(e.what());
        return result;
    } catch (...) {
        result.error_code = LIBTORRENT_ERROR_UNKNOWN;
        result.error_message = strdup("Unknown exception");
        return result;
    }
}

void libtorrent_free_read_piece_result(libtorrent_read_piece_result_t* result) {
    if (!result) return;
    
    if (result->data) {
        free(result->data);
        result->data = nullptr;
    }
    if (result->error_message) {
        free(result->error_message);
        result->error_message = nullptr;
    }
    result->size = 0;
}

libtorrent_info_hash_list_t libtorrent_get_torrents(libtorrent_session_t* session) {
    libtorrent_info_hash_list_t result = {nullptr, 0};
    
    if (!session) {
        return result;
    }
    
    try {
        std::vector<libtorrent::torrent_handle> handles = session->session.get_torrents();
        
        size_t valid_count = 0;
        for (const auto& h : handles) {
            if (h.is_valid()) {
                valid_count++;
            }
        }
        
        if (valid_count == 0) {
            return result;
        }
        
        result.info_hashes = static_cast<char**>(malloc(valid_count * sizeof(char*)));
        if (!result.info_hashes) {
            return result;
        }
        memset(result.info_hashes, 0, valid_count * sizeof(char*));
        result.count = valid_count;
        
        size_t out_idx = 0;
        for (const auto& h : handles) {
            if (!h.is_valid()) {
                continue;
            }
            
            std::string hash_str = h.info_hash().to_string();
            std::string hash_hex = libtorrent::aux::to_hex(libtorrent::span<const char>(hash_str.data(), hash_str.size()));
            char* hash_copy = strdup(hash_hex.c_str());
            if (!hash_copy) {
                libtorrent_free_info_hash_list(&result);
                return {nullptr, 0};
            }
            result.info_hashes[out_idx++] = hash_copy;
        }
    } catch (const std::exception& e) {
        fprintf(stderr, "libtorrent_get_torrents: %s\n", e.what());
        if (result.info_hashes) {
            libtorrent_free_info_hash_list(&result);
        }
        return {nullptr, 0};
    } catch (...) {
        fprintf(stderr, "libtorrent_get_torrents: unknown exception\n");
        if (result.info_hashes) {
            libtorrent_free_info_hash_list(&result);
        }
        return {nullptr, 0};
    }
    
    return result;
}

void libtorrent_free_info_hash_list(libtorrent_info_hash_list_t* list) {
    if (!list || !list->info_hashes) return;
    
    for (size_t i = 0; i < list->count; i++) {
        if (list->info_hashes[i]) {
            free(list->info_hashes[i]);
        }
    }
    free(list->info_hashes);
    list->info_hashes = nullptr;
    list->count = 0;
}

libtorrent_error_t libtorrent_save_resume_data(libtorrent_session_t* session, const char* info_hash_hex) {
    auto handle = find_torrent_by_hash(session, info_hash_hex);
    if (!handle.is_valid()) {
        return LIBTORRENT_ERROR_INVALID_DATA;
    }
    
    try {
        handle.save_resume_data();
        return LIBTORRENT_OK;
    } catch (const std::exception& e) {
        fprintf(stderr, "libtorrent_save_resume_data: %s\n", e.what());
        return LIBTORRENT_ERROR_UNKNOWN;
    } catch (...) {
        fprintf(stderr, "libtorrent_save_resume_data: unknown exception\n");
        return LIBTORRENT_ERROR_UNKNOWN;
    }
}

libtorrent_error_t libtorrent_pause_torrent(libtorrent_session_t* session, const char* info_hash_hex) {
    auto handle = find_torrent_by_hash(session, info_hash_hex);
    if (!handle.is_valid()) {
        return LIBTORRENT_ERROR_INVALID_DATA;
    }
    
    try {
        handle.pause();
        return LIBTORRENT_OK;
    } catch (const std::exception& e) {
        fprintf(stderr, "libtorrent_pause_torrent: %s\n", e.what());
        return LIBTORRENT_ERROR_UNKNOWN;
    } catch (...) {
        fprintf(stderr, "libtorrent_pause_torrent: unknown exception\n");
        return LIBTORRENT_ERROR_UNKNOWN;
    }
}

void libtorrent_restore_signal_handlers() {
    // No-op on C++ side - signal handlers are managed in Rust
}

} // extern "C"