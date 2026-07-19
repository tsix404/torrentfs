#include "libtorrent_wrapper.h"
#include <libtorrent/torrent_info.hpp>
#include <libtorrent/file_storage.hpp>
#include <libtorrent/sha1_hash.hpp>
#include <libtorrent/bdecode.hpp>
#include <libtorrent/span.hpp>
#include <libtorrent/session.hpp>
#include <libtorrent/add_torrent_params.hpp>
#include <libtorrent/torrent_handle.hpp>
#include <libtorrent/torrent_status.hpp>
#include <libtorrent/alert_types.hpp>
#include <libtorrent/torrent_flags.hpp>
#include <libtorrent/settings_pack.hpp>
#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <cctype>
#include <map>

struct lt_error {
    std::string message;
    int code;
};

struct lt_file_entry_inner {
    std::string path;
    uint64_t size;
};

struct lt_torrent_metadata {
    std::string name;
    uint64_t total_size;
    uint32_t piece_length;
    uint32_t num_pieces;
    uint32_t num_files;
    std::vector<lt_file_entry_inner> files;
    lt::sha1_hash info_hash;
};

struct lt_session_wrapper {
    lt::session* session;
    std::mutex mutex;
};

lt_torrent_info_t lt_torrent_info_create(const char* filepath, lt_error_t* error) {
    try {
        auto ti = new lt::torrent_info(std::string(filepath));
        return static_cast<lt_torrent_info_t>(ti);
    } catch (const lt::system_error& e) {
        if (error) {
            error->code = e.code().value();
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return nullptr;
    } catch (const std::exception& e) {
        if (error) {
            error->code = -1;
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return nullptr;
    }
}

lt_torrent_info_t lt_torrent_info_create_from_buffer(const uint8_t* data, size_t size, lt_error_t* error) {
    try {
        lt::error_code ec;
        lt::torrent_info ti(reinterpret_cast<const char*>(data), static_cast<int>(size), ec);
        if (ec) {
            if (error) {
                error->code = ec.value();
                static thread_local std::string err_msg;
                err_msg = ec.message();
                error->message = err_msg.c_str();
            }
            return nullptr;
        }
        return static_cast<lt_torrent_info_t>(new lt::torrent_info(std::move(ti)));
    } catch (const lt::system_error& e) {
        if (error) {
            error->code = e.code().value();
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return nullptr;
    } catch (const std::exception& e) {
        if (error) {
            error->code = -1;
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return nullptr;
    }
}

void lt_torrent_info_destroy(lt_torrent_info_t info) {
    if (info) {
        auto ti = static_cast<lt::torrent_info*>(info);
        delete ti;
    }
}

const char* lt_torrent_info_name(lt_torrent_info_t info) {
    if (!info) return nullptr;
    auto ti = static_cast<lt::torrent_info*>(info);
    static thread_local std::string name;
    name = ti->name();
    return name.c_str();
}

uint64_t lt_torrent_info_total_size(lt_torrent_info_t info) {
    if (!info) return 0;
    auto ti = static_cast<lt::torrent_info*>(info);
    return ti->total_size();
}

uint32_t lt_torrent_info_piece_length(lt_torrent_info_t info) {
    if (!info) return 0;
    auto ti = static_cast<lt::torrent_info*>(info);
    return static_cast<uint32_t>(ti->piece_length());
}

uint32_t lt_torrent_info_num_pieces(lt_torrent_info_t info) {
    if (!info) return 0;
    auto ti = static_cast<lt::torrent_info*>(info);
    return static_cast<uint32_t>(ti->num_pieces());
}

uint32_t lt_torrent_info_num_files(lt_torrent_info_t info) {
    if (!info) return 0;
    auto ti = static_cast<lt::torrent_info*>(info);
    return static_cast<uint32_t>(ti->num_files());
}

int lt_torrent_info_get_files(lt_torrent_info_t info, lt_file_entry_t** files, uint32_t* count) {
    if (!info || !files || !count) return -1;

    auto ti = static_cast<lt::torrent_info*>(info);
    const lt::file_storage& fs = ti->files();
    auto n = static_cast<uint32_t>(fs.num_files());

    auto* out = static_cast<lt_file_entry_t*>(std::calloc(n, sizeof(lt_file_entry_t)));
    if (!out) return -1;

    static thread_local std::vector<std::string> paths;
    paths.clear();
    paths.reserve(n);

    for (lt::file_index_t i(0); i < fs.end_file(); ++i) {
        auto idx = static_cast<int>(i);
        paths.emplace_back(fs.file_path(i));
        out[idx].path = paths.back().c_str();
        out[idx].size = static_cast<uint64_t>(fs.file_size(i));
    }

    *files = out;
    *count = n;
    return 0;
}

void lt_files_free(lt_file_entry_t* files) {
    std::free(files);
}

int lt_torrent_info_get_info_hash(lt_torrent_info_t info, uint8_t* hash_out) {
    if (!info || !hash_out) return -1;
    auto ti = static_cast<lt::torrent_info*>(info);
    auto h = ti->info_hashes();
    auto sha1 = h.get_best();
    std::memcpy(hash_out, sha1.data(), 20);
    return 0;
}

lt_torrent_metadata_t* lt_torrent_info_get_metadata(lt_torrent_info_t info) {
    if (!info) return nullptr;

    auto ti = static_cast<lt::torrent_info*>(info);
    auto* meta = new lt_torrent_metadata();

    meta->name = ti->name();
    meta->total_size = ti->total_size();
    meta->piece_length = static_cast<uint32_t>(ti->piece_length());
    meta->num_pieces = static_cast<uint32_t>(ti->num_pieces());
    meta->num_files = static_cast<uint32_t>(ti->num_files());

    const lt::file_storage& fs = ti->files();
    for (lt::file_index_t i(0); i < fs.end_file(); ++i) {
        meta->files.push_back({fs.file_path(i), static_cast<uint64_t>(fs.file_size(i))});
    }

    auto h = ti->info_hashes().get_best();
    meta->info_hash = h;

    return reinterpret_cast<lt_torrent_metadata_t*>(meta);
}

void lt_torrent_metadata_destroy(lt_torrent_metadata_t* metadata) {
    if (metadata) {
        auto* meta = reinterpret_cast<lt_torrent_metadata*>(metadata);
        delete meta;
    }
}

lt_session_t lt_session_create(const char* listen_interface, lt_error_t* error) {
    try {
        lt::settings_pack settings;
        if (listen_interface && strlen(listen_interface) > 0) {
            settings.set_str(lt::settings_pack::listen_interfaces, listen_interface);
        }
        settings.set_int(lt::settings_pack::alert_mask, 
            lt::alert_category::error | lt::alert_category::status);
        auto wrapper = new lt_session_wrapper();
        wrapper->session = new lt::session(settings);
        return static_cast<lt_session_t>(wrapper);
    } catch (const std::exception& e) {
        if (error) {
            error->code = -1;
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return nullptr;
    }
}

void lt_session_destroy(lt_session_t session) {
    if (session) {
        auto wrapper = static_cast<lt_session_wrapper*>(session);
        delete wrapper->session;
        delete wrapper;
    }
}

lt_torrent_handle_t lt_session_add_torrent(lt_session_t session, lt_torrent_info_t info, const char* save_path, lt_error_t* error) {
    if (!session || !info) {
        if (error) {
            error->code = -1;
            error->message = "Invalid session or torrent info";
        }
        return nullptr;
    }

    try {
        auto wrapper = static_cast<lt_session_wrapper*>(session);
        auto ti = static_cast<lt::torrent_info*>(info);

        lt::add_torrent_params params;
        params.ti = std::make_shared<lt::torrent_info>(*ti);
        if (save_path) {
            params.save_path = save_path;
        } else {
            params.save_path = "/tmp/torrentfs-cache";
        }

        std::lock_guard<std::mutex> lock(wrapper->mutex);
        auto handle = wrapper->session->add_torrent(params);
        return static_cast<lt_torrent_handle_t>(new lt::torrent_handle(handle));
    } catch (const std::exception& e) {
        if (error) {
            error->code = -1;
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return nullptr;
    }
}

void lt_session_remove_torrent(lt_session_t session, lt_torrent_handle_t handle, int remove_files) {
    if (session && handle) {
        auto wrapper = static_cast<lt_session_wrapper*>(session);
        auto h = static_cast<lt::torrent_handle*>(handle);
        lt::remove_flags_t flags = remove_files ? lt::session::delete_files : lt::remove_flags_t{};
        std::lock_guard<std::mutex> lock(wrapper->mutex);
        wrapper->session->remove_torrent(*h, flags);
        delete h;
    }
}

void lt_torrent_handle_destroy(lt_torrent_handle_t handle) {
    if (handle) {
        auto h = static_cast<lt::torrent_handle*>(handle);
        delete h;
    }
}

int lt_torrent_handle_is_valid(lt_torrent_handle_t handle) {
    if (!handle) return 0;
    auto h = static_cast<lt::torrent_handle*>(handle);
    return h->is_valid() ? 1 : 0;
}

int lt_torrent_handle_status(lt_torrent_handle_t handle, int* state, float* progress, uint64_t* total_done, uint64_t* total,
    int64_t* dl_rate, int64_t* ul_rate, int64_t* total_dl, int64_t* total_ul,
    int32_t* peers, int32_t* seeds) {
    if (!handle || !state || !progress || !total_done || !total) return -1;
    
    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) return -1;
    
    auto status = h->status();
    *state = static_cast<int>(status.state);
    *progress = status.progress;
    *total_done = static_cast<uint64_t>(status.total_done);
    *total = static_cast<uint64_t>(status.total);
    if (dl_rate) *dl_rate = static_cast<int64_t>(status.download_rate);
    if (ul_rate) *ul_rate = static_cast<int64_t>(status.upload_rate);
    if (total_dl) *total_dl = static_cast<int64_t>(status.total_download);
    if (total_ul) *total_ul = static_cast<int64_t>(status.total_upload);
    if (peers) *peers = status.num_peers;
    if (seeds) *seeds = status.num_seeds;
    return 0;
}

int lt_torrent_handle_read_piece(lt_session_t session, lt_torrent_handle_t handle, int piece_index, uint8_t** data_out, size_t* size_out, lt_error_t* error) {
    if (!session || !handle || !data_out || !size_out) {
        if (error) {
            error->code = -1;
            error->message = "Invalid arguments";
        }
        return -1;
    }

    auto wrapper = static_cast<lt_session_wrapper*>(session);
    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) {
        if (error) {
            error->code = -1;
            error->message = "Invalid torrent handle";
        }
        return -1;
    }

    try {
        h->read_piece(lt::piece_index_t(piece_index));
        
        auto start = std::chrono::steady_clock::now();
        auto timeout = std::chrono::seconds(60);
        
        while (true) {
            auto now = std::chrono::steady_clock::now();
            if (now - start > timeout) {
                if (error) {
                    error->code = -1;
                    error->message = "Timeout waiting for piece data";
                }
                return -1;
            }
            
            std::vector<lt::alert*> alerts;
            {
                std::lock_guard<std::mutex> lock(wrapper->mutex);
                wrapper->session->pop_alerts(&alerts);
            }
            
            for (auto* alert : alerts) {
                if (auto* rp = lt::alert_cast<lt::read_piece_alert>(alert)) {
                    if (rp->handle == *h && static_cast<int>(rp->piece) == piece_index) {
                        if (rp->error) {
                            if (error) {
                                error->code = rp->error.value();
                                static thread_local std::string err_msg;
                                err_msg = rp->error.message();
                                error->message = err_msg.c_str();
                            }
                            return -1;
                        }
                        
                        size_t sz = rp->size;
                        if (sz == 0) {
                            *size_out = 0;
                            *data_out = nullptr;
                            return 0;
                        }
                        
                        *size_out = sz;
                        *data_out = static_cast<uint8_t*>(std::malloc(sz));
                        if (*data_out) {
                            std::memcpy(*data_out, rp->buffer.get(), sz);
                            return 0;
                        }
                        return -1;
                    }
                }
            }
            
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    } catch (const std::exception& e) {
        if (error) {
            error->code = -1;
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return -1;
    }
}

void lt_piece_data_free(uint8_t* data) {
    if (data) {
        std::free(data);
    }
}

int lt_torrent_handle_get_piece_info(lt_torrent_handle_t handle, int file_index, int64_t* first_piece, int64_t* num_pieces, int64_t* file_offset) {
    if (!handle || !first_piece || !num_pieces || !file_offset) return -1;
    
    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) return -1;
    
    auto t = h->torrent_file();
    if (!t) return -1;
    
    const auto& fs = t->files();
    
    lt::file_index_t fi(file_index);
    if (fi >= fs.end_file()) return -1;
    
    auto file_size = fs.file_size(fi);
    auto piece_length = t->piece_length();
    auto file_offset_val = fs.file_offset(fi);
    
    int64_t start_piece = file_offset_val / piece_length;
    int64_t end_offset = file_offset_val + file_size;
    int64_t end_piece = (end_offset + piece_length - 1) / piece_length;
    
    *first_piece = start_piece;
    *num_pieces = end_piece - start_piece;
    *file_offset = file_offset_val;
    
    return 0;
}

int lt_torrent_handle_get_torrent_info(lt_torrent_handle_t handle, int64_t* piece_length, int64_t* num_pieces) {
    if (!handle || !piece_length || !num_pieces) return -1;
    
    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) return -1;
    
    auto t = h->torrent_file();
    if (!t) return -1;
    
    *piece_length = t->piece_length();
    *num_pieces = t->num_pieces();
    
    return 0;
}

int lt_torrent_handle_have_piece(lt_torrent_handle_t handle, int piece_index) {
    if (!handle) return 0;
    
    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) return 0;
    
    auto status = h->status();
    if (static_cast<int>(status.pieces.size()) <= piece_index) return 0;
    
    return status.pieces[lt::piece_index_t(piece_index)] ? 1 : 0;
}

// Minimal JSON parser for flat settings objects
// Handles: {"key1": "str", "key2": 123, "key3": true, "key4": false}
static void skip_json_ws(const char*& p) {
    while (*p && std::isspace(static_cast<unsigned char>(*p))) p++;
}

static std::string parse_json_string(const char*& p) {
    // p points to opening '"'
    p++;
    std::string result;
    while (*p && *p != '"') {
        if (*p == '\\' && *(p + 1)) {
            p++;
            char c = *p;
            switch (c) {
                case 'n': result += '\n'; break;
                case 't': result += '\t'; break;
                case 'r': result += '\r'; break;
                case '\\': result += '\\'; break;
                case '"': result += '"'; break;
                default: result += c; break;
            }
        } else {
            result += *p;
        }
        p++;
    }
    if (*p == '"') p++;
    return result;
}

static int64_t parse_json_int(const char*& p) {
    bool negative = false;
    if (*p == '-') { negative = true; p++; }
    int64_t val = 0;
    while (*p && std::isdigit(static_cast<unsigned char>(*p))) {
        val = val * 10 + (*p - '0');
        p++;
    }
    return negative ? -val : val;
}

static void apply_str_setting(lt::settings_pack& pack, const std::string& key, const std::string& val) {
    // Phase 1: core string settings
    if (key == "listen_interfaces") {
        pack.set_str(lt::settings_pack::listen_interfaces, val);
    } else if (key == "outgoing_interfaces") {
        pack.set_str(lt::settings_pack::outgoing_interfaces, val);
    } else if (key == "user_agent") {
        pack.set_str(lt::settings_pack::user_agent, val);
    } else if (key == "peer_fingerprint") {
        pack.set_str(lt::settings_pack::peer_fingerprint, val);
    }
    // Unknown keys are silently ignored
}

static void apply_int_setting(lt::settings_pack& pack, const std::string& key, int val) {
    // Phase 1: core integer settings
    if (key == "max_connections") {
        // libtorrent 2.0: max_connections is not directly settable via settings_pack
        // silently ignored
    } else if (key == "max_uploads") {
        // libtorrent 2.0: max_uploads is not directly settable via settings_pack
        // silently ignored
    } else if (key == "connection_speed") {
        pack.set_int(lt::settings_pack::connection_speed, val);
    } else if (key == "peer_connect_timeout") {
        pack.set_int(lt::settings_pack::peer_connect_timeout, val);
    } else if (key == "listen_queue_size") {
        pack.set_int(lt::settings_pack::listen_queue_size, val);
    } else if (key == "min_reconnect_time") {
        pack.set_int(lt::settings_pack::min_reconnect_time, val);
    } else if (key == "max_peerlist_size") {
        pack.set_int(lt::settings_pack::max_peerlist_size, val);
    } else if (key == "max_paused_peerlist_size") {
        pack.set_int(lt::settings_pack::max_paused_peerlist_size, val);
    } else if (key == "dht_announce_interval") {
        pack.set_int(lt::settings_pack::dht_announce_interval, val);
    } else if (key == "max_dht_items") {
        pack.set_int(lt::settings_pack::dht_max_dht_items, val);
    } else if (key == "max_active_dht_limit") {
        pack.set_int(lt::settings_pack::active_dht_limit, val);
    } else if (key == "download_rate_limit") {
        pack.set_int(lt::settings_pack::download_rate_limit, val);
    } else if (key == "upload_rate_limit") {
        pack.set_int(lt::settings_pack::upload_rate_limit, val);
    } else if (key == "disk_io_write_mode") {
        pack.set_int(lt::settings_pack::disk_io_write_mode, val);
    } else if (key == "disk_io_read_mode") {
        pack.set_int(lt::settings_pack::disk_io_read_mode, val);
    } else if (key == "file_pool_size") {
        pack.set_int(lt::settings_pack::file_pool_size, val);
    } else if (key == "max_queued_disk_bytes") {
        pack.set_int(lt::settings_pack::max_queued_disk_bytes, val);
    } else if (key == "max_queued_disk_bytes_low_watermark") {
        // libtorrent 2.0: not available, silently ignored
    } else if (key == "cache_size") {
        // libtorrent 2.0: cache_size removed, silently ignored
    } else if (key == "cache_expiry") {
        // libtorrent 2.0: cache_expiry removed, silently ignored
    } else if (key == "default_cache_min_age") {
        // libtorrent 2.0: default_cache_min_age removed, silently ignored
    } else if (key == "whole_pieces_threshold") {
        pack.set_int(lt::settings_pack::whole_pieces_threshold, val);
    } else if (key == "piece_timeout") {
        pack.set_int(lt::settings_pack::piece_timeout, val);
    } else if (key == "request_timeout") {
        pack.set_int(lt::settings_pack::request_timeout, val);
    } else if (key == "max_out_request_queue") {
        pack.set_int(lt::settings_pack::max_out_request_queue, val);
    } else if (key == "max_allowed_in_request_queue") {
        pack.set_int(lt::settings_pack::max_allowed_in_request_queue, val);
    } else if (key == "max_suggest_pieces") {
        pack.set_int(lt::settings_pack::max_suggest_pieces, val);
    } else if (key == "seeding_piece_quota") {
        pack.set_int(lt::settings_pack::seeding_piece_quota, val);
    } else if (key == "max_sparse_regions") {
        // libtorrent 2.0: not available, silently ignored
    } else if (key == "peer_timeout") {
        pack.set_int(lt::settings_pack::peer_timeout, val);
    } else if (key == "urlseed_timeout") {
        pack.set_int(lt::settings_pack::urlseed_timeout, val);
    } else if (key == "urlseed_pipeline_size") {
        pack.set_int(lt::settings_pack::urlseed_pipeline_size, val);
    } else if (key == "stop_tracker_timeout") {
        pack.set_int(lt::settings_pack::stop_tracker_timeout, val);
    } else if (key == "tracker_completion_timeout") {
        pack.set_int(lt::settings_pack::tracker_completion_timeout, val);
    } else if (key == "tracker_receive_timeout") {
        pack.set_int(lt::settings_pack::tracker_receive_timeout, val);
    } else if (key == "inactivity_timeout") {
        pack.set_int(lt::settings_pack::inactivity_timeout, val);
    } else if (key == "tracker_backoff") {
        pack.set_int(lt::settings_pack::tracker_backoff, val);
    } else if (key == "tracker_maximum_response_length") {
        pack.set_int(lt::settings_pack::tracker_maximum_response_length, val);
    } else if (key == "min_announce_interval") {
        pack.set_int(lt::settings_pack::min_announce_interval, val);
    } else if (key == "udp_tracker_token_expiry") {
        pack.set_int(lt::settings_pack::udp_tracker_token_expiry, val);
    } else if (key == "choking_algorithm") {
        pack.set_int(lt::settings_pack::choking_algorithm, val);
    } else if (key == "seed_choking_algorithm") {
        pack.set_int(lt::settings_pack::seed_choking_algorithm, val);
    } else if (key == "mixed_mode_algorithm") {
        pack.set_int(lt::settings_pack::mixed_mode_algorithm, val);
    } else if (key == "suggest_mode") {
        pack.set_int(lt::settings_pack::suggest_mode, val);
    } else if (key == "active_downloads") {
        pack.set_int(lt::settings_pack::active_downloads, val);
    } else if (key == "active_seeds") {
        pack.set_int(lt::settings_pack::active_seeds, val);
    } else if (key == "active_checking") {
        pack.set_int(lt::settings_pack::active_checking, val);
    } else if (key == "active_limit") {
        pack.set_int(lt::settings_pack::active_limit, val);
    } else if (key == "active_tracker_limit") {
        pack.set_int(lt::settings_pack::active_tracker_limit, val);
    } else if (key == "active_lsd_limit") {
        pack.set_int(lt::settings_pack::active_lsd_limit, val);
    } else if (key == "active_dht_limit") {
        pack.set_int(lt::settings_pack::active_dht_limit, val);
    } else if (key == "auto_manage_interval") {
        pack.set_int(lt::settings_pack::auto_manage_interval, val);
    } else if (key == "auto_manage_startup") {
        pack.set_int(lt::settings_pack::auto_manage_startup, val);
    } else if (key == "share_ratio_limit") {
        pack.set_int(lt::settings_pack::share_ratio_limit, val);
    } else if (key == "seed_time_ratio_limit") {
        pack.set_int(lt::settings_pack::seed_time_ratio_limit, val);
    } else if (key == "seed_time_limit") {
        pack.set_int(lt::settings_pack::seed_time_limit, val);
    } else if (key == "encryption_policy") {
        pack.set_int(lt::settings_pack::out_enc_policy, val);
    } else if (key == "allowed_encryption_level") {
        pack.set_int(lt::settings_pack::allowed_enc_level, val);
    } else if (key == "ssl_listen") {
        // libtorrent 2.0: ssl_listen removed, silently ignored
    } else if (key == "proxy_port") {
        pack.set_int(lt::settings_pack::proxy_port, val);
    } else if (key == "alert_queue_size") {
        pack.set_int(lt::settings_pack::alert_queue_size, val);
    } else if (key == "aio_threads") {
        pack.set_int(lt::settings_pack::aio_threads, val);
    } else if (key == "network_threads") {
        // libtorrent 2.0: network_threads removed, silently ignored
    } else if (key == "checking_mem_usage") {
        pack.set_int(lt::settings_pack::checking_mem_usage, val);
    } else if (key == "tick_interval") {
        pack.set_int(lt::settings_pack::tick_interval, val);
    } else if (key == "send_buffer_watermark") {
        pack.set_int(lt::settings_pack::send_buffer_watermark, val);
    } else if (key == "send_buffer_watermark_factor") {
        pack.set_int(lt::settings_pack::send_buffer_watermark_factor, val);
    } else if (key == "send_buffer_low_watermark") {
        pack.set_int(lt::settings_pack::send_buffer_low_watermark, val);
    } else if (key == "recv_socket_buffer_size") {
        pack.set_int(lt::settings_pack::recv_socket_buffer_size, val);
    } else if (key == "send_socket_buffer_size") {
        pack.set_int(lt::settings_pack::send_socket_buffer_size, val);
    } else if (key == "optimistic_disk_retry") {
        pack.set_int(lt::settings_pack::optimistic_disk_retry, val);
    } else if (key == "num_optimistic_unchoke_slots") {
        pack.set_int(lt::settings_pack::num_optimistic_unchoke_slots, val);
    } else if (key == "max_failcount") {
        pack.set_int(lt::settings_pack::max_failcount, val);
    } else if (key == "max_rejects") {
        pack.set_int(lt::settings_pack::max_rejects, val);
    } else if (key == "share_mode_target") {
        pack.set_int(lt::settings_pack::share_mode_target, val);
    } else if (key == "local_service_announce_interval") {
        pack.set_int(lt::settings_pack::local_service_announce_interval, val);
    } else if (key == "read_job_every") {
        // libtorrent 2.0: not available, silently ignored
    }
    // Unknown keys are silently ignored
}

static void apply_bool_setting(lt::settings_pack& pack, const std::string& key, bool val) {
    // Phase 1: core boolean settings
    if (key == "smooth_connects") {
        pack.set_bool(lt::settings_pack::smooth_connects, val);
    } else if (key == "allow_multiple_connections_per_ip") {
        pack.set_bool(lt::settings_pack::allow_multiple_connections_per_ip, val);
    } else if (key == "enable_dht") {
        pack.set_bool(lt::settings_pack::enable_dht, val);
    } else if (key == "enable_lsd") {
        pack.set_bool(lt::settings_pack::enable_lsd, val);
    } else if (key == "enable_upnp") {
        pack.set_bool(lt::settings_pack::enable_upnp, val);
    } else if (key == "enable_natpmp") {
        pack.set_bool(lt::settings_pack::enable_natpmp, val);
    } else if (key == "rate_limit_utp") {
        // libtorrent 2.0: rate_limit_utp removed, silently ignored
    } else if (key == "rate_limit_ip_overhead") {
        pack.set_bool(lt::settings_pack::rate_limit_ip_overhead, val);
    } else if (key == "use_disk_read_ahead") {
        // libtorrent 2.0: use_disk_read_ahead removed, silently ignored
    } else if (key == "lock_disk_cache") {
        // libtorrent 2.0: lock_disk_cache removed, silently ignored
    } else if (key == "no_atime_storage") {
        pack.set_bool(lt::settings_pack::no_atime_storage, val);
    } else if (key == "low_prio_disk") {
        // libtorrent 2.0: low_prio_disk removed, silently ignored
    } else if (key == "use_read_cache") {
        // libtorrent 2.0: use_read_cache removed, silently ignored
    } else if (key == "use_disk_cache_pool") {
        // libtorrent 2.0: use_disk_cache_pool removed, silently ignored
    } else if (key == "volatile_read_cache") {
        // libtorrent 2.0: volatile_read_cache deprecated, silently ignored
    } else if (key == "guided_read_cache") {
        // libtorrent 2.0: guided_read_cache removed, silently ignored
    } else if (key == "prioritize_partial_pieces") {
        pack.set_bool(lt::settings_pack::prioritize_partial_pieces, val);
    } else if (key == "drop_skipped_requests") {
        // libtorrent 2.0: not available, silently ignored
    } else if (key == "announce_to_all_trackers") {
        pack.set_bool(lt::settings_pack::announce_to_all_trackers, val);
    } else if (key == "announce_to_all_tiers") {
        pack.set_bool(lt::settings_pack::announce_to_all_tiers, val);
    } else if (key == "prefer_udp_trackers") {
        pack.set_bool(lt::settings_pack::prefer_udp_trackers, val);
    } else if (key == "auto_manage_prefer_seeds") {
        pack.set_bool(lt::settings_pack::auto_manage_prefer_seeds, val);
    } else if (key == "dont_count_slow_torrents") {
        pack.set_bool(lt::settings_pack::dont_count_slow_torrents, val);
    } else if (key == "proxy_hostnames") {
        pack.set_bool(lt::settings_pack::proxy_hostnames, val);
    } else if (key == "proxy_peer_connections") {
        pack.set_bool(lt::settings_pack::proxy_peer_connections, val);
    } else if (key == "proxy_tracker_connections") {
        pack.set_bool(lt::settings_pack::proxy_tracker_connections, val);
    } else if (key == "anonymous_mode") {
        pack.set_bool(lt::settings_pack::anonymous_mode, val);
    } else if (key == "force_proxy") {
        // libtorrent 2.0: force_proxy removed, silently ignored
    } else if (key == "always_send_user_agent") {
        pack.set_bool(lt::settings_pack::always_send_user_agent, val);
    } else if (key == "ignore_resume_timestamps") {
        // libtorrent 2.0: ignore_resume_timestamps removed, silently ignored
    } else if (key == "no_recheck_incomplete_resume") {
        pack.set_bool(lt::settings_pack::no_recheck_incomplete_resume, val);
    } else if (key == "disable_hash_checks") {
        pack.set_bool(lt::settings_pack::disable_hash_checks, val);
    } else if (key == "allow_i2p_mixed") {
        pack.set_bool(lt::settings_pack::allow_i2p_mixed, val);
    } else if (key == "incoming_starts_queued") {
        // libtorrent 2.0: not available, silently ignored
    } else if (key == "ban_web_seeds") {
        pack.set_bool(lt::settings_pack::ban_web_seeds, val);
    } else if (key == "report_web_seed_downloads") {
        pack.set_bool(lt::settings_pack::report_web_seed_downloads, val);
    } else if (key == "apply_ip_filter_to_trackers") {
        pack.set_bool(lt::settings_pack::apply_ip_filter_to_trackers, val);
    } else if (key == "announce_double_nat") {
        // libtorrent 2.0: announce_double_nat removed, silently ignored
    } else if (key == "lock_files") {
        // libtorrent 2.0: lock_files removed, silently ignored
    } else if (key == "strict_super_seeding") {
        // libtorrent 2.0: strict_super_seeding removed, silently ignored
    } else if (key == "enable_os_cache") {
        pack.set_bool(lt::settings_pack::enable_os_cache, val);
    }
    // Unknown keys are silently ignored
}

void lt_session_apply_settings(lt_session_t session, const char* settings_json) {
    if (!session || !settings_json || !*settings_json) return;

    auto wrapper = static_cast<lt_session_wrapper*>(session);
    lt::settings_pack pack;
    const char* p = settings_json;

    skip_json_ws(p);
    if (*p != '{') return;
    p++;

    while (*p) {
        skip_json_ws(p);
        if (*p == '}') { p++; break; }
        if (*p == ',') { p++; continue; }

        // Parse key
        if (*p != '"') break;
        std::string key = parse_json_string(p);

        skip_json_ws(p);
        if (*p != ':') break;
        p++;

        skip_json_ws(p);

        // Parse value
        if (*p == '"') {
            std::string val = parse_json_string(p);
            apply_str_setting(pack, key, val);
        } else if (*p == 't' || *p == 'f') {
            bool val = (*p == 't');
            while (*p && *p != ',' && *p != '}' && !std::isspace(static_cast<unsigned char>(*p))) p++;
            apply_bool_setting(pack, key, val);
        } else if (*p == '-' || std::isdigit(static_cast<unsigned char>(*p))) {
            int64_t val = parse_json_int(p);
            apply_int_setting(pack, key, static_cast<int>(val));
        } else {
            // Unknown value type, skip
            while (*p && *p != ',' && *p != '}') p++;
        }
    }

    std::lock_guard<std::mutex> lock(wrapper->mutex);
    wrapper->session->apply_settings(pack);
}

// Include session_stats_alert header
#include <libtorrent/session_stats.hpp>

int lt_session_get_stats(lt_session_t session, lt_session_stats_t* stats, int32_t* status) {
    if (!session || !stats) return -1;
    
    auto wrapper = static_cast<lt_session_wrapper*>(session);
    
    try {
        // Post session stats request
        wrapper->session->post_session_stats();
        
        // Wait for the session_stats_alert
        auto start = std::chrono::steady_clock::now();
        auto timeout = std::chrono::seconds(5);
        
        while (true) {
            auto now = std::chrono::steady_clock::now();
            if (now - start > timeout) {
                return -1;
            }
            
            std::vector<lt::alert*> alerts;
            {
                std::lock_guard<std::mutex> lock(wrapper->mutex);
                wrapper->session->pop_alerts(&alerts);
            }
            
            for (auto* alert : alerts) {
                if (auto* sa = lt::alert_cast<lt::session_stats_alert>(alert)) {
                    lt::span<std::int64_t const> counters = sa->counters();
                    
                    // Find metric indices by name
                    lt::span<lt::stats_metric const> metrics = lt::session_stats_metrics();
                    for (auto const& m : metrics) {
                        int idx = m.value_index;
                        if (idx < 0 || idx >= static_cast<int>(counters.size())) continue;
                        
                        std::string name(m.name);
                        if (name == "net.recv_rate") stats->download_rate = counters[idx];
                        else if (name == "net.sent_rate") stats->upload_rate = counters[idx];
                        else if (name == "net.recv_bytes") stats->total_downloaded = counters[idx];
                        else if (name == "net.sent_bytes") stats->total_uploaded = counters[idx];
                        else if (name == "dht.dht_nodes") stats->dht_nodes = static_cast<int32_t>(counters[idx]);
                        else if (name == "peer.num_peers_connected") stats->peers_connected = static_cast<int32_t>(counters[idx]);
                        else if (name == "peer.num_peers_half_open") stats->half_open_connections = static_cast<int32_t>(counters[idx]);
                    }
                    if (status) *status = 0;
                    return 0;
                }
            }
            
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    } catch (const std::exception&) {
        return -1;
    }
}
