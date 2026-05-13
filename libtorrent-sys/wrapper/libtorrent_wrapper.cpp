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
#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <chrono>

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

int lt_torrent_handle_status(lt_torrent_handle_t handle, int* state, float* progress, uint64_t* total_done, uint64_t* total) {
    if (!handle || !state || !progress || !total_done || !total) return -1;
    
    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) return -1;
    
    auto status = h->status();
    *state = static_cast<int>(status.state);
    *progress = status.progress;
    *total_done = static_cast<uint64_t>(status.total_done);
    *total = static_cast<uint64_t>(status.total);
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
                    if (static_cast<int>(rp->piece) == piece_index) {
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
    
    return status.pieces[piece_index] ? 1 : 0;
}
