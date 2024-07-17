/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "win_async_client_pool.h"

#include "../utils.h"
#include "sockets.h"

#include <algorithm>

// Using NULLs as specified by WinAPI
#ifdef __JETBRAINS_IDE__
# pragma ide diagnostic ignored "modernize-use-nullptr"
#endif

namespace ignite::network::detail {

win_async_client_pool::win_async_client_pool()
    : m_stopping(true)
    , m_async_handler()
    , m_connecting_thread()
    , m_worker_thread()
    , m_id_gen(0)
    , m_iocp(NULL)
    , m_clients_mutex()
    , m_client_id_map() {
}

win_async_client_pool::~win_async_client_pool() {
    internal_stop();
}

void win_async_client_pool::start(std::vector<tcp_range> addrs, uint32_t connLimit) {
    if (!m_stopping)
        throw ignite_error(error::code::CONNECTION, "Client pool is already started");

    m_stopping = false;

    init_wsa();

    m_iocp = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);
    if (!m_iocp)
        throw_last_system_error("Failed to create IOCP instance");

    try {
        m_connecting_thread.start(*this, connLimit, std::move(addrs));
        m_worker_thread.start(*this, m_iocp);
    } catch (...) {
        stop();

        throw;
    }
}

void win_async_client_pool::stop() {
    internal_stop();
}

void win_async_client_pool::internal_stop() {
    if (m_stopping)
        return;

    m_stopping = true;
    m_connecting_thread.stop();

    {
        std::lock_guard<std::mutex> lock(m_clients_mutex);

        for (auto it = m_client_id_map.begin(); it != m_client_id_map.end(); ++it) {
            win_async_client &client = *it->second;

            client.shutdown(std::nullopt);
            client.close();
        }
    }

    m_worker_thread.stop();

    CloseHandle(m_iocp);
    m_iocp = NULL;

    m_client_id_map.clear();
}

bool win_async_client_pool::add_client(const std::shared_ptr<win_async_client> &client) {
    uint64_t id;
    {
        std::lock_guard<std::mutex> lock(m_clients_mutex);

        if (m_stopping)
            return false;

        id = ++m_id_gen;
        client->set_id(id);

        HANDLE iocp0 = client->add_to_iocp(m_iocp);
        if (iocp0 == NULL)
            throw_last_system_error("Can not add socket to IOCP");

        m_iocp = iocp0;

        m_client_id_map[id] = client;
    }

    PostQueuedCompletionStatus(m_iocp, 0, reinterpret_cast<ULONG_PTR>(client.get()), NULL);

    return true;
}

void win_async_client_pool::handle_connection_error(const end_point &addr, const ignite_error &err) {
    auto asyncHandler0 = m_async_handler.lock();
    if (asyncHandler0)
        asyncHandler0->on_connection_error(addr, err);
}

void win_async_client_pool::handle_connection_success(const end_point &addr, uint64_t id) {
    auto asyncHandler0 = m_async_handler.lock();
    if (asyncHandler0)
        asyncHandler0->on_connection_success(addr, id);
}

void win_async_client_pool::handle_connection_closed(uint64_t id, std::optional<ignite_error> err) {
    auto asyncHandler0 = m_async_handler.lock();
    if (asyncHandler0)
        asyncHandler0->on_connection_closed(id, std::move(err));
}

void win_async_client_pool::handle_message_received(uint64_t id, bytes_view msg) {
    auto asyncHandler0 = m_async_handler.lock();
    if (asyncHandler0)
        asyncHandler0->on_message_received(id, msg);
}

void win_async_client_pool::handle_message_sent(uint64_t id) {
    auto asyncHandler0 = m_async_handler.lock();
    if (asyncHandler0)
        asyncHandler0->on_message_sent(id);
}

bool win_async_client_pool::send(uint64_t id, std::vector<std::byte> &&data) {
    if (m_stopping)
        throw ignite_error("Client is stopped");

    auto client = find_client(id);
    if (!client)
        return false;

    return client->send(std::move(data));
}

void win_async_client_pool::close_and_release(uint64_t id, std::optional<ignite_error> err) {
    std::shared_ptr<win_async_client> client;
    {
        std::lock_guard<std::mutex> lock(m_clients_mutex);

        auto it = m_client_id_map.find(id);
        if (it == m_client_id_map.end())
            return;

        client = it->second;

        m_client_id_map.erase(it);
    }

    bool closed = client->close();
    if (closed) {
        m_connecting_thread.notify_free_address(client->get_range());

        err = client->get_close_error();
        handle_connection_closed(id, err);
    }
}

void win_async_client_pool::close(uint64_t id, std::optional<ignite_error> err) {
    auto client = find_client(id);
    if (client && !client->is_closed())
        client->shutdown(std::move(err));
}

std::shared_ptr<win_async_client> win_async_client_pool::find_client(uint64_t id) const {
    std::lock_guard<std::mutex> lock(m_clients_mutex);

    return find_client_locked(id);
}

std::shared_ptr<win_async_client> win_async_client_pool::find_client_locked(uint64_t id) const {
    auto it = m_client_id_map.find(id);
    if (it == m_client_id_map.end())
        return {};

    return it->second;
}

void win_async_client_pool::set_handler(std::weak_ptr<async_handler> handler) {
    m_async_handler = handler;
}

} // namespace ignite::network::detail
