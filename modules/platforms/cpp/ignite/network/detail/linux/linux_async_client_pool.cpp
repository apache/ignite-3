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

#include "linux_async_client_pool.h"

#include "../utils.h"
#include "sockets.h"

#include <algorithm>

namespace ignite::network::detail {

linux_async_client_pool::linux_async_client_pool()
    : m_stopping(true)
    , m_async_handler()
    , m_worker_thread(*this)
    , m_id_gen(0)
    , m_clients_mutex()
    , m_client_id_map() {
}

linux_async_client_pool::~linux_async_client_pool() {
    internal_stop();
}

void linux_async_client_pool::start(const std::vector<tcp_range> addrs, uint32_t conn_limit) {
    if (!m_stopping)
        throw ignite_error("Client pool is already started");

    m_id_gen = 0;
    m_stopping = false;

    try {
        m_worker_thread.start(conn_limit, addrs);
    } catch (...) {
        stop();

        throw;
    }
}

void linux_async_client_pool::stop() {
    internal_stop();
}

bool linux_async_client_pool::send(uint64_t id, std::vector<std::byte> &&data) {
    if (m_stopping)
        throw ignite_error("Client is stopped");

    auto client = find_client(id);
    if (!client)
        return false;

    return client->send(std::move(data));
}

void linux_async_client_pool::close(uint64_t id, std::optional<ignite_error> err) {
    if (m_stopping)
        return;

    std::shared_ptr<linux_async_client> client = find_client(id);
    if (client && !client->is_closed())
        client->shutdown(std::move(err));
}

void linux_async_client_pool::close_and_release(uint64_t id, std::optional<ignite_error> err) {
    if (m_stopping)
        return;

    std::shared_ptr<linux_async_client> client;
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
        err = client->get_close_error();
        handle_connection_closed(id, err);
    }
}

bool linux_async_client_pool::add_client(const std::shared_ptr<linux_async_client>& client) {
    if (m_stopping)
        return false;

    auto client_addr = client->address();
    uint64_t client_id;
    {
        std::lock_guard<std::mutex> lock(m_clients_mutex);

        client_id = ++m_id_gen;
        client->set_id(client_id);

        m_client_id_map[client_id] = client;
    }

    handle_connection_success(client_addr, client_id);

    return true;
}

void linux_async_client_pool::handle_connection_error(const end_point &addr, ignite_error err) {
    if (auto handler = m_async_handler.lock())
        handler->on_connection_error(addr, std::move(err));
}

void linux_async_client_pool::handle_connection_success(const end_point &addr, uint64_t id) {
    if (auto handler = m_async_handler.lock())
        handler->on_connection_success(addr, id);
}

void linux_async_client_pool::handle_connection_closed(uint64_t id, std::optional<ignite_error> err) {
    if (auto handler = m_async_handler.lock())
        handler->on_connection_closed(id, std::move(err));
}

void linux_async_client_pool::handle_message_received(uint64_t id, bytes_view msg) {
    if (auto handler = m_async_handler.lock())
        handler->on_message_received(id, msg);
}

void linux_async_client_pool::handle_message_sent(uint64_t id) {
    if (auto handler = m_async_handler.lock())
        handler->on_message_sent(id);
}

void linux_async_client_pool::internal_stop() {
    m_stopping = true;
    m_worker_thread.stop();

    {
        std::lock_guard<std::mutex> lock(m_clients_mutex);

        for (auto [_, client] : m_client_id_map) {
            ignite_error err("Client stopped");
            handle_connection_closed(client->id(), err);
        }

        m_client_id_map.clear();
    }
}

std::shared_ptr<linux_async_client> linux_async_client_pool::find_client(uint64_t id) const {
    std::lock_guard<std::mutex> lock(m_clients_mutex);

    auto it = m_client_id_map.find(id);
    if (it == m_client_id_map.end())
        return {};

    return it->second;
}

} // namespace ignite::network::detail
