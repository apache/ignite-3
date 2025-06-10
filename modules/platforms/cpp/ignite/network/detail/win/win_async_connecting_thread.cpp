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

#include "win_async_connecting_thread.h"

#include "../utils.h"
#include "sockets.h"
#include "win_async_client_pool.h"

#include <cassert>
#include <random>
#include <memory>

// Using NULLs as specified by WinAPI
#ifdef __JETBRAINS_IDE__
# pragma ide diagnostic ignored "modernize-use-nullptr"
#endif

namespace ignite::network::detail {

namespace {

fibonacci_sequence<10> fibonacci10;

} // namespace

win_async_connecting_thread::win_async_connecting_thread()
    : m_client_pool(nullptr)
    , m_stopping(false)
    , m_failed_attempts(0)
    , m_min_addrs(0)
    , m_addr_position_seed(std::random_device()()) {
}

void win_async_connecting_thread::run() {
    assert(m_client_pool != nullptr);

    while (!m_stopping) {
        tcp_range range = get_next_address();

        if (m_stopping || range.empty())
            break;

        std::shared_ptr<win_async_client> client = try_connect(range);

        if (!client) {
            ++m_failed_attempts;

            auto ms_to_wait = static_cast<DWORD>(1000 * fibonacci10.get_value(m_failed_attempts));
            if (ms_to_wait)
                Sleep(ms_to_wait);

            continue;
        }

        m_failed_attempts = 0;

        if (m_stopping) {
            client->close();

            return;
        }

        try {
            bool added = m_client_pool->add_client(client);

            if (!added) {
                client->close();

                continue;
            }

            {
                std::lock_guard<std::mutex> lock(m_addrs_mutex);

                auto it = std::find(m_non_connected.begin(), m_non_connected.end(), range);
                if (it != m_non_connected.end())
                    m_non_connected.erase(it);
            }
        } catch (const ignite_error &err) {
            client->close();

            if (m_client_pool)
                m_client_pool->handle_connection_error(client->address(), err);

            continue;
        }
    }
}

void win_async_connecting_thread::notify_free_address(const tcp_range &range) {
    std::lock_guard<std::mutex> lock(m_addrs_mutex);

    m_non_connected.push_back(range);
    m_connect_needed.notify_one();
}

void win_async_connecting_thread::start(win_async_client_pool &clientPool, size_t limit, std::vector<tcp_range> addrs) {
    m_stopping = false;
    m_client_pool = &clientPool;
    m_failed_attempts = 0;
    m_non_connected = std::move(addrs);

    if (!limit || limit > m_non_connected.size())
        m_min_addrs = 0;
    else
        m_min_addrs = m_non_connected.size() - limit;

    m_thread = std::thread(&win_async_connecting_thread::run, this);
}

void win_async_connecting_thread::stop() {
    if (m_stopping)
        return;

    m_stopping = true;

    {
        std::lock_guard<std::mutex> lock(m_addrs_mutex);
        m_connect_needed.notify_one();
    }

    m_thread.join();
    m_non_connected.clear();
}

std::shared_ptr<win_async_client> win_async_connecting_thread::try_connect(const tcp_range &range) {
    for (uint16_t port = range.port; port <= (range.port + range.range); ++port) {
        end_point addr(range.host, port);
        try {
            SOCKET socket = try_connect(addr);

            return std::make_shared<win_async_client>(socket, addr, range, int32_t(BUFFER_SIZE));
        } catch (const ignite_error &err) {
            m_client_pool->handle_connection_error(addr, err);
        }
    }

    return {};
}

SOCKET win_async_connecting_thread::try_connect(const end_point &addr) {
    addrinfo hints{};
    memset(&hints, 0, sizeof(hints));

    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    std::stringstream converter;
    converter << addr.port;
    std::string str_port = converter.str();

    // Resolve the server address and port
    addrinfo *result = NULL;
    int res = getaddrinfo(addr.host.c_str(), str_port.c_str(), &hints, &result);

    if (res != 0)
        throw ignite_error(error::code::CONNECTION, "Can not resolve host: " + addr.host + ":" + str_port);

    std::string last_error_msg = "Failed to resolve host";

    SOCKET socket = INVALID_SOCKET; // NOLINT(modernize-use-auto)

    // Attempt to connect to an address until one succeeds
    for (addrinfo *it = result; it != NULL; it = it->ai_next) {
        last_error_msg = "Failed to establish connection with the host";

        socket = WSASocket(it->ai_family, it->ai_socktype, it->ai_protocol, NULL, 0, WSA_FLAG_OVERLAPPED);

        if (socket == INVALID_SOCKET)
            throw ignite_error(error::code::CONNECTION, "Socket creation failed: " + get_last_socket_error_message());

        try_set_socket_options(socket, BUFFER_SIZE, TRUE, TRUE, TRUE);

        // Connect to server.
        res = WSAConnect(socket, it->ai_addr, static_cast<int>(it->ai_addrlen), NULL, NULL, NULL, NULL);
        if (SOCKET_ERROR == res) {
            closesocket(socket);
            socket = INVALID_SOCKET;

            int last_error = WSAGetLastError();

            if (last_error != WSAEWOULDBLOCK) {
                last_error_msg.append(": ").append(get_socket_error_message(last_error));

                continue;
            }
        }

        break;
    }

    freeaddrinfo(result);

    if (socket == INVALID_SOCKET)
        throw ignite_error(error::code::CONNECTION, std::move(last_error_msg));

    return socket;
}

tcp_range win_async_connecting_thread::get_next_address() const {
    std::unique_lock<std::mutex> lock(m_addrs_mutex);

    if (m_stopping)
        return {};

    while (m_non_connected.size() <= m_min_addrs) {
        m_connect_needed.wait(lock);

        if (m_stopping)
            return {};
    }

    size_t idx = m_addr_position_seed % m_non_connected.size();
    tcp_range range = m_non_connected.at(idx);
    --m_addr_position_seed;

    return range;
}

} // namespace ignite::network::detail
