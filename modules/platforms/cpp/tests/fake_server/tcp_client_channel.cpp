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

#include "tcp_client_channel.h"

#include <algorithm>

namespace ignite {
std::vector<std::byte> tcp_client_channel::read_next_n_bytes(size_t n) {
    std::vector<std::byte> res;
    res.reserve(n);

    while (res.size() < n && !m_stopped) {
        if (m_remaining == 0) {
            receive_next_packet();
        }

        size_t bytes_to_consume = std::min(m_remaining, n);

        std::copy_n(m_buf + m_pos, bytes_to_consume, std::back_inserter(res));

        m_pos += bytes_to_consume;
        m_remaining -= bytes_to_consume;
    }

    if (m_stopped) {
        return {};
    }

    return res;
}

void tcp_client_channel::send_message(const std::vector<std::byte>& msg) {
    m_cl_sock.send_message(msg);
}

void tcp_client_channel::receive_next_packet() {
    int received = m_cl_sock.receive_next_packet(m_buf, sizeof(m_buf));

    if (received == 0) {
        m_logger->log_debug( "connection was closed");

        m_stopped.store(true);
    }

    if (received < 0 && !m_stopped) {
        std::stringstream ss;

        ss << "connection was closed with error: " << LAST_SOCKET_ERROR();
        throw ignite_error(ss.str());
    }

    m_remaining = received;
    m_pos = 0;
}

void tcp_client_channel::start() {
    m_logger->log_debug("waiting for client to connect");

    auto cl_fd = m_srv_sock.accept();
    m_cl_sock = client_socket_adapter(cl_fd);

    if (!m_cl_sock.is_valid() && !m_stopped) {
        std::stringstream ss;
        ss << "connection acceptance failed " << LAST_SOCKET_ERROR();
        throw std::runtime_error(ss.str());
    }

    m_logger->log_debug("Client connected");
}

void tcp_client_channel::stop() {
    m_stopped.store(true);

    if (m_cl_sock.is_valid()) {
        m_cl_sock.close();
    }
}
}; // namespace ignite