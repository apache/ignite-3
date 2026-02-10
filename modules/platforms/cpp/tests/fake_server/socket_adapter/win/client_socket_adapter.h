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

#pragma once
#include <winsock2.h>
#include <vector>
#include <cstddef>

namespace ignite {
class client_socket_adapter {
public:
    explicit client_socket_adapter(int m_fd)
        : m_fd(m_fd) {}

    ~client_socket_adapter() {
        if (is_valid()) {
            close();
        }
    }

    client_socket_adapter() = default;

    client_socket_adapter(const client_socket_adapter &other) = delete;

    client_socket_adapter(client_socket_adapter &&other) noexcept
        : m_fd(other.m_fd)
    {
        other.m_fd = INVALID_SOCKET;
    }

    client_socket_adapter &operator=(const client_socket_adapter &other) = delete;

    client_socket_adapter &operator=(client_socket_adapter &&other) noexcept {
        m_fd = other.m_fd;
        other.m_fd = INVALID_SOCKET;

        return *this;
    }

    [[nodiscard]] bool is_valid() const { return m_fd != INVALID_SOCKET; }

    void send_message(const std::vector<std::byte> &msg) const {
        ::send(m_fd, reinterpret_cast<const char *>(msg.data()), msg.size(), 0);
    }

    [[nodiscard]] int receive_next_packet(std::byte *buf, size_t buf_size) const {
        return ::recv(m_fd, reinterpret_cast<char *>(buf), buf_size, 0);
    }

    void close() {
        ::closesocket(m_fd);
        m_fd = INVALID_SOCKET;
    }

private:
    SOCKET m_fd = INVALID_SOCKET;
};
} // namespace ignite