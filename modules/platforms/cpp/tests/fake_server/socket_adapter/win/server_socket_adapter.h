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
#include <ws2tcpip.h>

namespace ignite {
class server_socket_adapter {
public:
    explicit server_socket_adapter(SOCKET m_fd)
        : m_fd(m_fd) {}

    server_socket_adapter() = default;

    server_socket_adapter(const server_socket_adapter &other) = default;

    server_socket_adapter &operator=(const server_socket_adapter &other) = default;

    void start() {
        m_fd = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    }

    bool is_valid() const {
        return m_fd != INVALID_SOCKET;
    }

    SOCKET accept() {
        sockaddr_in cl_addr{};

        socklen_t addr_len = sizeof(cl_addr);

        SOCKET cl_sock = ::accept(m_fd, reinterpret_cast<sockaddr *>(&cl_addr), &addr_len);

        return cl_sock;
    }

    int bind(int port) const {
        sockaddr_in srv_addr{};

        srv_addr.sin_family = AF_INET;
        srv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
        srv_addr.sin_port = htons(port);

        return ::bind(m_fd, reinterpret_cast<sockaddr*>(&srv_addr), sizeof(srv_addr));
    }

    int listen() const {
        return ::listen(m_fd, 1);
    }

    void close() {
        ::closesocket(m_fd);
        m_fd = INVALID_SOCKET;
    }
private:
    SOCKET m_fd = INVALID_SOCKET;
};
}