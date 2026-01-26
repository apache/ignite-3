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
#include <sys/socket.h>
#include <unistd.h>

namespace ignite {
class client_socket_adapter {
public:
    explicit client_socket_adapter(int m_fd)
        : m_fd(m_fd) {}

    client_socket_adapter() = default;

    client_socket_adapter(const client_socket_adapter &other) = default;

    client_socket_adapter &operator=(const client_socket_adapter &other) = default;

    bool is_valid() const { return m_fd >= 0; }

    void send_message(const std::vector<std::byte> &msg) { ::send(m_fd, msg.data(), msg.size(), 0); }

    int recieve_next_packet(std::byte *buf, size_t buf_size) { return ::recv(m_fd, buf, buf_size, 0); }

    void close() {
        ::close(m_fd);
        m_fd = -1;
    }

private:
    int m_fd = -1;
};
} // namespace ignite