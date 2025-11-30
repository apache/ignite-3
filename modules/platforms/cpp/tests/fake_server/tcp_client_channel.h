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

#include <atomic>
#include <cstddef>
#include <ignite/common/ignite_error.h>
#include <ignite/protocol/utils.h>

namespace ignite {

/**
 * Owning wrapper around server-side client socket.
 */
class tcp_client_channel {
    int m_srv_fd;
    int m_cl_fd = -1;
    std::byte m_buf[1024];
    size_t m_pos = 0;
    size_t m_remaining = 0;
    std::atomic_bool m_stopped{false};

public:
    explicit tcp_client_channel(int srv_socket_fd)
        : m_srv_fd(srv_socket_fd) {}

    void start();
    void stop();
    std::vector<std::byte> read_next_n_bytes(size_t n);
    void send_message(std::vector<std::byte> msg);

private:
    void receive_next_packet();

};
} // namespace ignite
