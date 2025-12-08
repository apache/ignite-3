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

#include "ignite/client/ignite_logger.h"
#include "ignite/protocol/bitmask_feature.h"
#include "ignite/protocol/client_operation.h"
#include "ignite/protocol/reader.h"
#include "ignite/protocol/writer.h"
#include "response_action.h"
#include "tcp_client_channel.h"

#include <atomic>
#include <thread>
#include <unistd.h>

using namespace ignite;

using raw_msg = std::vector<std::byte>;

class fake_server {
public:
    explicit fake_server(
        int srv_port,
        std::shared_ptr<ignite_logger> logger,
        std::function<std::unique_ptr<response_action>(protocol::client_operation)> op_type_handler = nullptr
        )
        : m_srv_port(srv_port)
        , m_logger(std::move(logger))
        , m_op_type_handler(op_type_handler)
    {}

    ~fake_server() {
        m_stopped.store(true);

        if (m_started)
            m_client_channel->stop();

        if (m_srv_fd > 0) {
            ::close(m_srv_fd);
        }

        m_io_thread->join();
    }

    /** Starts fake server. */
    void start();

    /** Returns configured port number. */
    int get_server_port() const { return m_srv_port; }

private:
    void start_socket();

    void bind_address_port() const;

    void start_socket_listen() const;

    void accept_client_connection();

    void handle_client_handshake() const;

    void send_server_handshake() const;

    void write_response(std::vector<std::byte>& resp, std::int64_t req_id, std::function<void(protocol::writer &wr)> body);

    void handle_requests();

    /** Server socket FD. */
    int m_srv_fd = -1;
    /** Flag is up when server initialization was complete. */
    std::atomic_bool m_started{false};

    /** Flag is up when server destruction initiated. */
    std::atomic_bool m_stopped{false};

    /** Port number server listens to. Default is 10800. */
    const int m_srv_port;

    /** Pointer to the thread which handles client requests. */
    std::unique_ptr<std::thread> m_io_thread{};

    /** Objects which owns client socket FD and handles reads and writes to it. */
    std::unique_ptr<tcp_client_channel> m_client_channel{};

    /** Logger. */
    std::shared_ptr<ignite_logger> m_logger;

    /**
     * Allows developer to define custom action on requests according to their type.
     */
    std::function<std::unique_ptr<response_action>(protocol::client_operation)> m_op_type_handler;
};
