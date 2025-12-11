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

#include "fake_server.h"
#include "ignite/protocol/protocol_version.h"

#include <cstring>
#include <ignite/common/ignite_error.h>
#include <ignite/protocol/utils.h>
#include <iostream>
#include <queue>

void fake_server::start() {
    start_socket();

    bind_address_port();

    start_socket_listen();

    m_io_thread = std::make_unique<std::thread>([this] {
        accept_client_connection();

        m_started.store(true);

        handle_client_handshake();

        send_server_handshake();

        handle_requests();
    });

    m_logger->log_debug("fake server started");
}

void fake_server::start_socket() {
    m_srv_fd = socket(AF_INET, SOCK_STREAM, 6);

    if (m_srv_fd < 0) {
        throw ignite_error("socket failed");
    }
}

void fake_server::bind_address_port() const {
    sockaddr_in srv_addr{};

    srv_addr.sin_family = AF_INET;
    srv_addr.sin_addr.s_addr = INADDR_ANY;
    srv_addr.sin_port = htons(m_srv_port);

    int bind_res = bind(m_srv_fd, reinterpret_cast<sockaddr *>(&srv_addr), sizeof(srv_addr));

    if (bind_res < 0) {
        std::stringstream ss;
        ss << "bind failed" << strerror(errno);
        throw std::runtime_error(ss.str());
    }
}

void fake_server::start_socket_listen() const {
    int listen_res = listen(m_srv_fd, 1);

    if (listen_res < 0) {
        throw std::runtime_error("listen failed");
    }

    m_logger->log_debug("fake server is listening on port=" + std::to_string(m_srv_port));
}

void fake_server::accept_client_connection() {
    m_client_channel = std::make_unique<tcp_client_channel>(m_srv_fd, m_logger);
    m_client_channel->start();
}

void fake_server::handle_client_handshake() const {
    using protocol::MAGIC_BYTES;

    auto magic = m_client_channel->read_next_n_bytes(4);

    if (!std::equal(magic.begin(), magic.end(), MAGIC_BYTES.begin(), MAGIC_BYTES.end())) {
        throw ignite_error("fake server handshake failed: incorrect magic bytes");
    }

    auto size_header = m_client_channel->read_next_n_bytes(4);

    int32_t msg_size = detail::bytes::load<detail::endian::BIG, int32_t>(size_header.data());

    m_logger->log_debug("Handshake message size = " + std::to_string(msg_size));

    auto msg = m_client_channel->read_next_n_bytes(msg_size);

    bytes_view bv(msg);

    protocol::reader reader(bv);

    int16_t ver_major = reader.read_int16();
    int16_t ver_minor = reader.read_int16();
    int16_t ver_patch = reader.read_int16();
    int16_t client_type = reader.read_int16();

    if (m_logger->is_debug_enabled()) {
        std::stringstream ss;
        ss << "Client version = " << ver_major << '.' << ver_minor << '.' << ver_patch << '\n';
        ss << "Client type = " << client_type;

        m_logger->log_debug(ss.str());
    }

    auto features_bytes = reader.read_binary();
    std::vector<std::byte> features{features_bytes.begin(), features_bytes.end()};

    // we ignore the rest for now.
}

void fake_server::send_server_handshake() const {
    std::vector<std::byte> msg;
    protocol::buffer_adapter buf(msg);
    protocol::writer writer(buf);

    buf.write_raw(protocol::MAGIC_BYTES);
    buf.reserve_length_header();

    auto ver = protocol::protocol_version::get_current();
    writer.write(ver.get_major());
    writer.write(ver.get_minor());
    writer.write(ver.get_patch());

    writer.write_nil(); // No error.

    writer.write(static_cast<int64_t>(0)); // idle_timeout_ms

    writer.write(uuid::random());
    writer.write("fake_server_node");

    writer.write(static_cast<int32_t>(1));
    writer.write(uuid::random());
    writer.write("fake_cluster");

    writer.write(static_cast<int64_t>(424242)); // Observable timestamp: ignore correct value for now.

    // dbms version
    writer.write(static_cast<int8_t>(42));
    writer.write(static_cast<int8_t>(42));
    writer.write(static_cast<int8_t>(42));
    writer.write(static_cast<int8_t>(42));
    writer.write("dbms_version_pre_release");

    bytes_view features(protocol::all_supported_bitmask_features());
    writer.write_binary(features);

    writer.write_map({}); // extensions

    buf.write_length_header();

    m_client_channel->send_message(msg);

    m_logger->log_debug("Server handshake message sent");
}

void fake_server::write_response(
    std::vector<std::byte> &resp, int64_t req_id, std::function<void(protocol::writer &wr)> body) {
    protocol::buffer_adapter buf(resp);
    protocol::writer wr(buf);

    buf.reserve_length_header();

    wr.write(req_id);
    wr.write(static_cast<int32_t>(0)); // flags
    wr.write(static_cast<int64_t>(424242)); // observable timestamp

    body(wr);

    buf.write_length_header();
}

void fake_server::handle_requests() {
    using protocol::client_operation;

    struct delayed_response {
        std::chrono::time_point<std::chrono::steady_clock> time_point;
        std::vector<std::byte> response;
    };

    auto cmp = [](delayed_response &lhs, delayed_response &rhs) { return lhs.time_point < rhs.time_point; };

    std::priority_queue<delayed_response, std::vector<delayed_response>, decltype(cmp)> delayed_responses(cmp);

    while (!m_stopped) {
        auto size_header = m_client_channel->read_next_n_bytes(4);

        if (size_header.empty()) {
            break;
        }

        int32_t msg_size = detail::bytes::load<detail::endian::BIG, int32_t>(size_header.data());

        auto msg = m_client_channel->read_next_n_bytes(msg_size);

        if (msg.empty()) {
            break; // connection was closed;
        }

        bytes_view view(msg);

        protocol::reader rd(view);

        int8_t op_code = rd.read_int8();
        client_operation op = static_cast<client_operation>(op_code);

        auto req_id = rd.read_int64();

        if (m_logger->is_debug_enabled()) {
            std::stringstream ss;
            ss << "Received message of size " << msg_size << " Operation type = " << static_cast<int>(op_code)
                      << " req_id = " << req_id;

            m_logger->log_debug(ss.str());
        }

        std::vector<std::byte> resp;
        switch (op) {
            case client_operation::HEARTBEAT: {
                auto body = [](protocol::writer&) {};

                write_response(resp, req_id, body);
            } break;

            case client_operation::CLUSTER_GET_NODES: {
                auto body = [](protocol::writer &wr) {
                    wr.write(static_cast<int32_t>(1));
                    wr.write(4); // field count
                    wr.write(uuid::random());
                    wr.write("fake_node");
                    wr.write("127.0.0.1");
                    wr.write(static_cast<int16_t>(10800));
                };

                write_response(resp, req_id, body);
            } break;
            default:
                std::stringstream ss;
                ss << "Unsupported fake server operation:" << static_cast<int>(op);
                throw ignite_error(ss.str());
        }

        auto request_action = m_op_type_handler ? m_op_type_handler(op) : nullptr;

        if (request_action) {
            if (request_action->type() == DROP) {
                // ignore that response
            }

            if (request_action->type() == DELAY) {

                if (auto delay_action = dynamic_cast<delayed_action *>(request_action.get())) {
                    auto time_point = std::chrono::steady_clock::now() + delay_action->delay();

                    delayed_responses.push({time_point, resp});
                }
            }
        } else {
            m_client_channel->send_message(resp);
        }

        auto now = std::chrono::steady_clock::now();
        while (!delayed_responses.empty() && delayed_responses.top().time_point < now) {
            m_client_channel->send_message(delayed_responses.top().response);

            delayed_responses.pop();
        }
    }
}