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

#include "node_connection.h"

#include <ignite/protocol/utils.h>

namespace ignite::detail {

node_connection::node_connection(
    uint64_t id, std::shared_ptr<network::async_client_pool> pool, std::shared_ptr<ignite_logger> logger)
    : m_id(id)
    , m_pool(std::move(pool))
    , m_logger(std::move(logger)) {
}

node_connection::~node_connection() {
    for (auto &handler : m_request_handlers) {
        auto handlingRes = result_of_operation<void>([&]() {
            auto res = handler.second->set_error(ignite_error("Connection closed before response was received"));
            if (res.has_error())
                m_logger->log_error(
                    "Uncaught user callback exception while handling operation error: " + res.error().what_str());
        });
        if (handlingRes.has_error())
            m_logger->log_error("Uncaught user callback exception: " + handlingRes.error().what_str());
    }
}

bool node_connection::handshake() {
    static constexpr int8_t CLIENT_TYPE = 2;

    std::vector<std::byte> message;
    {
        protocol::buffer_adapter buffer(message);
        buffer.write_raw(bytes_view(protocol::MAGIC_BYTES.data(), protocol::MAGIC_BYTES.size()));

        protocol::write_message_to_buffer(buffer, [&context = m_protocol_context](protocol::writer &writer) {
            auto ver = context.get_version();

            writer.write(ver.major());
            writer.write(ver.minor());
            writer.write(ver.patch());

            writer.write(CLIENT_TYPE);

            // Features.
            writer.write_binary_empty();

            // Extensions.
            writer.write_map_empty();
        });
    }

    return m_pool->send(m_id, std::move(message));
}

void node_connection::process_message(bytes_view msg) {
    protocol::reader reader(msg);
    auto responseType = reader.read_int32();
    if (message_type(responseType) != message_type::RESPONSE) {
        m_logger->log_warning("Unsupported message type: " + std::to_string(responseType));
        return;
    }

    auto reqId = reader.read_int64();
    auto flags = reader.read_int32();
    (void) flags; // Flags are unused for now.

    auto handler = get_and_remove_handler(reqId);

    if (!handler) {
        m_logger->log_error("Missing handler for request with id=" + std::to_string(reqId));
        return;
    }

    auto err = protocol::read_error(reader);
    if (err) {
        m_logger->log_error("Error: " + err->what_str());
        auto res = handler->set_error(std::move(err.value()));
        if (res.has_error())
            m_logger->log_error(
                "Uncaught user callback exception while handling operation error: " + res.error().what_str());
        return;
    }

    auto handlingRes = handler->handle(reader);
    if (handlingRes.has_error())
        m_logger->log_error("Uncaught user callback exception: " + handlingRes.error().what_str());
}

ignite_result<void> node_connection::process_handshake_rsp(bytes_view msg) {
    m_logger->log_debug("Got handshake response");

    protocol::reader reader(msg);

    auto verMajor = reader.read_int16();
    auto verMinor = reader.read_int16();
    auto verPatch = reader.read_int16();

    protocol_version ver(verMajor, verMinor, verPatch);
    m_logger->log_debug("Server-side protocol version: " + ver.to_string());

    // We now only support a single version
    if (ver != protocol_context::CURRENT_VERSION)
        return {ignite_error("Unsupported server version: " + ver.to_string())};

    auto err = protocol::read_error(reader);
    if (err)
        return {ignite_error(err.value())};

    (void) reader.read_int64(); // TODO: IGNITE-17606 Implement heartbeats
    (void) reader.read_string_nullable(); // Cluster node ID. Needed for partition-aware compute.
    (void) reader.read_string_nullable(); // Cluster node name. Needed for partition-aware compute.

    auto cluster_id = reader.read_uuid();
    reader.skip(); // Features.
    reader.skip(); // Extensions.

    m_protocol_context.set_version(ver);
    m_protocol_context.set_cluster_id(cluster_id);

    m_handshake_complete = true;

    return {};
}

std::shared_ptr<response_handler> node_connection::get_and_remove_handler(int64_t req_id) {
    std::lock_guard<std::mutex> lock(m_request_handlers_mutex);

    auto it = m_request_handlers.find(req_id);
    if (it == m_request_handlers.end())
        return {};

    auto res = std::move(it->second);
    m_request_handlers.erase(it);

    return res;
}

} // namespace ignite::detail
