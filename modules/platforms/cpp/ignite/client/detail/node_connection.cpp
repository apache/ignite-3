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

#include "ignite/client/detail/node_connection.h"

#include <ignite/protocol/messages.h>
#include <ignite/protocol/utils.h>

namespace ignite::detail {

node_connection::node_connection(uint64_t id, std::shared_ptr<network::async_client_pool> pool,
    std::weak_ptr<connection_event_handler> event_handler, std::shared_ptr<ignite_logger> logger,
    const ignite_client_configuration &cfg)
    : m_id(id)
    , m_pool(std::move(pool))
    , m_event_handler(std::move(event_handler))
    , m_logger(std::move(logger))
    , m_configuration(cfg) {
}

node_connection::~node_connection() {
    for (auto &handler : m_request_handlers) {
        auto handling_res = result_of_operation<void>([&]() {
            auto res = handler.second->set_error(ignite_error("Connection closed before response was received"));
            if (res.has_error())
                m_logger->log_error(
                    "Uncaught user callback exception while handling operation error: " + res.error().what_str());
        });
        if (handling_res.has_error())
            m_logger->log_error("Uncaught user callback exception: " + handling_res.error().what_str());
    }
}

bool node_connection::handshake() {
    static constexpr int8_t CLIENT_TYPE = 2;

    std::map<std::string, std::string> extensions;
    auto authenticator = m_configuration.get_authenticator();
    if (authenticator) {
        extensions.emplace("authn-type", authenticator->get_type());
        extensions.emplace("authn-identity", authenticator->get_identity());
        extensions.emplace("authn-secret", authenticator->get_secret());
    }

    std::vector<std::byte> message = protocol::make_handshake_request(CLIENT_TYPE, m_protocol_context.get_version());
    return m_pool->send(m_id, std::move(message));
}

void node_connection::process_message(bytes_view msg) {
    protocol::reader reader(msg);
    auto response_type = reader.read_int32();
    if (protocol::message_type(response_type) != protocol::message_type::RESPONSE) {
        m_logger->log_warning("Unsupported message type: " + std::to_string(response_type));
        return;
    }

    auto req_id = reader.read_int64();
    auto flags = reader.read_int32();
    UNUSED_VALUE flags; // Flags are unused for now.

    auto observable_timestamp = reader.read_int64();
    auto event_handler = m_event_handler.lock();
    if (event_handler) {
        event_handler->on_observable_timestamp_changed(observable_timestamp);
    }

    auto handler = get_and_remove_handler(req_id);
    if (!handler) {
        m_logger->log_error("Missing handler for request with id=" + std::to_string(req_id));
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

    auto pos = reader.position();
    bytes_view data{msg.data() + pos, msg.size() - pos};
    auto handling_res = handler->handle(shared_from_this(), data);
    if (handling_res.has_error())
        m_logger->log_error("Uncaught user callback exception: " + handling_res.error().what_str());
}

ignite_result<void> node_connection::process_handshake_rsp(bytes_view msg) {
    m_logger->log_debug("Got handshake response");

    auto response = protocol::parse_handshake_response(msg);
    auto const &ver = response.context.get_version();

    m_logger->log_debug("Server-side protocol version: " + ver.to_string());

    // We now only support a single version
    if (ver != protocol::protocol_version::get_current())
        return {ignite_error("Unsupported server version: " + ver.to_string())};

    if (response.error) {
        m_logger->log_warning("Handshake error: " + response.error->what_str());
        return {ignite_error(*response.error)};
    }

    m_protocol_context = response.context;
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
