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

node_connection::node_connection(std::uint64_t id, std::shared_ptr<network::async_client_pool> &&pool,
    std::weak_ptr<connection_event_handler> &&event_handler, std::shared_ptr<ignite_logger> &&logger,
    const ignite_client_configuration &cfg, std::weak_ptr<thread_timer> &&timer_thread)
    : m_id(id)
    , m_pool(std::move(pool))
    , m_event_handler(std::move(event_handler))
    , m_logger(std::move(logger))
    , m_configuration(cfg)
    , m_timer_thread(std::move(timer_thread)){}

node_connection::~node_connection() {
    for (auto& req : m_request_handlers) {
        auto handling_res = result_of_operation<void>([req,this]() {
            auto handler = req.second.handler;
            auto res = handler->set_error(ignite_error("Connection closed before response was received"));
            if (res.has_error())
                this->m_logger->log_error(
                    "Uncaught user callback exception while handling operation error: " + res.error().what_str());
        });
        if (handling_res.has_error())
            this->m_logger->log_error("Uncaught user callback exception: " + handling_res.error().what_str());
    }
}

bool node_connection::handshake() {
    static constexpr std::int8_t CLIENT_TYPE = 2;

    std::map<std::string, std::string> extensions;
    if (const auto authenticator = m_configuration.get_authenticator()) {
        extensions.emplace("authn-type", authenticator->get_type());
        extensions.emplace("authn-identity", authenticator->get_identity());
        extensions.emplace("authn-secret", authenticator->get_secret());
    }

    std::vector<std::byte> message =
        protocol::make_handshake_request(CLIENT_TYPE, m_protocol_context.get_version(), extensions);
    return m_pool->send(m_id, std::move(message));
}

void node_connection::process_message(bytes_view msg) {
    m_last_message_ts = std::chrono::steady_clock::now();

    protocol::reader reader(msg);

    auto req_id = reader.read_int64();
    auto flags = reader.read_int32();
    if (test_flag(flags, protocol::response_flag::PARTITION_ASSIGNMENT_CHANGED)) {
        auto assignment_ts = reader.read_int64();
        UNUSED_VALUE assignment_ts;
    }

    auto observable_timestamp = reader.read_int64();
    on_observable_timestamp_changed(observable_timestamp);

    std::optional<ignite_error> err{};
    if (test_flag(flags, protocol::response_flag::ERROR_FLAG)) {
        err = {protocol::read_error(reader)};
        m_logger->log_error("Error: " + err->what_str());
    }

    auto pos = reader.position();
    bytes_view data{msg.data() + pos, msg.size() - pos};

    { // Locking scope
        std::lock_guard<std::recursive_mutex> lock(m_request_handlers_mutex);

        auto handler = find_handler_unsafe(req_id);
        if (!handler) {
            m_logger->log_error("Missing handler for request with id=" + std::to_string(req_id));
            return;
        }

        ignite_result<void> result{};
        if (err) {
            result = handler->set_error(std::move(*err));
        } else {
            result = handler->handle(shared_from_this(), data, flags);
        }

        if (result.has_error()) {
            m_logger->log_error("Uncaught user callback exception: " + result.error().what_str());
        }

        if (handler->is_handling_complete()) {
            m_request_handlers.erase(req_id);
        }
    }
}

void node_connection::on_observable_timestamp_changed(int64_t observable_timestamp) const {
    if (auto event_handler = m_event_handler.lock()) {
        event_handler->on_observable_timestamp_changed(observable_timestamp);
    }
}

void node_connection::send_heartbeat() {
    auto res = perform_request_wr<void>(
        protocol::client_operation::HEARTBEAT, [](auto&, auto&){}, [self_weak = weak_from_this()](const auto&) {
            if (auto self = self_weak.lock()) {
                self->plan_heartbeat(self->m_heartbeat_interval);
            }
        }
    );

    // We don't care here if we were not able to send a heartbeat due to the connection is dead already.
    UNUSED_VALUE(res);
}

void node_connection::on_heartbeat_timeout() {
    auto idle_for = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - m_last_message_ts);

    if (idle_for > m_heartbeat_interval) {
        send_heartbeat();
    } else {
        auto sleep_for = m_heartbeat_interval - idle_for;
        plan_heartbeat(sleep_for);
    }
}

void node_connection::plan_heartbeat(std::chrono::milliseconds timeout) {
    if (auto timer_thread = m_timer_thread.lock()) {
        timer_thread->add(timeout, [self_weak = weak_from_this()] {
            if (auto self = self_weak.lock()) {
                self->on_heartbeat_timeout();
            }
        });
    }
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

    on_observable_timestamp_changed(response.observable_timestamp);

    auto heartbeat_ms = m_configuration.get_heartbeat_interval().count();
    if (heartbeat_ms) {
        assert(heartbeat_ms > 0);

        heartbeat_ms = std::min(response.idle_timeout_ms / 3, heartbeat_ms);
        heartbeat_ms = std::max(MIN_HEARTBEAT_INTERVAL.count(), heartbeat_ms);
    }
    m_heartbeat_interval = std::chrono::milliseconds(heartbeat_ms);

    m_protocol_context = response.context;
    m_handshake_complete = true;

    if (heartbeat_ms) {
        plan_heartbeat(m_heartbeat_interval);
    }

    if (m_configuration.get_operation_timeout().count() > 0) {
        handle_timeouts();
    }

    return {};
}

std::shared_ptr<response_handler> node_connection::get_and_remove_handler(std::int64_t req_id) {
    std::lock_guard<std::recursive_mutex> lock(m_request_handlers_mutex);

    auto it = m_request_handlers.find(req_id);
    if (it == m_request_handlers.end())
        return {};

    auto res = std::move(it->second.handler);
    m_request_handlers.erase(it);

    return res;
}

std::shared_ptr<response_handler> node_connection::find_handler_unsafe(std::int64_t req_id) {
    auto it = m_request_handlers.find(req_id);
    if (it == m_request_handlers.end())
        return {};

    return it->second.handler;
}

void node_connection::handle_timeouts() {
    auto now = std::chrono::steady_clock::now();

    {
        std::lock_guard lock(m_request_handlers_mutex);

        std::vector<int64_t> keys_for_erasure;

        for (auto& [id, req] : m_request_handlers) {
            if (req.timeouts_at > now) {

                auto res = req.handler->set_error(ignite_error("TIMEOUT!")); // TODO fix wording

                keys_for_erasure.push_back(id);

                if (res.has_error())
                    this->m_logger->log_error(
                        "Uncaught user callback exception while handling operation error: " + res.error().what_str());
            }
        }

        for (int64_t key : keys_for_erasure) {
            m_request_handlers.erase(key);
        }
    }

    if (auto timeout = m_configuration.get_operation_timeout(); timeout.count() > 0) {
        if (auto timer_thread = m_timer_thread.lock()) {
            timer_thread->add(timeout, [self_weak = weak_from_this()] {
                if (auto self = self_weak.lock()) {
                    self->handle_timeouts();
                }
            });
        }
    }
}

} // namespace ignite::detail
