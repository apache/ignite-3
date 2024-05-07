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

#include "ignite/protocol/protocol_context.h"
#include <ignite/client/detail/connection_event_handler.h>
#include <ignite/client/detail/response_handler.h>
#include <ignite/client/ignite_client_configuration.h>
#include <ignite/protocol/client_operation.h>

#include <ignite/common/utils.h>
#include <ignite/network/async_client_pool.h>
#include <ignite/protocol/reader.h>
#include <ignite/protocol/writer.h>

#include <atomic>
#include <future>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace ignite::detail {

class cluster_connection;

/**
 * Represents connection to the cluster.
 *
 * Considered established while there is connection to at least one server.
 */
class node_connection : public std::enable_shared_from_this<node_connection> {
    friend class cluster_connection;

public:
    // Deleted
    node_connection() = delete;
    node_connection(node_connection &&) = delete;
    node_connection(const node_connection &) = delete;
    node_connection &operator=(node_connection &&) = delete;
    node_connection &operator=(const node_connection &) = delete;

    /**
     * Destructor.
     */
    ~node_connection();

    /**
     * Makes new instance.
     *
     * @param id Connection ID.
     * @param pool Connection pool.
     * @param event_handler Event handler.
     * @param logger Logger.
     * @param cfg Configuration.
     * @return New instance.
     */
    static std::shared_ptr<node_connection> make_new(std::uint64_t id, std::shared_ptr<network::async_client_pool> pool,
        std::weak_ptr<connection_event_handler> event_handler, std::shared_ptr<ignite_logger> logger,
        const ignite_client_configuration &cfg) {
        return std::shared_ptr<node_connection>(
            new node_connection(id, std::move(pool), std::move(event_handler), std::move(logger), cfg));
    }

    /**
     * Get connection ID.
     *
     * @return ID.
     */
    [[nodiscard]] std::uint64_t id() const { return m_id; }

    /**
     * Check whether handshake complete.
     *
     * @return @c true if the handshake complete.
     */
    [[nodiscard]] bool is_handshake_complete() const { return m_handshake_complete; }

    /**
     * Send request.
     *
     * @tparam T Result type.
     * @param op Operation code.
     * @param wr Writer function.
     * @param handler response handler.
     * @return @c true on success and @c false otherwise.
     */
    bool perform_request(protocol::client_operation op, const std::function<void(protocol::writer &)> &wr,
        std::shared_ptr<response_handler> handler) {
        auto req_id = generate_request_id();
        std::vector<std::byte> message;
        {
            protocol::buffer_adapter buffer(message);
            buffer.reserve_length_header();

            protocol::writer writer(buffer);
            writer.write(std::int32_t(op));
            writer.write(req_id);
            wr(writer);

            buffer.write_length_header();
        }

        {
            std::lock_guard<std::recursive_mutex> lock(m_request_handlers_mutex);
            m_request_handlers[req_id] = std::move(handler);
        }

        if (m_logger->is_debug_enabled()) {
            m_logger->log_debug(
                "Performing request: op=" + std::to_string(int(op)) + ", req_id=" + std::to_string(req_id));
        }

        bool sent = m_pool->send(m_id, std::move(message));
        if (!sent) {
            get_and_remove_handler(req_id);
            return false;
        }
        return true;
    }

    /**
     * Perform request.
     *
     * @tparam T Result type.
     * @param op Operation code.
     * @param wr Request writer function.
     * @param rd response reader function.
     * @param callback Callback to call on result.
     * @return Channel used for the request.
     */
    template<typename T>
    bool perform_request(protocol::client_operation op, const std::function<void(protocol::writer &)> &wr,
        std::function<T(protocol::reader &)> rd, ignite_callback<T> callback) {
        auto handler = std::make_shared<response_handler_reader<T>>(std::move(rd), std::move(callback));
        return perform_request(op, wr, std::move(handler));
    }

    /**
     * Perform request without output data.
     *
     * @tparam T Result type.
     * @param op Operation code.
     * @param wr Request writer function.
     * @param callback Callback to call on result.
     * @return Channel used for the request.
     */
    template<typename T>
    void perform_request_wr(
        protocol::client_operation op, const std::function<void(protocol::writer &)> &wr, ignite_callback<T> callback) {
        perform_request<T>(
            op, wr, [](protocol::reader &) {}, std::move(callback));
    }

    /**
     * Perform handshake.
     *
     * @return @c true on success and @c false otherwise.
     */
    bool handshake();

    /**
     * Callback that called when new message is received.
     *
     * @param msg Received message.
     */
    void process_message(bytes_view msg);

    /**
     * Process handshake response.
     *
     * @param msg Handshake response message.
     */
    ignite_result<void> process_handshake_rsp(bytes_view msg);

    /**
     * Gets protocol context.
     *
     * @return Protocol context.
     */
    const protocol::protocol_context &get_protocol_context() const { return m_protocol_context; }

private:
    /**
     * Constructor.
     *
     * @param id Connection ID.
     * @param pool Connection pool.
     * @param event_handler Event handler.
     * @param logger Logger.
     * @param cfg Configuration.
     */
    node_connection(std::uint64_t id, std::shared_ptr<network::async_client_pool> pool,
        std::weak_ptr<connection_event_handler> event_handler, std::shared_ptr<ignite_logger> logger,
        const ignite_client_configuration &cfg);

    /**
     * Generate next request ID.
     *
     * @return New request ID.
     */
    [[nodiscard]] std::int64_t generate_request_id() { return m_req_id_gen.fetch_add(1, std::memory_order_relaxed); }

    /**
     * Get and remove request handler.
     *
     * @param reqId Request ID.
     * @return Handler.
     */
    std::shared_ptr<response_handler> get_and_remove_handler(std::int64_t req_id);

    /**
     * Find handler by ID.
     * @warning Warning: m_request_handlers_mutex should be locked.
     *
     * @param reqId Request ID.
     * @return Handler.
     */
    std::shared_ptr<response_handler> find_handler_unsafe(std::int64_t req_id);

    /**
     * Notify event handler about observable timestamp change.
     *
     * @param observable_timestamp New observable timestamp.
     */
    void on_observable_timestamp_changed(int64_t observable_timestamp) const;

    /** Handshake complete. */
    bool m_handshake_complete{false};

    /** Protocol context. */
    protocol::protocol_context m_protocol_context;

    /** Connection ID. */
    std::uint64_t m_id{0};

    /** Connection pool. */
    std::shared_ptr<network::async_client_pool> m_pool;

    /** Connection event handler. */
    std::weak_ptr<connection_event_handler> m_event_handler;

    /** Request ID generator. */
    std::atomic_int64_t m_req_id_gen{0};

    /** Pending request handlers. */
    std::unordered_map<std::int64_t, std::shared_ptr<response_handler>> m_request_handlers;

    /** Handlers map mutex. */
    std::recursive_mutex m_request_handlers_mutex;

    /** Logger. */
    std::shared_ptr<ignite_logger> m_logger;

    /** Configuration. */
    const ignite_client_configuration &m_configuration;
};

} // namespace ignite::detail
