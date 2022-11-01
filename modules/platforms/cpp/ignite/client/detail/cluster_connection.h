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

#include <ignite/client/detail/client_operation.h>
#include <ignite/client/detail/node_connection.h>
#include <ignite/client/detail/protocol_context.h>
#include <ignite/client/detail/response_handler.h>
#include <ignite/client/ignite_client_configuration.h>

#include <ignite/common/ignite_result.h>
#include <ignite/network/async_client_pool.h>
#include <ignite/protocol/reader.h>
#include <ignite/protocol/writer.h>

#include <array>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <random>
#include <unordered_map>

namespace ignite::protocol {

class reader;

} // namespace ignite::protocol

namespace ignite::detail {

/**
 * Represents connection to the cluster.
 *
 * Considered established while there is connection to at least one server.
 */
class cluster_connection : public std::enable_shared_from_this<cluster_connection>, public network::async_handler {
public:
    /** Default TCP port. */
    static constexpr uint16_t DEFAULT_TCP_PORT = 10800;

    /**
     * Create new instance of the object.
     *
     * @param configuration Configuration.
     * @return New instance.
     */
    static std::shared_ptr<cluster_connection> create(ignite_client_configuration configuration) {
        return std::shared_ptr<cluster_connection>(new cluster_connection(std::move(configuration)));
    }

    // Deleted
    cluster_connection() = delete;
    cluster_connection(cluster_connection &&) = delete;
    cluster_connection(const cluster_connection &) = delete;
    cluster_connection &operator=(cluster_connection &&) = delete;
    cluster_connection &operator=(const cluster_connection &) = delete;

    /**
     * Destructor.
     */
    ~cluster_connection() override { stop(); }

    /**
     * Start establishing connection.
     *
     * @param callback Callback.
     */
    void start_async(std::function<void(ignite_result<void>)> callback);

    /**
     * Stop connection.
     */
    void stop();

    /**
     * Perform request.
     *
     * @tparam T Result type.
     * @param op Operation code.
     * @param wr Request writer function.
     * @param rd Response reader function.
     * @param callback Callback to call on result.
     */
    template <typename T>
    void perform_request(client_operation op, const std::function<void(protocol::writer &)> &wr,
        std::function<T(protocol::reader&)> rd, ignite_callback<T> callback) {
        auto handler = std::make_shared<response_handler_impl<T>>(std::move(rd), std::move(callback));

        while (true) {
            auto channel = get_random_channel();
            if (!channel)
                throw ignite_error("No nodes connected");

            auto res = channel->perform_request(op, wr, handler);
            if (res)
                return;
        }
    }

    /**
     * Perform request without input data.
     *
     * @tparam T Result type.
     * @param op Operation code.
     * @param rd Response reader function.
     * @param callback Callback to call on result.
     */
    template <typename T>
    void perform_request_rd(client_operation op, std::function<T(protocol::reader&)> rd, ignite_callback<T> callback) {
        perform_request<T>(op, [](protocol::writer &) {}, std::move(rd), std::move(callback));
    }

    /**
     * Perform request without output data.
     *
     * @tparam T Result type.
     * @param op Operation code.
     * @param wr Request writer function.
     * @param callback Callback to call on result.
     */
    template <typename T>
    void perform_request_wr(client_operation op, const std::function<void(protocol::writer &)> &wr, ignite_callback<T> callback) {
        perform_request<T>(op, wr, [](protocol::reader &) {}, std::move(callback));
    }

private:
    /**
     * Get random node connection.
     *
     * @return Random node connection or nullptr if there are no active connections.
     */
    std::shared_ptr<node_connection> get_random_channel();

    /**
     * Constructor.
     *
     * @param configuration Configuration.
     */
    explicit cluster_connection(ignite_client_configuration configuration);

    /**
     * Callback that called on successful connection establishment.
     *
     * @param addr Address of the new connection.
     * @param id Connection ID.
     */
    void on_connection_success(const network::end_point &addr, uint64_t id) override;

    /**
     * Callback that called on error during connection establishment.
     *
     * @param addr Connection address.
     * @param err Error.
     */
    void on_connection_error(const network::end_point &addr, ignite_error err) override;

    /**
     * Callback that called on error during connection establishment.
     *
     * @param id Async client ID.
     * @param err Error. Can be null if connection closed without error.
     */
    void on_connection_closed(uint64_t id, std::optional<ignite_error> err) override;

    /**
     * Callback that called when new message is received.
     *
     * @param id Async client ID.
     * @param msg Received message.
     */
    void on_message_received(uint64_t id, bytes_view msg) override;

    /**
     * Callback that called when message is sent.
     *
     * @param id Async client ID.
     */
    void on_message_sent(uint64_t id) override;

    /**
     * Remove client.
     *
     * @param id Connection ID.
     */
    void remove_client(uint64_t id);

    /**
     * Handle initial connection result.
     *
     * @param res Connect result.
     */
    void initial_connect_result(ignite_result<void> &&res);

    /**
     * Find and return client.
     *
     * @param id Client ID.
     * @return Client if found and nullptr otherwise.
     */
    [[nodiscard]] std::shared_ptr<node_connection> find_client(uint64_t id);

    /** Configuration. */
    const ignite_client_configuration m_configuration;

    /** Callback to call on initial connect. */
    std::function<void(ignite_result<void>)> m_on_initial_connect;

    /** Initial connect mutex. */
    std::mutex m_on_initial_connect_mutex;

    /** Connection pool. */
    std::shared_ptr<network::async_client_pool> m_pool;

    /** Logger. */
    std::shared_ptr<ignite_logger> m_logger;

    /** Node connections. */
    std::unordered_map<uint64_t, std::shared_ptr<node_connection>> m_connections;

    /** Connections mutex. */
    std::recursive_mutex m_connections_mutex;

    /** Generator. */
    std::mt19937 m_generator;
};

} // namespace ignite::detail
