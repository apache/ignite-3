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

#include "ignite/client/detail/cluster_connection.h"
#include "ignite/client/detail/logger_wrapper.h"

#include "ignite/network/codec.h"
#include "ignite/network/codec_data_filter.h"
#include "ignite/network/length_prefix_codec.h"
#include "ignite/network/network.h"
#include "ignite/protocol/writer.h"

#include <iterator>

namespace ignite::detail {

cluster_connection::cluster_connection(ignite_client_configuration configuration)
    : m_configuration(std::move(configuration))
    , m_pool()
    , m_logger(std::make_shared<logger_wrapper>(m_configuration.get_logger()))
    , m_generator(std::random_device()()) {
}

void cluster_connection::start_async(std::function<void(ignite_result<void>)> callback) {
    using namespace network;

    if (m_pool)
        throw ignite_error("Client is already started");

    std::vector<tcp_range> addrs;
    addrs.reserve(m_configuration.get_endpoints().size());
    for (const auto &str_addr : m_configuration.get_endpoints()) {
        std::optional<tcp_range> ep = tcp_range::parse(str_addr, DEFAULT_TCP_PORT);
        if (!ep)
            throw ignite_error("Can not parse address range: " + str_addr);

        addrs.push_back(std::move(ep.value()));
    }

    data_filters filters;

    std::shared_ptr<factory<codec>> codec_factory = std::make_shared<length_prefix_codec_factory>();
    std::shared_ptr<codec_data_filter> codec_filter(new network::codec_data_filter(codec_factory));
    filters.push_back(codec_filter);

    m_pool = network::make_async_client_pool(filters);

    m_pool->set_handler(shared_from_this());

    m_on_initial_connect = std::move(callback);

    m_pool->start(std::move(addrs), m_configuration.get_connection_limit());
}

void cluster_connection::stop() {
    auto pool = m_pool;
    if (pool)
        pool->stop();
}

void cluster_connection::on_connection_success(const end_point &addr, uint64_t id) {
    m_logger->log_info("Established connection with remote host " + addr.to_string());
    m_logger->log_debug("Connection ID: " + std::to_string(id));

    auto connection = node_connection::make_new(id, m_pool, weak_from_this(), m_logger, m_configuration);
    {
        [[maybe_unused]] std::unique_lock<std::recursive_mutex> lock(m_connections_mutex);

        auto [_it, was_new] = m_connections.insert_or_assign(id, connection);
        if (!was_new)
            m_logger->log_error(
                "Unknown error: connecting is already in progress. Connection ID: " + std::to_string(id));
    }

    try {
        bool res = connection->handshake();
        if (!res) {
            m_logger->log_warning("Failed to send handshake request: Connection already closed.");
            remove_client(id);
            return;
        }
        m_logger->log_debug("Handshake sent successfully");
    } catch (const ignite_error &err) {
        m_logger->log_warning("Failed to send handshake request: " + err.what_str());
        remove_client(id);
    }
}

void cluster_connection::on_connection_error(const end_point &addr, ignite_error err) {
    m_logger->log_warning(
        "Failed to establish connection with remote host " + addr.to_string() + ", reason: " + err.what());

    if (err.get_status_code() == error::code::INTERNAL || err.get_status_code() == error::code::GENERIC)
        initial_connect_result(std::move(err));
}

void cluster_connection::on_connection_closed(uint64_t id, std::optional<ignite_error> err) {
    m_logger->log_debug("Closed Connection ID " + std::to_string(id) + ", error=" + (err ? err->what() : "none"));
    remove_client(id);
}

void cluster_connection::on_message_received(uint64_t id, bytes_view msg) {
    if (m_logger->is_debug_enabled())
        m_logger->log_debug("Message on Connection ID " + std::to_string(id) + ", size: " + std::to_string(msg.size()));

    std::shared_ptr<node_connection> connection = find_client(id);
    if (!connection)
        return;

    if (connection->is_handshake_complete()) {
        connection->process_message(msg);
        return;
    }

    auto res = connection->process_handshake_rsp(msg);
    if (res.has_error()) {
        initial_connect_result(std::move(res));
        remove_client(connection->id());
        return;
    }

    auto &context = connection->get_protocol_context();
    initial_connect_result(context);

    if (context.get_cluster_id() != m_cluster_id) {
        std::stringstream message;
        message << "Node from unknown cluster: current_cluster_id=" << m_cluster_id
                << ", node_cluster_id=" << context.get_cluster_id();

        m_logger->log_warning(message.str());
        remove_client(connection->id());
    }
}

std::shared_ptr<node_connection> cluster_connection::find_client(uint64_t id) {
    [[maybe_unused]] std::unique_lock<std::recursive_mutex> lock(m_connections_mutex);

    auto it = m_connections.find(id);
    if (it != m_connections.end())
        return it->second;

    return {};
}

void cluster_connection::on_message_sent(uint64_t id) {
    if (m_logger->is_debug_enabled())
        m_logger->log_debug("Message sent successfully on Connection ID " + std::to_string(id));
}

void cluster_connection::on_observable_timestamp_changed(std::int64_t timestamp) {
    auto expected = m_observable_timestamp.load();
    while (expected < timestamp) {
        auto success = m_observable_timestamp.compare_exchange_weak(expected, timestamp);
        if (success)
            return;
        expected = m_observable_timestamp.load();
    }
}

void cluster_connection::remove_client(uint64_t id) {
    [[maybe_unused]] std::unique_lock<std::recursive_mutex> lock(m_connections_mutex);

    m_connections.erase(id);
}

void cluster_connection::initial_connect_result(ignite_result<void> &&res) {
    [[maybe_unused]] std::lock_guard<std::mutex> lock(m_on_initial_connect_mutex);

    if (!m_on_initial_connect)
        return;

    m_on_initial_connect(std::move(res));
    m_on_initial_connect = {};
}

void cluster_connection::initial_connect_result(const protocol::protocol_context &context) {
    [[maybe_unused]] std::lock_guard<std::mutex> lock(m_on_initial_connect_mutex);

    if (!m_on_initial_connect)
        return;

    m_cluster_id = context.get_cluster_id();
    m_on_initial_connect({});
    m_on_initial_connect = {};
}

std::shared_ptr<node_connection> cluster_connection::get_random_channel() {
    [[maybe_unused]] std::unique_lock<std::recursive_mutex> lock(m_connections_mutex);

    if (m_connections.empty())
        return {};

    if (m_connections.size() == 1)
        return m_connections.begin()->second;

    std::uniform_int_distribution<size_t> distrib(0, m_connections.size() - 1);
    auto idx = ptrdiff_t(distrib(m_generator));
    return std::next(m_connections.begin(), idx)->second;
}

void cluster_connection::perform_request_handler(protocol::client_operation op, transaction_impl *tx,
    const std::function<void(protocol::writer &)> &wr, const std::shared_ptr<response_handler> &handler) {
    if (tx) {
        auto channel = tx->get_connection();
        if (!channel)
            throw ignite_error("Transaction was not started properly");

        auto res = channel->perform_request(op, wr, handler);
        if (!res)
            throw ignite_error("Connection associated with the transaction is closed");

        return;
    }

    while (true) {
        auto channel = get_random_channel();
        if (!channel)
            throw ignite_error("No nodes connected");

        auto res = channel->perform_request(op, wr, handler);
        if (res)
            return;
    }
}

void cluster_connection::perform_request_raw(protocol::client_operation op, transaction_impl *tx,
    const std::function<void(protocol::writer &)> &wr, ignite_callback<bytes_view> callback) {
    auto handler = std::make_shared<response_handler_raw>(std::move(callback));
    perform_request_handler(op, tx, wr, std::move(handler));
}

} // namespace ignite::detail
