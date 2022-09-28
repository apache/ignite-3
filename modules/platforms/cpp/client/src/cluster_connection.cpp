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

#include <iterator>

#include "ignite/network/codec.h"
#include "ignite/network/codec_data_filter.h"
#include "ignite/network/length_prefix_codec.h"
#include "ignite/network/network.h"

#include "ignite/protocol/writer.h"

#include "cluster_connection.h"

namespace ignite::detail
{

ClusterConnection::ClusterConnection(IgniteClientConfiguration configuration) :
    m_configuration(std::move(configuration)),
    m_pool(),
    m_logger(m_configuration.getLogger()),
    m_generator(std::random_device()()) { }

void ClusterConnection::startAsync(std::function<void(IgniteResult<void>)> callback) {
    using namespace network;

    if (m_pool)
        throw IgniteError("Client is already started");

    std::vector<TcpRange> addrs;
    addrs.reserve(m_configuration.getEndpoints().size());
    for (const auto& strAddr : m_configuration.getEndpoints())
    {
        std::optional<TcpRange> ep = TcpRange::parse(strAddr, DEFAULT_TCP_PORT);
        if (!ep)
            throw IgniteError("Can not parse address range: " + strAddr);

        addrs.push_back(std::move(ep.value()));
    }

    DataFilters filters;

    std::shared_ptr<Factory<Codec>> codecFactory = std::make_shared<LengthPrefixCodecFactory>();
    std::shared_ptr<CodecDataFilter> codecFilter(new network::CodecDataFilter(codecFactory));
    filters.push_back(codecFilter);

    m_pool = network::makeAsyncClientPool(filters);

    m_pool->setHandler(shared_from_this());

    m_onInitialConnect = std::move(callback);

    m_pool->start(std::move(addrs), m_configuration.getConnectionLimit());
}

void ClusterConnection::stop() {
    auto pool = m_pool;
    if (pool)
        pool->stop();
}

void ClusterConnection::onConnectionSuccess(const network::EndPoint &addr, uint64_t id) {
    m_logger->logInfo("Established connection with remote host " + addr.toString());
    m_logger->logDebug("Connection ID: " + std::to_string(id));

    auto connection = std::make_shared<NodeConnection>(id, m_pool, m_logger);
    {
        [[maybe_unused]]
        std::unique_lock<std::recursive_mutex> lock(m_connectionsMutex);

        auto [_it, wasNew] = m_connections.insert_or_assign(id, connection);
        if (!wasNew)
            m_logger->logError("Unknown error: connecting is already in progress. Connection ID: " + std::to_string(id));
    }

    try {
        bool res = connection->handshake();
        if (!res) {
            m_logger->logWarning("Failed to send handshake request: Connection already closed.");
            removeClient(id);
            return;
        }
        m_logger->logDebug("Handshake sent successfully");
    }
    catch (const IgniteError& err) {
        m_logger->logWarning("Failed to send handshake request: " + err.whatStr());
        removeClient(id);
    }
}

void ClusterConnection::onConnectionError(const network::EndPoint &addr, IgniteError err) {
    m_logger->logWarning("Failed to establish connection with remote host " + addr.toString() +
        ", reason: " + err.what());

    if (err.getStatusCode() == StatusCode::OS)
        initialConnectResult(std::move(err));
}

void ClusterConnection::onConnectionClosed(uint64_t id, std::optional<IgniteError> err) {
    m_logger->logDebug("Closed Connection ID " + std::to_string(id) + ", error=" + (err ? err->what() : "none"));
    removeClient(id);
}

void ClusterConnection::onMessageReceived(uint64_t id, BytesView msg) {
    m_logger->logDebug("Message on Connection ID " + std::to_string(id) +
        ", size: " + std::to_string(msg.size()));

    std::shared_ptr<NodeConnection> connection = findClient(id);
    if (!connection)
        return;

    if (connection->isHandshakeComplete()) {
        connection->processMessage(msg);
        return;
    }

    auto res = connection->processHandshakeRsp(msg);
    if (res.hasError())
        removeClient(connection->getId());

    initialConnectResult(std::move(res));
}

std::shared_ptr<NodeConnection> ClusterConnection::findClient(uint64_t id) {
    [[maybe_unused]]
    std::unique_lock<std::recursive_mutex> lock(m_connectionsMutex);

    auto it = m_connections.find(id);
    if (it != m_connections.end())
        return it->second;

    return {};
}

void ClusterConnection::onMessageSent(uint64_t id) {
    m_logger->logDebug("Message sent successfully on Connection ID " + std::to_string(id));
}

void ClusterConnection::removeClient(uint64_t id) {
    [[maybe_unused]]
    std::unique_lock<std::recursive_mutex> lock(m_connectionsMutex);

    m_connections.erase(id);
}

void ClusterConnection::initialConnectResult(IgniteResult<void>&& res) {
    [[maybe_unused]]
    std::lock_guard<std::mutex> lock(m_onInitialConnectMutex);

    if (!m_onInitialConnect)
        return;

    m_onInitialConnect(std::move(res));
    m_onInitialConnect = {};
}

std::shared_ptr<NodeConnection> ClusterConnection::getRandomChannel() {
    [[maybe_unused]]
    std::unique_lock<std::recursive_mutex> lock(m_connectionsMutex);

    if (m_connections.empty())
        return {};

    if (m_connections.size() == 1)
        return m_connections.begin()->second;

    std::uniform_int_distribution<size_t> distrib(0, m_connections.size() - 1);
    auto idx = ptrdiff_t(distrib(m_generator));
    return std::next(m_connections.begin(), idx)->second;
}

} // namespace ignite::detail
