/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

#include <array>
#include <future>
#include <memory>
#include <unordered_map>
#include <mutex>

#include "ignite/network/async_client_pool.h"

#include "ignite/ignite_client_configuration.h"
#include "node_connection.h"
#include "protocol_context.h"

namespace ignite::protocol
{

class Reader;

}

namespace ignite::impl
{

/**
 * Represents connection to the cluster.
 *
 * Considered established while there is connection to at least one server.
 */
class ClusterConnection : public std::enable_shared_from_this<ClusterConnection>, public network::AsyncHandler
{
public:
    /** Default TCP port. */
    static constexpr uint16_t DEFAULT_TCP_PORT = 10800;

    // Deleted
    ClusterConnection() = delete;
    ClusterConnection(ClusterConnection&&) = delete;
    ClusterConnection(const ClusterConnection&) = delete;
    ClusterConnection& operator=(ClusterConnection&&) = delete;
    ClusterConnection& operator=(const ClusterConnection&) = delete;

    // Default
    ~ClusterConnection() override = default;

    /**
     * Start establishing connection.
     *
     * @return Future representing finishing of the connection process.
     */
    std::future<void> start();

    /**
     * Stop connection.
     *
     * @return Future representing finishing of the connection process.
     */
    void stop();

    /**
     * Create new instance of the object.
     *
     * @param configuration Configuration.
     * @return New instance.
     */
    static std::shared_ptr<ClusterConnection> create(IgniteClientConfiguration configuration)
    {
        return std::shared_ptr<ClusterConnection>(new ClusterConnection(std::move(configuration)));
    }

private:
    /**
     * Constructor.
     *
     * @param configuration Configuration.
     */
    explicit ClusterConnection(IgniteClientConfiguration configuration);

    /**
     * Callback that called on successful connection establishment.
     *
     * @param addr Address of the new connection.
     * @param id Connection ID.
     */
    void onConnectionSuccess(const network::EndPoint& addr, uint64_t id) override;

    /**
     * Callback that called on error during connection establishment.
     *
     * @param addr Connection address.
     * @param err Error.
     */
    void onConnectionError(const network::EndPoint& addr, const IgniteError& err) override;

    /**
     * Callback that called on error during connection establishment.
     *
     * @param id Async client ID.
     * @param err Error. Can be null if connection closed without error.
     */
    void onConnectionClosed(uint64_t id, const IgniteError* err) override;

    /**
     * Callback that called when new message is received.
     *
     * @param id Async client ID.
     * @param msg Received message.
     */
    void onMessageReceived(uint64_t id, const network::DataBuffer& msg) override;

    /**
     * Callback that called when message is sent.
     *
     * @param id Async client ID.
     */
    void onMessageSent(uint64_t id) override;

    /**
     * Perform handshake.
     *
     * @param id Connection id.
     * @param context Handshake context.
     */
    void handshake(uint64_t id, ProtocolContext &context);

    /**
     * Process handshake failure.
     *
     * @param id Connection ID.
     * @param err Error. If set, connection is stopped and failed.
     */
    void handshakeFail(uint64_t id, std::optional<IgniteError> err);

    /**
     * Process handshake response.
     *
     * @param protocolCtx Handshake context.
     * @param buffer Message.
     */
    void handshakeRsp(ProtocolContext &protocolCtx, const network::DataBuffer &buffer);

    /**
     * Read error.
     *
     * @param reader Reader.
     * @return Error.
     */
    static std::optional<IgniteError> readError(protocol::Reader& reader);

    /** Configuration. */
    const IgniteClientConfiguration m_configuration;

    /** Initial connect promise. */
    std::promise<void> m_initialConnect;

    /** Connection pool. */
    std::shared_ptr<network::AsyncClientPool> m_pool;

    /** Logger. */
    std::shared_ptr<IgniteLogger> m_logger;

    /** Node connections in progress. */
    std::unordered_map<uint64_t, std::shared_ptr<ProtocolContext>> m_inProgress;

    /** Node connections in progress mutex. */
    std::recursive_mutex m_inProgressMutex;

    /** Node connections. */
    std::unordered_map<uint64_t, std::shared_ptr<NodeConnection>> m_connections;

    /** Connections mutex. */
    std::recursive_mutex m_connectionsMutex;
};

} // namespace ignite::impl
