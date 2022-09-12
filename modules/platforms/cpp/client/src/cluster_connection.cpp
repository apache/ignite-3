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

#include <random>

#include "common/utils.h"

#include "ignite/network/codec.h"
#include "ignite/network/codec_data_filter.h"
#include "ignite/network/length_prefix_codec.h"
#include "ignite/network/network.h"

#include "ignite/protocol/writer.h"
#include "ignite/protocol/reader.h"
#include "ignite/protocol/utils.h"

#include "cluster_connection.h"

namespace
{

/**
 * Make random GUID.
 *
 * @return Random GUID instance.
 */
ignite::Guid makeRandomGuid()
{
    static std::random_device rd;
    static std::mt19937 gen(rd());

    std::uniform_int_distribution<int64_t> distrib;
    return {distrib(gen), distrib(gen)};
}

} // namespace

namespace ignite::impl
{

ClusterConnection::ClusterConnection(IgniteClientConfiguration configuration) :
    m_configuration(std::move(configuration)),
    m_pool(),
    m_logger(m_configuration.getLogger()) { }

std::future<void> ClusterConnection::start()
{
    using namespace network;

    if (m_pool)
        return makeFutureError<void>(IgniteError("Client is already started"));

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

    // TODO: Implement connection limit.
    m_pool->start(addrs, 0);

    return m_initialConnect.get_future();
}

void ClusterConnection::stop()
{
    auto pool = m_pool;
    if (pool)
        pool->stop();
}

void ClusterConnection::onConnectionSuccess(const network::EndPoint &addr, uint64_t id)
{
    m_logger->logInfo("Established connection with remote host " + addr.toString());
    m_logger->logDebug("Connection ID: " + std::to_string(id));

    auto protocolCtx = std::make_shared<ProtocolContext>();

    {
        std::lock_guard<std::recursive_mutex> lock(m_inProgressMutex);

        if (m_inProgress.find(id) != m_inProgress.end())
            m_logger->logError("Unknown error: connecting is already in progress. Connection ID: " + std::to_string(id));

        m_inProgress[id] = protocolCtx;
    }

    handshake(id, *protocolCtx);
}

void ClusterConnection::onConnectionError(const network::EndPoint &addr, const IgniteError &err)
{
    m_logger->logWarning("Failed to establish connection with remote host " + addr.toString() +
        ", reason: " + err.what());

    if (err.getStatusCode() == StatusCode::OS)
        handshakeFail(0, err);
}

void ClusterConnection::onConnectionClosed(uint64_t id, const IgniteError *err)
{
    m_logger->logDebug("Closed Connection ID " + std::to_string(id));

    {
        std::lock_guard<std::recursive_mutex> lock(m_inProgressMutex);

        auto it = m_inProgress.find(id);
        if (it != m_inProgress.end())
        {
            handshakeFail(id, std::nullopt);

            // No sense in locking connections mutex - connection was not established.
            // Returning early.
            return;
        }
    }

    {
        std::lock_guard<std::recursive_mutex> lock(m_connectionsMutex);

        m_connections.erase(id);
    }
}

void ClusterConnection::onMessageReceived(uint64_t id, const network::DataBuffer &msg)
{
    m_logger->logDebug("Message on Connection ID " + std::to_string(id) +
        ", size: " + std::to_string(msg.getSize()));

    std::shared_ptr<NodeConnection> connection;
    {
        std::lock_guard<std::recursive_mutex> lock(m_inProgressMutex);

        auto it = m_inProgress.find(id);
        if (it != m_inProgress.end())
        {
            try
            {
                handshakeRsp(*it->second, msg);

                connection = std::make_shared<NodeConnection>(id, m_pool);

                m_inProgress.erase(it);
            }
            catch (const IgniteError& err)
            {
                handshakeFail(id, err);

                // No sense in locking connections mutex - connection was not established.
                // Returning early.
                return;
            }
        }
    }

    {
        std::lock_guard<std::recursive_mutex> lock(m_connectionsMutex);

        {
            auto it = m_connections.find(id);
            if (connection)
            {
                if (it != m_connections.end())
                    m_logger->logError("Unknown error: connection already established. Connection ID: " + std::to_string(id));

                m_connections[id] = connection;
                m_initialConnect.set_value();
            }
            else
                connection = it->second;
        }
    }

    if (connection)
        connection->processMessage(msg);
}

void ClusterConnection::onMessageSent(uint64_t id)
{
    m_logger->logDebug("Message sent successfully on Connection ID " + std::to_string(id));
}

void ClusterConnection::handshake(uint64_t id, ProtocolContext& context)
{
    static constexpr int8_t CLIENT_TYPE = 2;

    auto buffer = std::make_shared<protocol::Buffer>();
    buffer->writeRawData(BytesView(protocol::MAGIC_BYTES.data(), protocol::MAGIC_BYTES.size()));

    protocol::Writer::writeMessageToBuffer(*buffer, [&context](protocol::Writer& writer) {
        auto ver = context.getVersion();

        writer.write(ver.getMajor());
        writer.write(ver.getMinor());
        writer.write(ver.getPatch());

        writer.write(CLIENT_TYPE);

        // Features.
        writer.writeBinaryEmpty();

        // Extensions.
        writer.writeMapEmpty();
    });

    network::DataBuffer dataBuffer(buffer);

    try
    {
        bool res = m_pool->send(id, dataBuffer);
        m_logger->logDebug("Handshake sent successfully");
    }
    catch (const IgniteError& err)
    {
        m_logger->logWarning("Failed to send handshake request: " + err.whatStr());
        handshakeFail(id, std::nullopt);
    }
}

void ClusterConnection::handshakeFail(uint64_t id, std::optional<IgniteError> err)
{
    std::lock_guard<std::recursive_mutex> lock(m_inProgressMutex);

    m_inProgress.erase(id);

    if (err)
        m_initialConnect.set_exception(std::make_exception_ptr<IgniteError>(std::move(err.value())));
}

void ClusterConnection::handshakeRsp(ProtocolContext& protocolCtx, const network::DataBuffer& buffer)
{
    m_logger->logDebug("Got handshake response");

    protocol::Reader reader(buffer.getBytesView());

    auto verMajor = reader.readInt16();
    auto verMinor = reader.readInt16();
    auto verPatch = reader.readInt16();

    ProtocolVersion ver(verMajor, verMinor, verPatch);

    m_logger->logDebug("Server-side protocol version: " + ver.toString());

    // We now only support a single version
    if (ver != ProtocolContext::CURRENT_VERSION)
        throw IgniteError("Unsupported server version: " + ver.toString());

    auto err = readError(reader);
    if (err)
        throw IgniteError(err.value());

    (void) reader.readInt64(); // TODO: IGNITE-17606 Implement heartbeats
    (void) reader.readStringNullable(); // Cluster node ID. Needed for partition-aware compute.
    (void) reader.readStringNullable(); // Cluster node name. Needed for partition-aware compute.

    reader.skip(); // Features.
    reader.skip(); // Extensions.

    protocolCtx.setVersion(ver);
}

std::optional<IgniteError> ClusterConnection::readError(protocol::Reader &reader)
{
    if (reader.tryReadNil())
        return std::nullopt;

    Guid traceId = reader.tryReadNil() ? makeRandomGuid() : reader.readGuid();
    int32_t code = reader.tryReadNil() ? 65537 : reader.readInt32();
    std::string className = reader.readString();
    std::string message = reader.readString();

    std::stringstream errMsgBuilder;

    errMsgBuilder << className << ": " << message << " (" << code << ", " << traceId << ")";

    return {IgniteError(StatusCode(code), errMsgBuilder.str())};
}

} // namespace ignite::impl