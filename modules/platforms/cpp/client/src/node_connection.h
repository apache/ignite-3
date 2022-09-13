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

#include <future>
#include <memory>
#include <atomic>
#include <mutex>
#include <unordered_map>

#include "common/utils.h"

#include "ignite/protocol/reader.h"
#include "ignite/protocol/writer.h"
#include "ignite/network/async_client_pool.h"
#include "ignite/ignite_client_configuration.h"

#include "client_operation.h"

namespace ignite::impl
{

class ClusterConnection;

/**
 * Represents connection to the cluster.
 *
 * Considered established while there is connection to at least one server.
 */
class NodeConnection
{
    friend class ClusterConnection;
public:
    // Deleted
    NodeConnection() = delete;
    NodeConnection(NodeConnection&&) = delete;
    NodeConnection(const NodeConnection&) = delete;
    NodeConnection& operator=(NodeConnection&&) = delete;
    NodeConnection& operator=(const NodeConnection&) = delete;

    // Default
    ~NodeConnection() = default;

    /**
     * Constructor.
     *
     * @param id Connection ID.
     * @param pool Connection pool.
     * @param logger Logger.
     */
    NodeConnection(uint64_t id, std::shared_ptr<network::AsyncClientPool> pool, std::shared_ptr<IgniteLogger> logger);

    /**
     * Get connection ID.
     *
     * @return ID.
     */
    [[nodiscard]]
    uint64_t getId() const
    {
        return m_id;
    }

    /**
     * Send request.
     *
     * @tparam T Result type.
     * @param op Operation code.
     * @param wr Writer function.
     * @param rd Reader function.
     * @return Future result.
     */
    template<typename T>
    std::future<T> performRequest(ClientOperation op,
        const std::function<void(protocol::Writer&)>& wr, std::packaged_task<T(protocol::Reader&)>&& rd)
    {
        auto reqId = generateRequestId();
        auto buffer = std::make_shared<protocol::Buffer>();

        buffer->reserveLengthHeader();

        protocol::Writer writer(*buffer);
        writer.write(int32_t(op));
        writer.write(reqId);
        wr(writer);

        buffer->writeLengthHeader();

        bool sent = m_pool->send(m_id, network::DataBuffer(std::move(buffer)));
        if (!sent)
            return {};

        auto fut = rd.get_future();
        auto task = std::make_shared<std::packaged_task<T(protocol::Reader&)>>(std::move(rd));
        std::function<void(protocol::Reader&)> handler = [task = std::move(task)] (protocol::Reader& reader) {
            (*task)(reader);
        };

        {
            std::lock_guard<std::mutex> lock(m_requestHandlersMutex);

            m_requestHandlers[reqId] = handler;
        }

        return fut;
    }

private:
    /**
     * Callback that called when new message is received.
     *
     * @param msg Received message.
     */
    void processMessage(const network::DataBuffer& msg);

    /**
     * Generate next request ID.
     *
     * @return New request ID.
     */
    [[nodiscard]]
    int64_t generateRequestId()
    {
        return m_reqIdGen.fetch_add(1, std::memory_order_seq_cst);
    }

    /**
     * Get and remove request handler.
     *
     * @param reqId Request ID.
     * @return Handler.
     */
    std::function<void(protocol::Reader&)> getAndRemoveHandler(int64_t reqId);

    /** Connection ID. */
    uint64_t m_id;

    /** Handshake result. */
    std::promise<void> handshakeRes;

    /** Connection pool. */
    std::shared_ptr<network::AsyncClientPool> m_pool;

    /** Request ID generator. */
    std::atomic_int64_t m_reqIdGen;

    /** Pending request handlers. */
    std::unordered_map<int64_t, std::function<void(protocol::Reader&)>> m_requestHandlers;

    /** Handlers map mutex. */
    std::mutex m_requestHandlersMutex;

    /** Logger. */
    std::shared_ptr<IgniteLogger> m_logger;
};

} // namespace ignite::impl
