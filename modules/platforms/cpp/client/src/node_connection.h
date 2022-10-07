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

#include "client_operation.h"
#include "protocol_context.h"
#include "response_handler.h"

#include "ignite/ignite_client_configuration.h"

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

class ClusterConnection;

/**
 * Represents connection to the cluster.
 *
 * Considered established while there is connection to at least one server.
 */
class NodeConnection {
    friend class ClusterConnection;

public:
    // Deleted
    NodeConnection() = delete;
    NodeConnection(NodeConnection &&) = delete;
    NodeConnection(const NodeConnection &) = delete;
    NodeConnection &operator=(NodeConnection &&) = delete;
    NodeConnection &operator=(const NodeConnection &) = delete;

    /**
     * Destructor.
     */
    ~NodeConnection();

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
    [[nodiscard]] uint64_t getId() const { return m_id; }

    /**
     * Check whether handshake complete.
     *
     * @return @c true if the handshake complete.
     */
    [[nodiscard]] bool isHandshakeComplete() const { return m_handshakeComplete; }

    /**
     * Send request.
     *
     * @tparam T Result type.
     * @param op Operation code.
     * @param wr Writer function.
     * @param handler Response handler.
     * @return @c true on success and @c false otherwise.
     */
    template <typename T>
    bool performRequest(ClientOperation op, const std::function<void(protocol::writer &)> &wr,
        std::shared_ptr<ResponseHandlerImpl<T>> handler) {
        auto reqId = generateRequestId();
        std::vector<std::byte> message;
        {
            protocol::buffer_adapter buffer(message);
            buffer.reserve_length_header();

            protocol::writer writer(buffer);
            writer.write(int32_t(op));
            writer.write(reqId);
            wr(writer);

            buffer.write_length_header();

            {
                std::lock_guard<std::mutex> lock(m_requestHandlersMutex);
                m_requestHandlers[reqId] = std::move(handler);
            }
        }

        bool sent = m_pool->send(m_id, std::move(message));
        if (!sent) {
            std::lock_guard<std::mutex> lock(m_requestHandlersMutex);
            getAndRemoveHandler(reqId);

            return false;
        }
        return true;
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
    void processMessage(bytes_view msg);

    /**
     * Process handshake response.
     *
     * @param msg Handshake response message.
     */
    ignite_result<void> processHandshakeRsp(bytes_view msg);

private:
    /**
     * Generate next request ID.
     *
     * @return New request ID.
     */
    [[nodiscard]] int64_t generateRequestId() { return m_reqIdGen.fetch_add(1, std::memory_order_relaxed); }

    /**
     * Get and remove request handler.
     *
     * @param reqId Request ID.
     * @return Handler.
     */
    std::shared_ptr<ResponseHandler> getAndRemoveHandler(int64_t reqId);

    /** Handshake complete. */
    bool m_handshakeComplete{false};

    /** Protocol context. */
    ProtocolContext m_protocolContext;

    /** Connection ID. */
    uint64_t m_id{0};

    /** Connection pool. */
    std::shared_ptr<network::AsyncClientPool> m_pool;

    /** Request ID generator. */
    std::atomic_int64_t m_reqIdGen{0};

    /** Pending request handlers. */
    std::unordered_map<int64_t, std::shared_ptr<ResponseHandler>> m_requestHandlers;

    /** Handlers map mutex. */
    std::mutex m_requestHandlersMutex;

    /** Logger. */
    std::shared_ptr<IgniteLogger> m_logger;
};

} // namespace ignite::detail
