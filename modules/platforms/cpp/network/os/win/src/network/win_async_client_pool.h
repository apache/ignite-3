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

#include <cstdint>
#include <map>
#include <mutex>

#include <common/ignite_error.h>

#include <ignite/network/async_client_pool.h>
#include <ignite/network/async_handler.h>
#include <ignite/network/tcp_range.h>

#include "network/win_async_client.h"
#include "network/win_async_connecting_thread.h"
#include "network/win_async_worker_thread.h"

namespace ignite::network
{
/**
 * Windows-specific implementation of asynchronous client pool.
 */
class WinAsyncClientPool : public AsyncClientPool
{
public:
    /**
     * Constructor
     *
     * @param handler Upper level event handler.
     */
    WinAsyncClientPool();

    /**
     * Destructor.
     */
    ~WinAsyncClientPool() override;

    /**
     * Start internal thread that establishes connections to provided addresses and asynchronously sends and
     * receives messages from them. Function returns either when thread is started and first connection is
     * established or failure happened.
     *
     * @param addrs Addresses to connect to.
     * @param connLimit Connection upper limit. Zero means limit is disabled.
     *
     * @throw IgniteError on error.
     */
    void start(std::vector<TcpRange> addrs, uint32_t connLimit) override;

    /**
     * Close all established connections and stops handling thread.
     */
    void stop() override;

    /**
     * Set handler.
     *
     * @param handler Handler to set.
     */
    void setHandler(std::weak_ptr<AsyncHandler> handler) override;

    /**
     * Send data to specific established connection.
     *
     * @param id Client ID.
     * @param data Data to be sent.
     * @return @c true if connection is present and @c false otherwise.
     *
     * @throw IgniteError on error.
     */
    bool send(uint64_t id, const DataBuffer& data) override;

    /**
     * Closes specified connection if it's established. Connection to the specified address is planned for
     * re-connect. Event is issued to the handler with specified error.
     *
     * @param id Client ID.
     */
    void close(uint64_t id, std::optional<IgniteError> err) override;

    /**
     * Closes and releases memory allocated for client with specified ID.
     * Error is reported to handler.
     *
     * @param id Client ID.
     * @param err Error to report. May be null.
     * @return @c true if connection with specified ID was found.
     */
    void closeAndRelease(uint64_t id, std::optional<IgniteError> err);

    /**
     * Add client to connection map. Notify user.
     *
     * @param client Client.
     * @return Client ID.
     */
    bool addClient(const std::shared_ptr<WinAsyncClient>& client);

    /**
     * Handle error during connection establishment.
     *
     * @param addr Connection address.
     * @param err Error.
     */
    void handleConnectionError(const EndPoint& addr, const IgniteError& err);

    /**
     * Handle successful connection establishment.
     *
     * @param addr Address of the new connection.
     * @param id Connection ID.
     */
    void handleConnectionSuccess(const EndPoint& addr, uint64_t id);

    /**
     * Handle error during connection establishment.
     *
     * @param id Async client ID.
     * @param err Error. Can be null if connection closed without error.
     */
    void handleConnectionClosed(uint64_t id, std::optional<IgniteError> err);

    /**
     * Handle new message.
     *
     * @param id Async client ID.
     * @param msg Received message.
     */
    void handleMessageReceived(uint64_t id, const DataBuffer& msg);

    /**
     * Handle sent message event.
     *
     * @param id Async client ID.
     */
    void handleMessageSent(uint64_t id);

private:
     /**
     * Close all established connections and stops handling threads.
     */
    void internalStop();

    /**
     * Find client by ID.
     *
     * @param id Client ID.
     * @return Client. Null pointer if is not found.
     */
    [[nodiscard]]
    std::shared_ptr<WinAsyncClient> findClient(uint64_t id) const;

    /**
     * Find client by ID.
     *
     * @warning Should only be called with m_clientsMutex lock held.
     * @param id Client ID.
     * @return Client. Null pointer if is not found.
     */
    [[nodiscard]]
    std::shared_ptr<WinAsyncClient> findClientLocked(uint64_t id) const;

    /** Flag indicating that pool is stopping. */
    volatile bool m_stopping;

    /** Event handler. */
    std::weak_ptr<AsyncHandler> m_asyncHandler;

    /** Connecting thread. */
    WinAsyncConnectingThread m_connectingThread;

    /** Internal thread. */
    WinAsyncWorkerThread m_workerThread;

    /** ID counter. */
    uint64_t m_idGen;

    /** IO Completion Port. Windows-specific primitive for asynchronous IO. */
    HANDLE m_iocp;

    /** Clients critical section. */
    mutable std::mutex m_clientsMutex;

    /** Client mapping ID -> client */
    std::map<uint64_t, std::shared_ptr<WinAsyncClient>> m_clientIdMap;
};

} // namespace ignite::network
