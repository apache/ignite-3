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

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>

#include "common/ignite_error.h"

#include <ignite/network/async_client_pool.h>
#include <ignite/network/async_handler.h>
#include <ignite/network/tcp_range.h>

#include "network/linux_async_client.h"
#include "network/linux_async_worker_thread.h"

namespace ignite::network {

/**
 * Linux-specific implementation of asynchronous client pool.
 */
class LinuxAsyncClientPool : public AsyncClientPool {
public:
    /**
     * Constructor
     *
     * @param handler Upper level event handler.
     */
    LinuxAsyncClientPool();

    /**
     * Destructor.
     */
    ~LinuxAsyncClientPool() override;

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
    void setHandler(std::weak_ptr<AsyncHandler> handler) override { m_asyncHandler = std::move(handler); }

    /**
     * Send data to specific established connection.
     *
     * @param id Client ID.
     * @param data Data to be sent.
     * @return @c true if connection is present and @c false otherwise.
     *
     * @throw IgniteError on error.
     */
    bool send(uint64_t id, std::vector<std::byte> &&data) override;

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
    bool addClient(std::shared_ptr<LinuxAsyncClient> client);

    /**
     * Handle error during connection establishment.
     *
     * @param addr Connection address.
     * @param err Error.
     */
    void handleConnectionError(const EndPoint &addr, IgniteError err);

    /**
     * Handle successful connection establishment.
     *
     * @param addr Address of the new connection.
     * @param id Connection ID.
     */
    void handleConnectionSuccess(const EndPoint &addr, uint64_t id);

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
    void handleMessageReceived(uint64_t id, BytesView msg);

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
    std::shared_ptr<LinuxAsyncClient> findClient(uint64_t id) const;

    /** Flag indicating that pool is stopping. */
    volatile bool m_stopping;

    /** Event handler. */
    std::weak_ptr<AsyncHandler> m_asyncHandler;

    /** Worker thread. */
    LinuxAsyncWorkerThread m_workerThread;

    /** ID counter. */
    uint64_t m_idGen;

    /** Clients critical section. */
    mutable std::mutex m_clientsMutex;

    /** Client mapping ID -> client */
    std::map<uint64_t, std::shared_ptr<LinuxAsyncClient>> m_clientIdMap;
};

} // namespace ignite::network
