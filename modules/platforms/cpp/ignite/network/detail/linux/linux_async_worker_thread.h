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

#include "connecting_context.h"
#include "linux_async_client.h"

#include <ignite/network/async_handler.h>
#include <ignite/network/end_point.h>
#include <ignite/network/tcp_range.h>

#include <cstdint>
#include <ctime>
#include <memory>
#include <mutex>
#include <thread>

namespace ignite::network::detail {

class LinuxAsyncClientPool;

/**
 * Async pool working thread.
 */
class LinuxAsyncWorkerThread {
public:
    /**
     * Default constructor.
     */
    explicit LinuxAsyncWorkerThread(LinuxAsyncClientPool &clientPool);

    /**
     * Destructor.
     */
    ~LinuxAsyncWorkerThread();

    /**
     * Start worker thread.
     *
     * @param limit Connection limit.
     * @param addrs Addresses to connect to.
     */
    void start(size_t limit, std::vector<TcpRange> addrs);

    /**
     * Stop thread.
     */
    void stop();

private:
    /**
     * Run thread.
     */
    void run();

    /**
     * Initiate new connection process if needed.
     */
    void handleNewConnections();

    /**
     * Handle epoll events.
     */
    void handleConnectionEvents();

    /**
     * Handle network error during connection establishment.
     *
     * @param addr End point.
     * @param msg Error message.
     */
    void reportConnectionError(const EndPoint &addr, std::string msg);

    /**
     * Handle failed connection.
     *
     * @param msg Error message.
     */
    void handleConnectionFailed(std::string msg);

    /**
     * Handle network error on established connection.
     *
     * @param client Client instance.
     */
    void handleConnectionClosed(LinuxAsyncClient *client);

    /**
     * Handle successfully established connection.
     *
     * @param client Client instance.
     */
    void handleConnectionSuccess(LinuxAsyncClient *client);

    /**
     * Calculate connection timeout.
     *
     * @return Connection timeout.
     */
    [[nodiscard]] int calculateConnectionTimeout() const;

    /**
     * Check whether new connection should be initiated.
     *
     * @return @c true if new connection should be initiated.
     */
    [[nodiscard]] bool shouldInitiateNewConnection() const;

    /** Client pool. */
    LinuxAsyncClientPool &m_clientPool;

    /** Flag indicating that thread is stopping. */
    volatile bool m_stopping;

    /** Client epoll file descriptor. */
    int m_epoll;

    /** Stop event file descriptor. */
    int m_stopEvent;

    /** Addresses to use for connection establishment. */
    std::vector<TcpRange> m_nonConnected;

    /** Connection which is currently in connecting process. */
    std::unique_ptr<ConnectingContext> m_currentConnection;

    /** Currently connected client. */
    std::shared_ptr<LinuxAsyncClient> m_currentClient;

    /** Failed connection attempts. */
    size_t m_failedAttempts;

    /** Last connection time. */
    timespec m_lastConnectionTime;

    /** Minimal number of addresses. */
    size_t m_minAddrs;

    /** Thread. */
    std::thread m_thread;
};

} // namespace ignite::network::detail
