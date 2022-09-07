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
#include <thread>
#include <mutex>
#include <condition_variable>

#include "common/ignite_error.h"

#include <ignite/network/async_client_pool.h>
#include <ignite/network/async_handler.h>
#include <ignite/network/tcp_range.h>

#include "network/win_async_client.h"

namespace ignite::network
{

class WinAsyncClientPool;

/**
 * Async pool connecting thread.
 */
class WinAsyncConnectingThread
{
    /** Send and receive buffers size. */
    static constexpr size_t BUFFER_SIZE = 0x10000;

public:
    /**
     * Constructor.
     */
    WinAsyncConnectingThread();

    /**
     * Start thread.
     *
     * @param clientPool Client pool.
     * @param limit Connection limit.
     * @param addrs Addresses.
     */
    void start(WinAsyncClientPool& clientPool, size_t limit, std::vector<TcpRange> addrs);

    /**
     * Stop thread.
     */
    void stop();

    /**
     * Notify about new address available for connection.
     *
     * @param range Address range.
     */
    void notifyFreeAddress(const TcpRange &range);

private:
    /**
     * Run thread.
     */
    void run();

    /**
     * Try establish connection to address in the range.
     * @param range TCP range.
     * @return New client.
     */
    static std::shared_ptr<WinAsyncClient> tryConnect(const TcpRange& range);

    /**
     * Try establish connection to address.
     * @param addr Address.
     * @return Socket.
     */
    static SOCKET tryConnect(const EndPoint& addr);

    /**
     * Get random address.
     *
     * @warning Will block if no addresses are available for connect.
     * @return @c true if a new connection should be established.
     */
    TcpRange getRandomAddress() const;

    /** Thread. */
    std::thread m_thread;

    /** Client pool. */
    WinAsyncClientPool* m_clientPool;

    /** Flag to signal that thread is stopping. */
    volatile bool m_stopping;

    /** Failed connection attempts. */
    size_t m_failedAttempts;

    /** Minimal number of addresses. */
    size_t m_minAddrs;

    /** Addresses critical section. */
    mutable std::mutex m_addrsMutex;

    /** Condition variable, which signalled when new connect is needed. */
    mutable std::condition_variable m_connectNeeded;

    /** Addresses to use for connection establishment. */
    std::vector<TcpRange> m_nonConnected;
};
}
