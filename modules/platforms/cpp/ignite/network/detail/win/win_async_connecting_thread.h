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

#include "win_async_client.h"

#include <ignite/common/ignite_error.h>
#include <ignite/network/async_client_pool.h>
#include <ignite/network/async_handler.h>
#include <ignite/network/tcp_range.h>

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <thread>

namespace ignite::network::detail {

class win_async_client_pool;

/**
 * Async pool connecting thread.
 */
class win_async_connecting_thread {
    /** Send and receive buffers size. */
    static constexpr size_t BUFFER_SIZE = 0x10000;

public:
    /**
     * Constructor.
     */
    win_async_connecting_thread();

    /**
     * Start thread.
     *
     * @param clientPool Client pool.
     * @param limit Connection limit.
     * @param addrs Addresses.
     */
    void start(win_async_client_pool &clientPool, size_t limit, std::vector<tcp_range> addrs);

    /**
     * Stop thread.
     */
    void stop();

    /**
     * Notify about new address available for connection.
     *
     * @param range Address range.
     */
    void notify_free_address(const tcp_range &range);

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
    std::shared_ptr<win_async_client> try_connect(const tcp_range &range);

    /**
     * Try establish connection to address.
     * @param addr Address.
     * @return Socket.
     */
    static SOCKET try_connect(const end_point &addr);

    /**
     * Get next address.
     *
     * @warning Will block if no addresses are available for connect.
     * @return @c true if a new connection should be established.
     */
    tcp_range get_next_address() const;

    /** Thread. */
    std::thread m_thread;

    /** Client pool. */
    win_async_client_pool *m_client_pool;

    /** Flag to signal that thread is stopping. */
    volatile bool m_stopping;

    /** Failed connection attempts. */
    size_t m_failed_attempts;

    /** Minimal number of addresses. */
    size_t m_min_addrs;

    /** Addresses critical section. */
    mutable std::mutex m_addrs_mutex;

    /** Condition variable, which signalled when new connect is needed. */
    mutable std::condition_variable m_connect_needed;

    /** Addresses to use for connection establishment. */
    std::vector<tcp_range> m_non_connected;

    /** Position seed. */
    mutable size_t m_addr_position_seed;
};

} // namespace ignite::network::detail
