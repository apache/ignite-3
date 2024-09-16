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

#include <ignite/network/async_client_pool.h>
#include <ignite/network/data_filter.h>

#include <optional>

namespace ignite::network {

/**
 * Asynchronous client pool adapter.
 */
class async_client_pool_adapter : public async_client_pool {
public:
    /**
     * Constructor.
     *
     * @param filters Filters.
     * @param pool Client pool.
     */
    async_client_pool_adapter(data_filters filters, std::shared_ptr<async_client_pool> pool);

    /**
     * Start internal thread that establishes connections to provided addresses and asynchronously sends and
     * receives messages from them. Function returns either when thread is started and first connection is
     * established or failure happened.
     *
     * @param addrs Addresses to connect to.
     * @param conn_limit Connection upper limit. Zero means limit is disabled.
     *
     * @throw ignite_error on error.
     */
    void start(std::vector<tcp_range> addrs, uint32_t conn_limit) override;

    /**
     * Close all established connections and stops handling threads.
     */
    void stop() override;

    /**
     * Set handler.
     *
     * @param handler Handler to set.
     */
    void set_handler(std::weak_ptr<async_handler> handler) override;

    /**
     * Send data to specific established connection.
     *
     * @param id Client ID.
     * @param data Data to be sent.
     * @return @c true if connection is present and @c false otherwise.
     *
     * @throw ignite_error on error.
     */
    bool send(uint64_t id, std::vector<std::byte> &&data) override;

    /**
     * Closes specified connection if it's established. Connection to the specified address is planned for
     * re-connect. Error is reported to handler.
     *
     * @param id Client ID.
     */
    void close(uint64_t id, std::optional<ignite_error> err) override;

private:
    /** Filters. */
    data_filters m_filters;

    /** Underlying pool. */
    std::shared_ptr<async_client_pool> m_pool;

    /** Lower level data sink. */
    data_sink *m_sink;
};

} // namespace ignite::network
