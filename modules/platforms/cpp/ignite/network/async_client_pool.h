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

#include <ignite/network/async_handler.h>
#include <ignite/network/data_sink.h>
#include <ignite/network/tcp_range.h>

#include <cstdint>
#include <memory>
#include <vector>

namespace ignite::network {

/**
 * Asynchronous client pool.
 */
class async_client_pool : public data_sink {
public:
    // Default
    ~async_client_pool() override = default;

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
    virtual void start(std::vector<tcp_range> addrs, uint32_t conn_limit) = 0;

    /**
     * Close all established connections and stops handling threads.
     */
    virtual void stop() = 0;

    /**
     * Set handler.
     *
     * @param handler Handler to set.
     */
    virtual void set_handler(std::weak_ptr<async_handler> handler) = 0;
};

} // namespace ignite::network
