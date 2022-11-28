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

#include <ignite/client/ignite_client_configuration.h>
#include <ignite/client/table/tables.h>

#include <ignite/common/config.h>
#include <ignite/common/ignite_result.h>

#include <functional>
#include <memory>

namespace ignite {

namespace detail {

class ignite_client_impl;

} // namespace detail

/**
 * Ignite client.
 */
class ignite_client {
public:
    // Default
    ignite_client() = default;
    ~ignite_client() = default;
    ignite_client(ignite_client &&) = default;
    ignite_client &operator=(ignite_client &&) = default;

    // Deleted
    ignite_client(const ignite_client &) = delete;
    ignite_client &operator=(const ignite_client &) = delete;

    /**
     * Start client asynchronously.
     *
     * Client tries to establish connection to every endpoint. First endpoint is
     * selected randomly. After that round-robin is used to determine the next
     * address to establish connection to.
     *
     * System means are used to resolve endpoint IP addresses. If more than one
     * IP address is returned, client attempts to connect to them in random order.
     *
     * Only one connection establishment can be in process at the same time.
     *
     * Client considered connected to a cluster when there is at least one
     * connection to any node of the cluster. Upon this event, future will be set
     * with a usable ignite_client instance.
     *
     * @param configuration Client configuration.
     * @param timeout Operation timeout.
     * @param callback Callback to be called once operation is complete.
     */
    IGNITE_API static void start_async(ignite_client_configuration configuration, std::chrono::milliseconds timeout,
        ignite_callback<ignite_client> callback);

    /**
     * Start client synchronously.
     *
     * @see start_async for details.
     *
     * @param configuration Client configuration.
     * @param timeout Operation timeout.
     * @return ignite_client instance.
     */
    IGNITE_API static ignite_client start(ignite_client_configuration configuration, std::chrono::milliseconds timeout);

    /**
     * Get client configuration.
     *
     * @return Configuration.
     */
    [[nodiscard]] IGNITE_API const ignite_client_configuration &configuration() const noexcept;

    /**
     * Get the table API.
     *
     * @return Table API.
     */
    [[nodiscard]] IGNITE_API tables get_tables() const noexcept;

private:
    /**
     * Constructor
     *
     * @param impl Implementation
     */
    explicit ignite_client(std::shared_ptr<void> impl);

    /**
     * Get implementation reference.
     *
     * @return Implementation reference.
     */
    [[nodiscard]] detail::ignite_client_impl &impl() noexcept;

    /**
     * Get implementation reference.
     *
     * @return Implementation reference.
     */
    [[nodiscard]] const detail::ignite_client_impl &impl() const noexcept;

    /** Implementation. */
    std::shared_ptr<void> m_impl;
};

} // namespace ignite
