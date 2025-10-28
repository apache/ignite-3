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

#include "ignite/client/compute/compute.h"
#include "ignite/client/ignite_client_configuration.h"
#include "ignite/client/network/cluster_node.h"
#include "ignite/client/sql/sql.h"
#include "ignite/client/table/tables.h"
#include "ignite/client/transaction/transactions.h"

#include "ignite/common/detail/config.h"
#include "ignite/common/ignite_result.h"

#include <chrono>
#include <functional>
#include <memory>

namespace ignite {

namespace detail {

class ignite_client_impl;

} // namespace detail

/**
 * @brief Ignite client.
 */
class ignite_client {
public:
    // Default
    ignite_client() = default;
    ignite_client(ignite_client &&) = default;
    ignite_client &operator=(ignite_client &&) = default;

    // Deleted
    ignite_client(const ignite_client &) = delete;
    ignite_client &operator=(const ignite_client &) = delete;

    /**
     * Starts a client asynchronously.
     *
     * Client tries to establish connection to every endpoint. At first, an endpoint is selected randomly. After that,
     * round-robin is used to determine the next address to establish connection to.
     *
     * System means are used to resolve endpoint IP addresses. If more than one IP address is returned, the client
     * attempts to connect to them in random order.
     *
     * Only one connection establishment can be in process at the same time.
     *
     * Client considered connected to a cluster when there is at least one connection to any node of the cluster.
     * Upon this event, a callback will be called with a usable ignite_client instance.
     *
     * @param configuration Client configuration.
     * @param timeout Operation timeout.
     * @param callback Callback to be called once the operation is complete.
     */
    IGNITE_API static void start_async(ignite_client_configuration configuration, std::chrono::milliseconds timeout,
        ignite_callback<ignite_client> callback);

    /**
     * Starts a client synchronously.
     *
     * @see start_async for details.
     *
     * @param configuration Client configuration.
     * @param timeout Operation timeout.
     * @return ignite_client instance.
     */
    [[nodiscard]] IGNITE_API static ignite_client start(
        ignite_client_configuration configuration, std::chrono::milliseconds timeout);

    /**
     * Gets client configuration.
     *
     * @return Configuration.
     */
    [[nodiscard]] IGNITE_API const ignite_client_configuration &configuration() const noexcept;

    /**
     * Gets the table API.
     *
     * @return Table API.
     */
    [[nodiscard]] IGNITE_API tables get_tables() const noexcept;

    /**
     * Gets the SQL API.
     *
     * @return SQL API.
     */
    [[nodiscard]] IGNITE_API sql get_sql() const noexcept;

    /**
     * Gets the Compute API.
     *
     * @return Compute API.
     */
    [[nodiscard]] IGNITE_API compute get_compute() const noexcept;

    /**
     * Gets the Transactions API.
     *
     * @return Transactions API.
     */
    [[nodiscard]] IGNITE_API transactions get_transactions() const noexcept;

    /**
     * Gets the cluster nodes asynchronously.
     * NOTE: Temporary API to enable Compute until we have proper Cluster API.
     *
     * @param callback Callback called with the list of cluster nodes upon success.
     */
    IGNITE_API void get_cluster_nodes_async(ignite_callback<std::vector<cluster_node>> callback);

    /**
     * Gets the cluster nodes.
     * NOTE: Temporary API to enable Compute until we have proper Cluster API.
     *
     * @return The list of cluster nodes upon success.
     */
    [[nodiscard]] IGNITE_API std::vector<cluster_node> get_cluster_nodes();

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
