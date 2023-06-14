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

#include <ignite/client/detail/cluster_connection.h>
#include <ignite/client/detail/compute/compute_impl.h>
#include <ignite/client/detail/sql/sql_impl.h>
#include <ignite/client/detail/table/tables_impl.h>
#include <ignite/client/detail/transaction/transactions_impl.h>
#include <ignite/client/ignite_client_configuration.h>

#include <ignite/common/ignite_result.h>

#include <future>
#include <memory>

namespace ignite::detail {

/**
 * Ignite client implementation.
 */
class ignite_client_impl {
public:
    // Deleted
    ignite_client_impl() = delete;
    ignite_client_impl(ignite_client_impl &&) = delete;
    ignite_client_impl(const ignite_client_impl &) = delete;
    ignite_client_impl &operator=(ignite_client_impl &&) = delete;
    ignite_client_impl &operator=(const ignite_client_impl &) = delete;

    /**
     * Constructor.
     *
     * @param configuration Configuration.
     */
    explicit ignite_client_impl(ignite_client_configuration configuration)
        : m_configuration(std::move(configuration))
        , m_connection(cluster_connection::create(m_configuration))
        , m_tables(std::make_shared<tables_impl>(m_connection))
        , m_sql(std::make_shared<sql_impl>(m_connection))
        , m_compute(std::make_shared<compute_impl>(m_connection, m_tables))
        , m_transactions(std::make_shared<transactions_impl>(m_connection)) {}

    /**
     * Destructor.
     */
    ~ignite_client_impl() { stop(); }

    /**
     * Start client.
     *
     * @param timeout Timeout.
     * @param callback Callback.
     */
    void start(std::function<void(ignite_result<void>)> callback) { m_connection->start_async(std::move(callback)); }

    /**
     * Stop client.
     */
    void stop() { m_connection->stop(); }

    /**
     * Get client configuration.
     *
     * @return Configuration.
     */
    [[nodiscard]] const ignite_client_configuration &configuration() const { return m_configuration; }

    /**
     * Get table management API implementation.
     *
     * @return Table management API implementation.
     */
    [[nodiscard]] std::shared_ptr<tables_impl> get_tables_impl() const { return m_tables; }

    /**
     * Get SQL management API implementation.
     *
     * @return SQL management API implementation.
     */
    [[nodiscard]] std::shared_ptr<sql_impl> get_sql_impl() const { return m_sql; }

    /**
     * Get Compute management API implementation.
     *
     * @return Compute management API implementation.
     */
    [[nodiscard]] std::shared_ptr<compute_impl> get_compute_impl() const { return m_compute; }

    /**
     * Get transactions management API implementation.
     *
     * @return Transactions management API implementation.
     */
    [[nodiscard]] std::shared_ptr<transactions_impl> get_transactions_impl() const { return m_transactions; }

    /**
     * Gets the cluster nodes asynchronously.
     * NOTE: Temporary API to enable Compute until we have proper Cluster API.
     *
     * @param callback Callback called with the list of cluster nodes upon success.
     */
    void get_cluster_nodes_async(ignite_callback<std::vector<cluster_node>> callback);

private:
    /** Configuration. */
    const ignite_client_configuration m_configuration;

    /** Cluster connection. */
    std::shared_ptr<cluster_connection> m_connection;

    /** Tables. */
    std::shared_ptr<tables_impl> m_tables;

    /** SQL. */
    std::shared_ptr<sql_impl> m_sql;

    /** Compute. */
    std::shared_ptr<compute_impl> m_compute;

    /** Transactions. */
    std::shared_ptr<transactions_impl> m_transactions;
};

} // namespace ignite::detail
