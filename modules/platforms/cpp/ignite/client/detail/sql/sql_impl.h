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

#include "ignite/client/detail/cluster_connection.h"
#include "ignite/client/sql/result_set.h"
#include "ignite/client/sql/sql_statement.h"
#include "ignite/client/transaction/transaction.h"
#include "ignite/common/primitive.h"

#include <memory>
#include <utility>

namespace ignite::detail {

/**
 * Ignite SQL query facade.
 */
class sql_impl {
public:
    // Default
    sql_impl(sql_impl &&) noexcept = default;
    sql_impl &operator=(sql_impl &&) noexcept = default;

    // Deleted
    sql_impl() = delete;
    sql_impl(const sql_impl &) = delete;
    sql_impl &operator=(const sql_impl &) = delete;

    /**
     * Constructor.
     *
     * @param connection Connection.
     */
    explicit sql_impl(std::shared_ptr<cluster_connection> connection)
        : m_connection(std::move(connection)) {}

    /**
     * Executes single SQL statement and returns rows.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param statement statement to execute.
     * @param args Arguments for the statement.
     * @param callback A callback called on operation completion with SQL result set.
     */
    void execute_async(transaction *tx, const sql_statement &statement, std::vector<primitive> &&args,
        ignite_callback<result_set> &&callback);

    /**
     * Executes a multi-statement SQL query asynchronously.
     *
     * @param statement statement to execute.
     * @param args Arguments for the template (can be empty).
     * @param callback A callback called on operation completion with SQL result set.
     */
    void execute_script_async(
        const sql_statement &statement, std::vector<primitive> &&args, ignite_callback<void> &&callback);

private:
    /** Cluster connection. */
    std::shared_ptr<cluster_connection> m_connection;
};

} // namespace ignite::detail
