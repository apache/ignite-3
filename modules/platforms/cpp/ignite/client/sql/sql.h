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

#include "ignite/client/sql/result_set.h"
#include "ignite/client/sql/sql_statement.h"
#include "ignite/client/transaction/transaction.h"
#include "ignite/common/config.h"
#include "ignite/common/ignite_result.h"
#include "ignite/common/primitive.h"

#include <memory>
#include <utility>

namespace ignite {

namespace detail {
class sql_impl;
}

/**
 * Ignite SQL query facade.
 */
class sql {
    friend class ignite_client;

public:
    // Delete
    sql() = delete;

    /**
     * Executes single SQL statement asynchronously and returns rows.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this single operation is used.
     * @param statement Statement to execute.
     * @param args Arguments for the statement (can be empty).
     * @param callback A callback called on operation completion with SQL result set.
     */
    IGNITE_API void execute_async(transaction *tx, const sql_statement &statement, std::vector<primitive> args,
        ignite_callback<result_set> callback);

    /**
     * Executes single SQL statement and returns rows.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this single operation is used.
     * @param statement Statement to execute.
     * @param args Arguments for the statement (can be empty).
     * @return SQL result set.
     */
    IGNITE_API result_set execute(transaction *tx, const sql_statement &statement, std::vector<primitive> args) {
        return sync<result_set>([this, tx, &statement, args = std::move(args)](auto callback) mutable {
            execute_async(tx, statement, std::move(args), std::move(callback));
        });
    }

    /**
     * Executes a multi-statement SQL query asynchronously.
     *
     * @param statement Statement to execute.
     * @param args Arguments for the template (can be empty).
     * @param callback A callback called on operation completion with SQL result set.
     */
    IGNITE_API void execute_script_async(
        const sql_statement &statement, std::vector<primitive> args, ignite_callback<void> callback);

    /**
     * Executes a multi-statement SQL query.
     *
     * @param statement Statement to execute.
     * @param args Arguments for the template (can be empty).
     */
    IGNITE_API void execute_script(const sql_statement &statement, std::vector<primitive> args) {
        sync<void>([this, &statement, args = std::move(args)](auto callback) mutable {
            execute_script_async(statement, std::move(args), std::move(callback));
        });
    }

private:
    /**
     * Constructor
     *
     * @param impl Implementation
     */
    explicit sql(std::shared_ptr<detail::sql_impl> impl)
        : m_impl(std::move(impl)) {}

    /** Implementation. */
    std::shared_ptr<detail::sql_impl> m_impl;
};

} // namespace ignite
