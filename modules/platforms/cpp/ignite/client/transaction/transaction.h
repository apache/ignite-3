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

#include "ignite/common/detail/config.h"
#include "ignite/common/ignite_result.h"

namespace ignite {

namespace detail {
class sql_impl;
class table_impl;
class transaction_impl;
class transactions_impl;
}

/**
 * @brief Ignite transaction.
 */
class transaction {
    friend class detail::sql_impl;
    friend class detail::table_impl;
    friend class detail::transactions_impl;

public:
    // Default
    transaction() = default;

    /**
     * Commits the transaction.
     */
    IGNITE_API void commit() {
        return sync<void>([this](const auto& callback) { commit_async(std::move(callback)); });
    }

    /**
     * Commits the transaction asynchronously.
     *
     * @param callback Callback to be called upon asynchronous operation completion.
     */
    IGNITE_API void commit_async(const ignite_callback<void> &callback);

    /**
     * Rollbacks the transaction.
     */
    IGNITE_API void rollback() {
        sync<void>([this](const auto& callback) { rollback_async(std::move(callback)); });
    }

    /**
     * Rollbacks the transaction asynchronously.
     *
     * @param callback Callback to be called upon asynchronous operation completion.
     */
    IGNITE_API void rollback_async(const ignite_callback<void> &callback);

private:
    /**
     * Constructor
     *
     * @param impl Implementation
     */
    explicit transaction(std::shared_ptr<detail::transaction_impl> impl)
        : m_impl(std::move(impl)) {}

    /** Implementation. */
    std::shared_ptr<detail::transaction_impl> m_impl;
};

} // namespace ignite
