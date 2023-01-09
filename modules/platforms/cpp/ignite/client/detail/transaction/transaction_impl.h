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

#include "ignite/client/detail/node_connection.h"

#include "ignite/common/config.h"
#include "ignite/common/ignite_result.h"

#include <memory>

namespace ignite::detail {

/**
 * Ignite transaction implementation.
 */
class transaction_impl {
public:
    // Default
    ~transaction_impl() = default;
    transaction_impl(transaction_impl &&) noexcept = default;
    transaction_impl &operator=(transaction_impl &&) noexcept = default;

    // Deleted
    transaction_impl() = delete;
    transaction_impl(const transaction_impl &) = delete;
    transaction_impl &operator=(const transaction_impl &) = delete;

    /**
     * Constructor.
     *
     * @param id Transaction ID.
     * @param connection Connection.
     */
    explicit transaction_impl(std::int64_t id, std::shared_ptr<node_connection> connection)
        : m_id(id)
        , m_connection(std::move(connection)) {}

    /**
     * Commits the transaction asynchronously.
     *
     * @param callback Callback to be called upon asynchronous operation completion.
     */
    IGNITE_API void commit_async(ignite_callback<void> callback) {
        (void) callback;
        throw ignite_error("Not implemented");
    }

    /**
     * Rollbacks the transaction asynchronously.
     *
     * @param callback Callback to be called upon asynchronous operation completion.
     */
    IGNITE_API void rollback_async(ignite_callback<void> callback) {
        (void) callback;
        throw ignite_error("Not implemented");
    }

private:
    /** ID. */
    std::int64_t m_id;

    /** Cluster connection. */
    std::shared_ptr<node_connection> m_connection;
};

} // namespace ignite
