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

#include "ignite/common/detail/config.h"
#include "ignite/common/ignite_result.h"

#include <atomic>
#include <memory>

namespace ignite::detail {

/**
 * Ignite transaction implementation.
 */
class transaction_impl {
public:
    /** Transaction state. */
    enum class state {
        OPEN,

        COMMITTED,

        ROLLED_BACK
    };

    // Deleted
    transaction_impl() = delete;
    transaction_impl(transaction_impl &&) noexcept = delete;
    transaction_impl(const transaction_impl &) = delete;
    transaction_impl &operator=(transaction_impl &&) noexcept = delete;
    transaction_impl &operator=(const transaction_impl &) = delete;

    /**
     * Constructor.
     *
     * @param id Transaction ID.
     * @param connection Connection.
     */
    explicit transaction_impl(std::int64_t id, std::shared_ptr<node_connection> connection)
        : m_id(id)
        , m_state(state::OPEN)
        , m_connection(std::move(connection)) {}

    /**
     * Destructor.
     *
     * It is important to rollback transaction synchronously on destruction to
     * avoid conflicts if the same data is accessed after transaction is destructed.
     */
    ~transaction_impl() {
        sync<void>([this](const auto& callback) { rollback_async(std::move(callback)); });
    }

    /**
     * Commits the transaction asynchronously.
     *
     * @param callback Callback to be called upon asynchronous operation completion.
     */
    void commit_async(const ignite_callback<void>& callback) {
        if (set_state(state::COMMITTED))
            finish(true, callback);
        else
            callback({});
    }

    /**
     * Rollbacks the transaction asynchronously.
     *
     * @param callback Callback to be called upon asynchronous operation completion.
     */
    void rollback_async(const ignite_callback<void>& callback) {
        if (set_state(state::ROLLED_BACK))
            finish(false, callback);
        else
            callback({});
    }

    /**
     * Get transaction ID.
     *
     * @return Transaction ID.
     */
    [[nodiscard]] std::int64_t get_id() const { return m_id; }

    /**
     * Get connection.
     *
     * @return Connection.
     */
    [[nodiscard]] std::shared_ptr<node_connection> get_connection() const { return m_connection; }

private:
    /**
     * Perform operation.
     *
     * @param commit Flag indicating should transaction be committed or rolled back.
     * @param callback Callback to be called upon asynchronous operation completion.
     */
    void finish(bool commit, const ignite_callback<void> &callback) {
        auto writer_func = [id = m_id](protocol::writer &writer, auto) { writer.write(id); };

        auto req_id = m_connection->perform_request_wr<void>(
            commit ? protocol::client_operation::TX_COMMIT : protocol::client_operation::TX_ROLLBACK, writer_func,
            callback);

        if (!req_id) {
            callback(ignite_error{error::code::CONNECTION, ""
                "Can not perform an operation on transaction as the associated connection is closed."
                " Transaction is rolled back implicitly"});
        }
    }

    /**
     * Set state.
     *
     * @param st State.
     * @return @c true if state changed successfully.
     */
    bool set_state(state st) {
        state old = state::OPEN;
        return m_state.compare_exchange_strong(old, st, std::memory_order_relaxed);
    }

    /** ID. */
    std::int64_t m_id;

    /** State. */
    std::atomic<state> m_state{state::OPEN};

    /** Cluster connection. */
    std::shared_ptr<node_connection> m_connection;
};

} // namespace ignite::detail
