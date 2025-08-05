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
#include "ignite/client/detail/transaction/transaction_impl.h"
#include "ignite/client/transaction/transaction_options.h"

#include "ignite/common/detail/config.h"
#include "ignite/common/ignite_result.h"

#include <memory>

namespace ignite::detail {

/**
 * Ignite transactions implementation.
 */
class transactions_impl {
public:
    // Default
    transactions_impl(transactions_impl &&) noexcept = default;
    transactions_impl &operator=(transactions_impl &&) noexcept = default;

    // Deleted
    transactions_impl() = delete;
    transactions_impl(const transactions_impl &) = delete;
    transactions_impl &operator=(const transactions_impl &) = delete;

    /**
     * Constructor.
     *
     * @param connection Connection.
     */
    explicit transactions_impl(std::shared_ptr<cluster_connection> connection)
        : m_connection(std::move(connection)) {}

    /**
     * Starts a new transaction asynchronously.
     *
     * @param callback Callback to be called with a new transaction or error upon completion of asynchronous operation.
     * @param tx_opts Transaction options.
     */
    IGNITE_API void begin_async(transaction_options tx_opts, ignite_callback<transaction> callback) {
        auto writer_func = [this, &tx_opts](protocol::writer &writer, auto) {
            writer.write_bool(tx_opts.is_read_only());
            writer.write(tx_opts.get_timeout_millis());
            writer.write(m_connection->get_observable_timestamp());
        };

        auto reader_func = [](protocol::reader &reader, std::shared_ptr<node_connection> conn) mutable -> transaction {
            auto id = reader.read_int64();

            return transaction(std::make_shared<transaction_impl>(id, std::move(conn)));
        };

        m_connection->perform_request<transaction>(
            protocol::client_operation::TX_BEGIN, std::move(writer_func), std::move(reader_func), std::move(callback));
    }

private:
    /** Cluster connection. */
    std::shared_ptr<cluster_connection> m_connection;
};

} // namespace ignite::detail
