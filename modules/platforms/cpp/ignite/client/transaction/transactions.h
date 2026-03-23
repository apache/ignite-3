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

#include "ignite/client/transaction/transaction.h"
#include "ignite/client/transaction/transaction_options.h"

#include "ignite/common/detail/config.h"
#include "ignite/common/ignite_result.h"

#include <memory>

namespace ignite {

namespace detail {
class transactions_impl;
}

/**
 * @brief Ignite Transactions API facade.
 */
class transactions {
    friend class ignite_client;

public:
    // Delete
    transactions() = delete;

    /**
     * Starts a new transaction.
     *
     * @param tx_opts Transaction options.
     * @return A new transaction.
     */
    IGNITE_API transaction begin(transaction_options tx_opts) {
        return sync<transaction>([this, &tx_opts](auto callback) { begin_async(tx_opts, std::move(callback)); });
    }

    /**
     * Starts a new transaction.
     *
     * @return A new transaction.
     */
    IGNITE_API transaction begin() {
        return begin({});
    }

    /**
     * Starts a new transaction asynchronously.
     *
     * @param tx_opts Transaction options.
     * @param callback Callback to be called with a new transaction or error upon completion of asynchronous operation.
     */
    IGNITE_API void begin_async(transaction_options tx_opts, ignite_callback<transaction> callback);

    /**
     * Starts a new transaction asynchronously.
     *
     * @param callback Callback to be called with a new transaction or error upon completion of asynchronous operation.
     */
    IGNITE_API void begin_async(ignite_callback<transaction> callback);

private:
    /**
     * Constructor
     *
     * @param impl Implementation
     */
    explicit transactions(std::shared_ptr<detail::transactions_impl> impl)
        : m_impl(std::move(impl)) {}

    /** Implementation. */
    std::shared_ptr<detail::transactions_impl> m_impl;
};

} // namespace ignite
