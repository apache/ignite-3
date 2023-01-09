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

#include "ignite/common/config.h"
#include "ignite/common/ignite_result.h"

namespace ignite::detail {

/**
 * Ignite transactions.
 */
class transactions_impl {
public:
    // Default
    ~transactions_impl() = default;
    transactions_impl(transactions_impl &&) noexcept = default;
    transactions_impl &operator=(transactions_impl &&) noexcept = default;

    // Deleted
    transactions_impl() = delete;
    transactions_impl(const transactions_impl &) = delete;
    transactions_impl &operator=(const transactions_impl &) = delete;

    /**
     * Starts a new transaction asynchronously.
     *
     * @param callback Callback to be called with a new transaction or error upon completion of asynchronous operation.
     */
    IGNITE_API void begin_async(ignite_callback<transaction> callback) {
        //
    }
private:
};

} // namespace ignite
