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

#include "ignite/client/table/ignite_tuple.h"
#include "ignite/client/transaction/transaction.h"

#include "ignite/common/config.h"
#include "ignite/common/ignite_result.h"

#include <memory>
#include <utility>
#include <type_traits>

namespace ignite {

class table;

namespace detail {
class record_binary_view_impl;
}

/**
 * Record view interface provides methods to access table records.
 */
template<typename T>
class record_view {
public:
    typedef typename std::decay<T>::type value_type;

    // Deleted
    record_view(const record_view &) = delete;
    record_view &operator=(const record_view &) = delete;

    // Default
    record_view() = default;
    ~record_view() = default;
    record_view(record_view &&) noexcept = default;
    record_view &operator=(record_view &&) noexcept = default;
};

/**
 * Record view interface provides methods to access table records.
 */
template<>
class record_view<ignite_tuple> {
    friend class table;
public:
    typedef ignite_tuple value_type;

    // Deleted
    record_view(const record_view &) = delete;
    record_view &operator=(const record_view &) = delete;

    // Default
    record_view() = default;
    ~record_view() = default;
    record_view(record_view &&) noexcept = default;
    record_view &operator=(record_view &&) noexcept = default;

    /**
     * Gets a record by key asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *  single operation is used.
     * @param key Key.
     * @param callback Callback.
     */
    IGNITE_API void get_async(transaction* tx, const value_type& key, ignite_callback<std::optional<value_type>> callback);

    /**
     * Gets a record by key.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *  single operation is used.
     * @param key Key.
     * @param callback Callback.
     */
    IGNITE_API std::optional<value_type> get(transaction* tx, const value_type& key) {
        return sync<std::optional<value_type>>([this, &tx, &key] (auto callback) {
            get_async(tx, key, std::move(callback));
        });
    }

    /**
     * Inserts a record into the table if does not exist or replaces the existed one.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *  single operation is used.
     * @param record A record to insert into the table. The record cannot be @c nullptr.
     * @param callback Callback.
     */
    IGNITE_API void upsert_async(transaction* tx, const value_type& record, ignite_callback<void> callback);

    /**
     * Inserts a record into the table if does not exist or replaces the existed one.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *  single operation is used.
     * @param record A record to insert into the table. The record cannot be @c nullptr.
     */
    IGNITE_API void upsert(transaction* tx, const value_type& record) {
        sync<void>([this, &tx, &record] (auto callback) {
            upsert_async(tx, record, std::move(callback));
        });
    }

private:
    /**
     * Constructor
     *
     * @param impl Implementation
     */
    explicit record_view(std::shared_ptr<detail::record_binary_view_impl> impl)
        : m_impl(std::move(impl)) { }

    /** Implementation. */
    std::shared_ptr<detail::record_binary_view_impl> m_impl;
};

} // namespace ignite
