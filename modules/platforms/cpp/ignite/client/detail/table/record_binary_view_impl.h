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

#include "ignite/client/detail/cluster_connection.h"
#include "ignite/client/detail/table/table_impl.h"

namespace ignite::detail {

/**
 * Record binary view provides methods to access table records.
 */
class record_binary_view_impl {
public:
    // Deleted
    record_binary_view_impl(const record_binary_view_impl &) = delete;
    record_binary_view_impl &operator=(const record_binary_view_impl &) = delete;

    // Default
    ~record_binary_view_impl() = default;
    record_binary_view_impl(record_binary_view_impl &&) noexcept = default;
    record_binary_view_impl &operator=(record_binary_view_impl &&) noexcept = default;

    /**
     * Constructor.
     *
     * @param table Table.
     */
    explicit record_binary_view_impl(std::shared_ptr<table_impl> table)
        : m_table(std::move(table)) { }

    /**
     * Gets a record by key asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *  single operation is used.
     * @param key Key.
     * @param callback Callback.
     */
    void get_async(transaction* tx, const ignite_tuple& key,
        const ignite_callback<std::optional<ignite_tuple>>& callback) {
        // TODO: Implement me.
    }

private:
    /** Table. */
    std::shared_ptr<table_impl> m_table;
};

} // namespace ignite
