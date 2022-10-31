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

// TODO: remove this class as useless.
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
    void get_async(transaction* tx, const ignite_tuple& key, ignite_callback<std::optional<ignite_tuple>> callback) {
        m_table->get_async(tx, key, std::move(callback));
    }

    /**
     * Gets multiple records by keys asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param keys Keys.
     * @param callback Callback that called on operation completion. Called with
     *   resulting records with all columns filled from the table. The order of
     *   elements is guaranteed to be the same as the order of keys. If a record
     *   does not exist, the resulting element of the corresponding order is
     *   @c std::nullopt.
     */
    void get_all_async(transaction* tx, std::vector<ignite_tuple> keys,
        ignite_callback<std::vector<std::optional<ignite_tuple>>> callback)
    {
        m_table->get_all_async(tx, std::move(keys), std::move(callback));
    }

    /**
     * Inserts a record into the table if does not exist or replaces the existed one.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *  single operation is used.
     * @param record A record to insert into the table. The record cannot be @c nullptr.
     * @param callback Callback.
     */
    void upsert_async(transaction* tx, const ignite_tuple& record, ignite_callback<void> callback) {
        m_table->upsert_async(tx, record, std::move(callback));
    }

    /**
     * Inserts multiple records into the table asynchronously, replacing existing ones.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param records Records to upsert.
     * @param callback Callback that called on operation completion.
     */
    void upsert_all_async(transaction* tx, std::vector<ignite_tuple> records, ignite_callback<void> callback) {
        m_table->upsert_all_async(tx, std::move(records), std::move(callback));
    }

    /**
     * Inserts a record into the table if it does not exist.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param record A record to insert into the table. The record cannot be
     *   @c nullptr.
     * @param callback Callback. Called with a value indicating whether the
     *   record was inserted. Equals @c false if a record with the same key
     *   already exists.
     */
    void insert_async(transaction* tx, const ignite_tuple& record, ignite_callback<bool> callback) {
        m_table->insert_async(tx, record, std::move(callback));
    }

private:
    /** Table. */
    std::shared_ptr<table_impl> m_table;
};

} // namespace ignite
