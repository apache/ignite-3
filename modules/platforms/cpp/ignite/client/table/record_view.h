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

#include <ignite/client/detail/type_mapping_utils.h>
#include <ignite/client/table/ignite_tuple.h>
#include <ignite/client/transaction/transaction.h>
#include <ignite/client/type_mapping.h>

#include "ignite/common/detail/config.h"
#include <ignite/common/ignite_result.h>

#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

namespace ignite {

class table;

namespace detail {
class table_impl;
}

template<typename T>
class record_view;

/**
 * @brief Record view interface for table.
 *
 * Provides methods to access table records.
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
    record_view(record_view &&) noexcept = default;
    record_view &operator=(record_view &&) noexcept = default;

    /**
     * Gets a record by key asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param callback Callback which is called on success with value if it
     *   exists and @c std::nullopt otherwise
     */
    IGNITE_API void get_async(
        transaction *tx, const value_type &key, ignite_callback<std::optional<value_type>> callback);

    /**
     * Gets a record by key.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @return Value if exists and @c std::nullopt otherwise.
     */
    [[nodiscard]] IGNITE_API std::optional<value_type> get(transaction *tx, const value_type &key) {
        return sync<std::optional<value_type>>(
            [this, tx, &key](auto callback) { get_async(tx, key, std::move(callback)); });
    }

    /**
     * Gets multiple records by keys asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param keys Keys.
     * @param callback Callback that is called on operation completion. Called with
     *   resulting records with all columns filled from the table. The order of
     *   elements is guaranteed to be the same as the order of keys. If a record
     *   does not exist, the resulting element of the corresponding order is
     *   @c std::nullopt.
     */
    IGNITE_API void get_all_async(transaction *tx, std::vector<value_type> keys,
        ignite_callback<std::vector<std::optional<value_type>>> callback);

    /**
     * Gets multiple records by keys.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param keys Keys.
     * @return Resulting records with all columns filled from the table.
     *   The order of elements is guaranteed to be the same as the order of
     *   keys. If a record does not exist, the resulting element of the
     *   corresponding order is @c std::nullopt.
     */
    [[nodiscard]] IGNITE_API std::vector<std::optional<value_type>> get_all(
        transaction *tx, std::vector<value_type> keys) {
        return sync<std::vector<std::optional<value_type>>>([this, tx, keys = std::move(keys)](auto callback) mutable {
            get_all_async(tx, std::move(keys), std::move(callback));
        });
    }

    /**
     * Inserts a record into the table if does not exist or replaces the existing one.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *  single operation is used.
     * @param record A record to insert into the table. The record cannot be @c nullptr.
     * @param callback Callback.
     */
    IGNITE_API void upsert_async(transaction *tx, const value_type &record, ignite_callback<void> callback);

    /**
     * Inserts a record into the table if does not exist or replaces the existing one.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *  single operation is used.
     * @param record A record to insert into the table. The record cannot be @c nullptr.
     */
    IGNITE_API void upsert(transaction *tx, const value_type &record) {
        sync<void>([this, tx, &record](auto callback) { upsert_async(tx, record, std::move(callback)); });
    }

    /**
     * Inserts multiple records into the table asynchronously, replacing
     * existing.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param records Records to upsert.
     * @param callback Callback that is called on operation completion.
     */
    IGNITE_API void upsert_all_async(transaction *tx, std::vector<value_type> records, ignite_callback<void> callback);

    /**
     * Inserts multiple records into the table, replacing existing.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param records Records to upsert.
     */
    IGNITE_API void upsert_all(transaction *tx, std::vector<value_type> records) {
        sync<void>([this, tx, records = std::move(records)](
                       auto callback) mutable { upsert_all_async(tx, std::move(records), std::move(callback)); });
    }

    /**
     * Inserts a record into the table and returns previous record asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param record A record to upsert.
     * @param callback Callback. Called with a value which contains replaced
     *   record or @c std::nullopt if it did not exist.
     */
    IGNITE_API void get_and_upsert_async(
        transaction *tx, const value_type &record, ignite_callback<std::optional<value_type>> callback);

    /**
     * Inserts a record into the table and returns previous record.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param record A record to upsert.
     * @return A replaced record or @c std::nullopt if it did not exist.
     */
    [[nodiscard]] IGNITE_API std::optional<value_type> get_and_upsert(transaction *tx, const value_type &record) {
        return sync<std::optional<value_type>>(
            [this, tx, &record](auto callback) { get_and_upsert_async(tx, record, std::move(callback)); });
    }

    /**
     * Inserts a record into the table if it does not exist asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param record A record to insert into the table.
     * @param callback Callback. Called with a value indicating whether the
     *   record was inserted. Equals @c false if a record with the same key
     *   already exists.
     */
    IGNITE_API void insert_async(transaction *tx, const value_type &record, ignite_callback<bool> callback);

    /**
     * Inserts a record into the table if does not exist.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param record A record to insert into the table.
     */
    IGNITE_API bool insert(transaction *tx, const value_type &record) {
        return sync<bool>([this, tx, &record](auto callback) { insert_async(tx, record, std::move(callback)); });
    }

    /**
     * Inserts multiple records into the table asynchronously, skipping existing ones.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param records Records to insert.
     * @param callback Callback that is called on operation completion. Called with
     *   skipped records.
     */
    IGNITE_API void insert_all_async(
        transaction *tx, std::vector<value_type> records, ignite_callback<std::vector<value_type>> callback);

    /**
     * Inserts multiple records into the table, skipping existing ones.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param records Records to insert.
     * @return Skipped records.
     */
    IGNITE_API std::vector<value_type> insert_all(transaction *tx, std::vector<value_type> records) {
        return sync<std::vector<value_type>>([this, tx, records = std::move(records)](auto callback) mutable {
            insert_all_async(tx, std::move(records), std::move(callback));
        });
    }

    /**
     * Asynchronously replaces a record with the same key columns if it exists,
     * otherwise does nothing.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param record A record to insert into the table.
     * @param callback Callback. Called with a value indicating whether a record
     *   with the specified key was replaced.
     */
    IGNITE_API void replace_async(transaction *tx, const value_type &record, ignite_callback<bool> callback);

    /**
     * Replaces a record with the same key columns if it exists, otherwise does
     * nothing.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param record A record to insert into the table.
     * @return A value indicating whether a record with the specified key was
     *   replaced.
     */
    IGNITE_API bool replace(transaction *tx, const value_type &record) {
        return sync<bool>([this, tx, &record](auto callback) { replace_async(tx, record, std::move(callback)); });
    }

    /**
     * Asynchronously replaces a record with a new one only if all existing
     * columns have the same values as the specified @c record.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param record Current value of the record to be replaced.
     * @param new_record A record to replace it with.
     * @param callback Callback. Called with a value indicating whether a
     *   specified record was replaced.
     */
    IGNITE_API void replace_async(
        transaction *tx, const value_type &record, const value_type &new_record, ignite_callback<bool> callback);

    /**
     * Replaces a record with a new one only if all existing columns have
     * the same values as the specified @c record.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param record Current value of the record to be replaced.
     * @param new_record A record to replace it with.
     * @return A value indicating whether a specified record was replaced.
     */
    IGNITE_API bool replace(transaction *tx, const value_type &record, const value_type &new_record) {
        return sync<bool>([this, tx, &record, &new_record](
                              auto callback) { replace_async(tx, record, new_record, std::move(callback)); });
    }

    /**
     * Asynchronously replaces a record with the same key columns if it exists
     * returning previous record value.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param record A record to insert.
     * @param callback Callback. Called with a previous value for the given key,
     *   or @c std::nullopt if it did not exist.
     */
    IGNITE_API void get_and_replace_async(
        transaction *tx, const value_type &record, ignite_callback<std::optional<value_type>> callback);

    /**
     * Replaces a record with the same key columns if it exists returning
     * previous record value.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param record A record to insert.
     * @param callback A previous value for the given key, or @c std::nullopt if
     *   it did not exist.
     */
    [[nodiscard]] IGNITE_API std::optional<value_type> get_and_replace(transaction *tx, const value_type &record) {
        return sync<std::optional<value_type>>(
            [this, tx, &record](auto callback) { get_and_replace_async(tx, record, std::move(callback)); });
    }

    /**
     * Deletes a record with the specified key asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key A record with key columns set.
     * @param callback Callback that is called on operation completion. Called with
     *   a value indicating whether a record with the specified key was deleted.
     */
    IGNITE_API void remove_async(transaction *tx, const value_type &key, ignite_callback<bool> callback);

    /**
     * Deletes a record with the specified key.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key A record with key columns set.
     * @return A value indicating whether a record with the specified key was deleted.
     */
    IGNITE_API bool remove(transaction *tx, const value_type &record) {
        return sync<bool>([this, tx, &record](auto callback) { remove_async(tx, record, std::move(callback)); });
    }

    /**
     * Deletes a record only if all existing columns have the same values as
     * the specified record asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param record A record with all columns set.
     * @param callback Callback that is called on operation completion. Called with
     *   a value indicating whether a record with the specified key was deleted.
     */
    IGNITE_API void remove_exact_async(transaction *tx, const value_type &record, ignite_callback<bool> callback);

    /**
     * Deletes a record only if all existing columns have the same values as
     * the specified record.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param record A record with all columns set.
     * @return A value indicating whether a record with the specified key was
     *   deleted.
     */
    IGNITE_API bool remove_exact(transaction *tx, const value_type &record) {
        return sync<bool>([this, tx, &record](auto callback) { remove_exact_async(tx, record, std::move(callback)); });
    }

    /**
     * Gets and deletes a record with the specified key asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key A record with key columns set.
     * @param callback Callback that is called on operation completion. Called with
     *   a deleted record or @c std::nullopt if it did not exist.
     */
    IGNITE_API void get_and_remove_async(
        transaction *tx, const value_type &key, ignite_callback<std::optional<value_type>> callback);

    /**
     * Gets and deletes a record with the specified key.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key A record with key columns set.
     * @return A deleted record or @c std::nullopt if it did not exist.
     */
    IGNITE_API std::optional<value_type> get_and_remove(transaction *tx, const value_type &key) {
        return sync<std::optional<value_type>>(
            [this, tx, &key](auto callback) { get_and_remove_async(tx, key, std::move(callback)); });
    }

    /**
     * Deletes multiple records from the table asynchronously. If one or more
     * keys do not exist, other records are still deleted
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param keys Record keys to delete.
     * @param callback Callback that is called on operation completion. Called with
     *   records from @c keys that did not exist.
     */
    IGNITE_API void remove_all_async(
        transaction *tx, std::vector<value_type> keys, ignite_callback<std::vector<value_type>> callback);

    /**
     * Deletes multiple records from the table If one or more keys do not exist,
     * other records are still deleted
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param keys Record keys to delete.
     * @return Records from @c keys that did not exist.
     */
    IGNITE_API std::vector<value_type> remove_all(transaction *tx, std::vector<value_type> keys) {
        return sync<std::vector<value_type>>([this, tx, keys = std::move(keys)](auto callback) mutable {
            remove_all_async(tx, std::move(keys), std::move(callback));
        });
    }

    /**
     * Deletes multiple exactly matching records asynchronously. If one or more
     * records do not exist, other records are still deleted.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param records Records to delete.
     * @param callback Callback that is called on operation completion. Called with
     *   records from @c records that did not exist.
     */
    IGNITE_API void remove_all_exact_async(
        transaction *tx, std::vector<value_type> records, ignite_callback<std::vector<value_type>> callback);

    /**
     * Deletes multiple exactly matching records. If one or more records do not
     * exist, other records are still deleted.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param records Records to delete.
     * @return Records from @c records that did not exist.
     */
    IGNITE_API std::vector<value_type> remove_all_exact(transaction *tx, std::vector<value_type> records) {
        return sync<std::vector<value_type>>([this, tx, records = std::move(records)](auto callback) mutable {
            remove_all_exact_async(tx, std::move(records), std::move(callback));
        });
    }

private:
    /**
     * Constructor
     *
     * @param impl Implementation
     */
    explicit record_view(std::shared_ptr<detail::table_impl> impl)
        : m_impl(std::move(impl)) {}

    /** Implementation. */
    std::shared_ptr<detail::table_impl> m_impl;
};

/**
 * @brief Record view interface for table.
 *
 * Provides methods to access table records.
 */
template<typename T>
class record_view {
    friend class table;

public:
    typedef typename std::decay<T>::type value_type;

    // Deleted
    record_view(const record_view &) = delete;
    record_view &operator=(const record_view &) = delete;

    // Default
    record_view() = default;
    record_view(record_view &&) noexcept = default;
    record_view &operator=(record_view &&) noexcept = default;

    /**
     * Gets a record by key asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param callback Callback which is called on success with value if it
     *   exists and @c std::nullopt otherwise
     */
    void get_async(transaction *tx, const value_type &key, ignite_callback<std::optional<value_type>> callback) {
        m_delegate.get_async(tx, convert_to_tuple(key),
            [callback = std::move(callback)](auto res) { callback(convert_result<value_type>(std::move(res))); });
    }

    /**
     * Gets a record by key.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @return Value if exists and @c std::nullopt otherwise.
     */
    [[nodiscard]] std::optional<value_type> get(transaction *tx, const value_type &key) {
        return sync<std::optional<value_type>>(
            [this, tx, &key](auto callback) { get_async(tx, key, std::move(callback)); });
    }

    /**
     * Gets multiple records by keys asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param keys Keys.
     * @param callback Callback that is called on operation completion. Called with
     *   resulting records with all columns filled from the table. The order of
     *   elements is guaranteed to be the same as the order of keys. If a record
     *   does not exist, the resulting element of the corresponding order is
     *   @c std::nullopt.
     */
    void get_all_async(transaction *tx, std::vector<value_type> keys,
        ignite_callback<std::vector<std::optional<value_type>>> callback) {
        m_delegate.get_all_async(tx, values_to_tuples<value_type>(std::move(keys)),
            [callback = std::move(callback)](auto res) { callback(convert_result<value_type>(std::move(res))); });
    }

    /**
     * Gets multiple records by keys.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param keys Keys.
     * @return Resulting records with all columns filled from the table.
     *   The order of elements is guaranteed to be the same as the order of
     *   keys. If a record does not exist, the resulting element of the
     *   corresponding order is @c std::nullopt.
     */
    [[nodiscard]] std::vector<std::optional<value_type>> get_all(transaction *tx, std::vector<value_type> keys) {
        return sync<std::vector<std::optional<value_type>>>([this, tx, keys = std::move(keys)](auto callback) mutable {
            get_all_async(tx, std::move(keys), std::move(callback));
        });
    }

    /**
     * Inserts a record into the table if does not exist or replaces the existing one.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *  single operation is used.
     * @param record A record to insert into the table. The record cannot be @c nullptr.
     * @param callback Callback.
     */
    void upsert_async(transaction *tx, const value_type &record, ignite_callback<void> callback) {
        m_delegate.upsert_async(tx, convert_to_tuple(record), std::move(callback));
    }

    /**
     * Inserts a record into the table if does not exist or replaces the existing one.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *  single operation is used.
     * @param record A record to insert into the table. The record cannot be @c nullptr.
     */
    void upsert(transaction *tx, const value_type &record) {
        sync<void>([this, tx, &record](auto callback) { upsert_async(tx, record, std::move(callback)); });
    }

    /**
     * Inserts multiple records into the table asynchronously, replacing
     * existing.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param records Records to upsert.
     * @param callback Callback that is called on operation completion.
     */
    void upsert_all_async(transaction *tx, std::vector<value_type> records, ignite_callback<void> callback) {
        m_delegate.upsert_all_async(tx, values_to_tuples<value_type>(std::move(records)), std::move(callback));
    }

    /**
     * Inserts multiple records into the table, replacing existing.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param records Records to upsert.
     */
    void upsert_all(transaction *tx, std::vector<value_type> records) {
        sync<void>([this, tx, records = std::move(records)](
                       auto callback) mutable { upsert_all_async(tx, std::move(records), std::move(callback)); });
    }

    /**
     * Inserts a record into the table and returns previous record asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param record A record to upsert.
     * @param callback Callback. Called with a value which contains replaced
     *   record or @c std::nullopt if it did not exist.
     */
    void get_and_upsert_async(
        transaction *tx, const value_type &record, ignite_callback<std::optional<value_type>> callback) {
        m_delegate.get_and_upsert_async(tx, convert_to_tuple(record),
            [callback = std::move(callback)](auto res) { callback(convert_result<value_type>(std::move(res))); });
    }

    /**
     * Inserts a record into the table and returns previous record.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param record A record to upsert.
     * @return A replaced record or @c std::nullopt if it did not exist.
     */
    [[nodiscard]] std::optional<value_type> get_and_upsert(transaction *tx, const value_type &record) {
        return sync<std::optional<value_type>>(
            [this, tx, &record](auto callback) { get_and_upsert_async(tx, record, std::move(callback)); });
    }

    /**
     * Inserts a record into the table if it does not exist asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param record A record to insert into the table.
     * @param callback Callback. Called with a value indicating whether the
     *   record was inserted. Equals @c false if a record with the same key
     *   already exists.
     */
    void insert_async(transaction *tx, const value_type &record, ignite_callback<bool> callback) {
        m_delegate.insert_async(tx, convert_to_tuple(record), std::move(callback));
    }

    /**
     * Inserts a record into the table if does not exist.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param record A record to insert into the table.
     */
    bool insert(transaction *tx, const value_type &record) {
        return sync<bool>([this, tx, &record](auto callback) { insert_async(tx, record, std::move(callback)); });
    }

    /**
     * Inserts multiple records into the table asynchronously, skipping existing ones.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param records Records to insert.
     * @param callback Callback that is called on operation completion. Called with
     *   skipped records.
     */
    void insert_all_async(
        transaction *tx, std::vector<value_type> records, ignite_callback<std::vector<value_type>> callback) {
        m_delegate.insert_all_async(tx, values_to_tuples<value_type>(std::move(records)),
            [callback = std::move(callback)](auto res) { callback(convert_result<value_type>(std::move(res))); });
    }

    /**
     * Inserts multiple records into the table, skipping existing ones.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param records Records to insert.
     * @return Skipped records.
     */
    std::vector<value_type> insert_all(transaction *tx, std::vector<value_type> records) {
        return sync<std::vector<value_type>>([this, tx, records = std::move(records)](auto callback) mutable {
            insert_all_async(tx, std::move(records), std::move(callback));
        });
    }

    /**
     * Asynchronously replaces a record with the same key columns if it exists,
     * otherwise does nothing.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param record A record to insert into the table.
     * @param callback Callback. Called with a value indicating whether a record
     *   with the specified key was replaced.
     */
    void replace_async(transaction *tx, const value_type &record, ignite_callback<bool> callback) {
        m_delegate.replace_async(tx, convert_to_tuple(record), std::move(callback));
    }

    /**
     * Replaces a record with the same key columns if it exists, otherwise does
     * nothing.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param record A record to insert into the table.
     * @return A value indicating whether a record with the specified key was
     *   replaced.
     */
    bool replace(transaction *tx, const value_type &record) {
        return sync<bool>([this, tx, &record](auto callback) { replace_async(tx, record, std::move(callback)); });
    }

    /**
     * Asynchronously replaces a record with a new one only if all existing
     * columns have the same values as the specified @c record.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param record Current value of the record to be replaced.
     * @param new_record A record to replace it with.
     * @param callback Callback. Called with a value indicating whether a
     *   specified record was replaced.
     */
    void replace_async(
        transaction *tx, const value_type &record, const value_type &new_record, ignite_callback<bool> callback) {
        m_delegate.replace_async(tx, convert_to_tuple(record), convert_to_tuple(new_record), std::move(callback));
    }

    /**
     * Replaces a record with a new one only if all existing columns have
     * the same values as the specified @c record.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param record Current value of the record to be replaced.
     * @param new_record A record to replace it with.
     * @return A value indicating whether a specified record was replaced.
     */
    bool replace(transaction *tx, const value_type &record, const value_type &new_record) {
        return sync<bool>([this, tx, &record, &new_record](
                              auto callback) { replace_async(tx, record, new_record, std::move(callback)); });
    }

    /**
     * Asynchronously replaces a record with the same key columns if it exists
     * returning previous record value.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param record A record to insert.
     * @param callback Callback. Called with a previous value for the given key,
     *   or @c std::nullopt if it did not exist.
     */
    void get_and_replace_async(
        transaction *tx, const value_type &record, ignite_callback<std::optional<value_type>> callback) {
        m_delegate.get_and_replace_async(tx, convert_to_tuple(record),
            [callback = std::move(callback)](auto res) { callback(convert_result<value_type>(std::move(res))); });
    }

    /**
     * Replaces a record with the same key columns if it exists returning
     * previous record value.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param record A record to insert.
     * @param callback A previous value for the given key, or @c std::nullopt if
     *   it did not exist.
     */
    [[nodiscard]] std::optional<value_type> get_and_replace(transaction *tx, const value_type &record) {
        return sync<std::optional<value_type>>(
            [this, tx, &record](auto callback) { get_and_replace_async(tx, record, std::move(callback)); });
    }

    /**
     * Deletes a record with the specified key asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key A record with key columns set.
     * @param callback Callback that is called on operation completion. Called with
     *   a value indicating whether a record with the specified key was deleted.
     */
    void remove_async(transaction *tx, const value_type &key, ignite_callback<bool> callback) {
        m_delegate.remove_async(tx, convert_to_tuple(key), std::move(callback));
    }

    /**
     * Deletes a record with the specified key.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key A record with key columns set.
     * @return A value indicating whether a record with the specified key was deleted.
     */
    bool remove(transaction *tx, const value_type &record) {
        return sync<bool>([this, tx, &record](auto callback) { remove_async(tx, record, std::move(callback)); });
    }

    /**
     * Deletes a record only if all existing columns have the same values as
     * the specified record asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param record A record with all columns set.
     * @param callback Callback that is called on operation completion. Called with
     *   a value indicating whether a record with the specified key was deleted.
     */
    void remove_exact_async(transaction *tx, const value_type &record, ignite_callback<bool> callback) {
        m_delegate.remove_exact_async(tx, convert_to_tuple(record), std::move(callback));
    }

    /**
     * Deletes a record only if all existing columns have the same values as
     * the specified record.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param record A record with all columns set.
     * @return A value indicating whether a record with the specified key was
     *   deleted.
     */
    bool remove_exact(transaction *tx, const value_type &record) {
        return sync<bool>([this, tx, &record](auto callback) { remove_exact_async(tx, record, std::move(callback)); });
    }

    /**
     * Gets and deletes a record with the specified key asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key A record with key columns set.
     * @param callback Callback that is called on operation completion. Called with
     *   a deleted record or @c std::nullopt if it did not exist.
     */
    void get_and_remove_async(
        transaction *tx, const value_type &key, ignite_callback<std::optional<value_type>> callback) {
        m_delegate.get_and_remove_async(tx, convert_to_tuple(key),
            [callback = std::move(callback)](auto res) { callback(convert_result<value_type>(std::move(res))); });
    }

    /**
     * Gets and deletes a record with the specified key.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key A record with key columns set.
     * @return A deleted record or @c std::nullopt if it did not exist.
     */
    std::optional<value_type> get_and_remove(transaction *tx, const value_type &key) {
        return sync<std::optional<value_type>>(
            [this, tx, &key](auto callback) { get_and_remove_async(tx, key, std::move(callback)); });
    }

    /**
     * Deletes multiple records from the table asynchronously. If one or more
     * keys do not exist, other records are still deleted
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param keys Record keys to delete.
     * @param callback Callback that is called on operation completion. Called with
     *   records from @c keys that did not exist.
     */
    void remove_all_async(
        transaction *tx, std::vector<value_type> keys, ignite_callback<std::vector<value_type>> callback) {
        m_delegate.remove_all_async(tx, values_to_tuples<value_type>(std::move(keys)),
            [callback = std::move(callback)](auto res) { callback(convert_result<value_type>(std::move(res))); });
    }

    /**
     * Deletes multiple records from the table If one or more keys do not exist,
     * other records are still deleted
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param keys Record keys to delete.
     * @return Records from @c keys that did not exist.
     */
    std::vector<value_type> remove_all(transaction *tx, std::vector<value_type> keys) {
        return sync<std::vector<value_type>>([this, tx, keys = std::move(keys)](auto callback) mutable {
            remove_all_async(tx, std::move(keys), std::move(callback));
        });
    }

    /**
     * Deletes multiple exactly matching records asynchronously. If one or more
     * records do not exist, other records are still deleted.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param records Records to delete.
     * @param callback Callback that is called on operation completion. Called with
     *   records from @c records that did not exist.
     */
    void remove_all_exact_async(
        transaction *tx, std::vector<value_type> records, ignite_callback<std::vector<value_type>> callback) {
        m_delegate.remove_all_exact_async(tx, values_to_tuples<value_type>(std::move(records)),
            [callback = std::move(callback)](auto res) { callback(convert_result<value_type>(std::move(res))); });
    }

    /**
     * Deletes multiple exactly matching records. If one or more records do not
     * exist, other records are still deleted.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param records Records to delete.
     * @return Records from @c records that did not exist.
     */
    std::vector<value_type> remove_all_exact(transaction *tx, std::vector<value_type> records) {
        return sync<std::vector<value_type>>([this, tx, records = std::move(records)](auto callback) mutable {
            remove_all_exact_async(tx, std::move(records), std::move(callback));
        });
    }

private:
    /**
     * Constructor
     *
     * @param impl Implementation
     */
    explicit record_view(record_view<ignite_tuple> delegate)
        : m_delegate(std::move(delegate)) {}

    /** Delegate. */
    record_view<ignite_tuple> m_delegate;
};

} // namespace ignite
