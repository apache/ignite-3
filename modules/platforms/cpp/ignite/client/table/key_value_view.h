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

#include "ignite/client/detail/type_mapping_utils.h"
#include "ignite/client/table/ignite_tuple.h"
#include "ignite/client/transaction/transaction.h"
#include "ignite/client/type_mapping.h"

#include "ignite/common/detail/config.h"
#include "ignite/common/ignite_result.h"

#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

namespace ignite {

class table;

namespace detail {
class table_impl;
}

template<typename K, typename V>
class key_value_view;

/**
 * Key-Value view interface provides methods to access table records in form of separate key and value parts.
 */
template<>
class key_value_view<ignite_tuple, ignite_tuple> {
    friend class table;

public:
    typedef ignite_tuple key_type;
    typedef ignite_tuple value_type;

    // Deleted
    key_value_view(const key_value_view &) = delete;
    key_value_view &operator=(const key_value_view &) = delete;

    // Default
    key_value_view() = default;
    key_value_view(key_value_view &&) noexcept = default;
    key_value_view &operator=(key_value_view &&) noexcept = default;

    /**
     * Gets a value by key asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param callback Callback which is called on success with value if it
     *   exists and @c std::nullopt otherwise
     */
    IGNITE_API void get_async(
        transaction *tx, const key_type &key, ignite_callback<std::optional<value_type>> callback);

    /**
     * Gets a value by key.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @return Value if exists and @c std::nullopt otherwise.
     */
    [[nodiscard]] IGNITE_API std::optional<value_type> get(transaction *tx, const key_type &key) {
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
    IGNITE_API void get_all_async(
        transaction *tx, std::vector<key_type> keys, ignite_callback<std::vector<std::optional<value_type>>> callback);

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
        transaction *tx, std::vector<key_type> keys) {
        return sync<std::vector<std::optional<value_type>>>([this, tx, keys = std::move(keys)](auto callback) mutable {
            get_all_async(tx, std::move(keys), std::move(callback));
        });
    }

    /**
     * Asynchronously determines if the table contains an entry for the specified key.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param callback Callback which is called on success with value
     *   indicating whether value exists or not.
     */
    IGNITE_API void contains_async(transaction *tx, const key_type &key, ignite_callback<bool> callback);

    /**
     * Determines if the table contains an entry for the specified key.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @return Value indicating whether value exists or not.
     */
    [[nodiscard]] IGNITE_API bool contains(transaction *tx, const key_type &key) {
        return sync<bool>([this, tx, &key](auto callback) { contains_async(tx, key, std::move(callback)); });
    }

    /**
     * Puts a value with a given key asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *  single operation is used.
     * @param key Key.
     * @param value Value.
     * @param callback Callback.
     */
    IGNITE_API void put_async(
        transaction *tx, const key_type &key, const value_type &value, ignite_callback<void> callback);

    /**
     * Puts a value with a given key.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *  single operation is used.
     * @param key Key.
     * @param value Value.
     */
    IGNITE_API void put(transaction *tx, const key_type &key, const value_type &value) {
        sync<void>([this, tx, &key, &value](auto callback) { put_async(tx, key, value, std::move(callback)); });
    }

    /**
     * Puts multiple key-value pairs asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param pairs Pairs to put.
     * @param callback Callback that is called on operation completion.
     */
    IGNITE_API void put_all_async(
        transaction *tx, const std::vector<std::pair<key_type, value_type>> &pairs, ignite_callback<void> callback);

    /**
     * Puts multiple key-value pairs.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param pairs Pairs to put.
     */
    IGNITE_API void put_all(transaction *tx, const std::vector<std::pair<key_type, value_type>> &pairs) {
        sync<void>([this, tx, pairs](auto callback) mutable { put_all_async(tx, pairs, std::move(callback)); });
    }

    /**
     * Puts a value with a given key and returns previous value for the key asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param value Value.
     * @param callback Callback. Called with a value which contains replaced
     *   value or @c std::nullopt if it did not exist.
     */
    IGNITE_API void get_and_put_async(transaction *tx, const key_type &key, const value_type &value,
        ignite_callback<std::optional<value_type>> callback);

    /**
     * Puts a value with a given key and returns previous value for the key.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param value Value.
     * @return A replaced value or @c std::nullopt if it did not exist.
     */
    [[nodiscard]] IGNITE_API std::optional<value_type> get_and_put(
        transaction *tx, const key_type &key, const value_type &value) {
        return sync<std::optional<value_type>>(
            [this, tx, &key, &value](auto callback) { get_and_put_async(tx, key, value, std::move(callback)); });
    }

    /**
     * Asynchronously puts a value with a given key if the specified key is not present in the table.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param value Value.
     * @param callback Callback. Called with a value indicating whether the
     *   record was inserted. Equals @c false if a record with the same key
     *   already exists.
     */
    IGNITE_API void put_if_absent_async(
        transaction *tx, const key_type &key, const value_type &value, ignite_callback<bool> callback);

    /**
     * Puts a value with a given key if the specified key is not present in the table.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param value Value.
     */
    IGNITE_API bool put_if_absent(transaction *tx, const key_type &key, const value_type &value) {
        return sync<bool>(
            [this, tx, &key, &value](auto callback) { put_if_absent_async(tx, key, value, std::move(callback)); });
    }

    /**
     * Removes a value with the specified key asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param callback Callback that is called on operation completion. Called with
     *   a value indicating whether a record with the specified key was deleted.
     */
    IGNITE_API void remove_async(transaction *tx, const key_type &key, ignite_callback<bool> callback);

    /**
     * Removes a value with the specified key.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @return A value indicating whether a record with the specified key was deleted.
     */
    IGNITE_API bool remove(transaction *tx, const key_type &key) {
        return sync<bool>([this, tx, &key](auto callback) { remove_async(tx, key, std::move(callback)); });
    }

    /**
     * Asynchronously removes a value with a given key from the table only if it is equal to the specified value.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param value Value.
     * @param callback Callback that is called on operation completion. Called with
     *   a value indicating whether a record with the specified key was deleted.
     */
    IGNITE_API void remove_async(
        transaction *tx, const key_type &key, const value_type &value, ignite_callback<bool> callback);

    /**
     * Removes a value with a given key from the table only if it is equal to the specified value.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param value Value.
     * @return A value indicating whether a record with the specified key was
     *   deleted.
     */
    IGNITE_API bool remove(transaction *tx, const key_type &key, const value_type &value) {
        return sync<bool>(
            [this, tx, &key, &value](auto callback) { remove_async(tx, key, value, std::move(callback)); });
    }

    /**
     * Removes values with given keys from the table asynchronously. If one or
     * more keys do not exist, other values are still removed
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param keys Keys.
     * @param callback Callback that is called on operation completion. Called with
     *   records from @c keys that did not exist.
     */
    IGNITE_API void remove_all_async(
        transaction *tx, std::vector<key_type> keys, ignite_callback<std::vector<key_type>> callback);

    /**
     * Removes values with given keys from the table. If one or more keys
     * do not exist, other values are still removed
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param keys Keys.
     * @return Records from @c keys that did not exist.
     */
    IGNITE_API std::vector<key_type> remove_all(transaction *tx, std::vector<key_type> keys) {
        return sync<std::vector<key_type>>([this, tx, keys = std::move(keys)](auto callback) mutable {
            remove_all_async(tx, std::move(keys), std::move(callback));
        });
    }

    /**
     * Removes records with given keys and values from the table asynchronously.
     * If one or more records do not exist, other records are still removed.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param pairs Pairs to remove.
     * @param callback Callback that is called on operation completion. Called with
     *   records from @c records that did not exist.
     */
    IGNITE_API void remove_all_async(transaction *tx, const std::vector<std::pair<key_type, value_type>> &pairs,
        ignite_callback<std::vector<key_type>> callback);

    /**
     * Removes records with given keys and values from the table. If one or more
     * records do not exist, other records are still removed.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param pairs Pairs to remove.
     * @return Records from @c records that did not exist.
     */
    IGNITE_API std::vector<key_type> remove_all(transaction *tx, std::vector<std::pair<key_type, value_type>> pairs) {
        return sync<std::vector<key_type>>([this, tx, pairs = std::move(pairs)](auto callback) mutable {
            remove_all_async(tx, std::move(pairs), std::move(callback));
        });
    }

    /**
     * Gets and removes a value associated with the given key asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param callback Callback that is called on operation completion. Called with
     *   a removed record or @c std::nullopt if it did not exist.
     */
    IGNITE_API void get_and_remove_async(
        transaction *tx, const key_type &key, ignite_callback<std::optional<value_type>> callback);

    /**
     * Gets and removes a value associated with the given key.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key key.
     * @return A removed record or @c std::nullopt if it did not exist.
     */
    IGNITE_API std::optional<value_type> get_and_remove(transaction *tx, const key_type &key) {
        return sync<std::optional<value_type>>(
            [this, tx, &key](auto callback) { get_and_remove_async(tx, key, std::move(callback)); });
    }

    /**
     * Asynchronously replaces a record with the specified key if it exists,
     * otherwise does nothing.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param value Value.
     * @param callback Callback. Called with a value indicating whether a record
     *   with the specified key was replaced.
     */
    IGNITE_API void replace_async(
        transaction *tx, const key_type &key, const value_type &value, ignite_callback<bool> callback);

    /**
     * Replaces a record with the same key columns if it exists, otherwise does
     * nothing.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param value Value.
     * @return A value indicating whether a record with the specified key was
     *   replaced.
     */
    IGNITE_API bool replace(transaction *tx, const key_type &key, const value_type &value) {
        return sync<bool>(
            [this, tx, &key, &value](auto callback) { replace_async(tx, key, value, std::move(callback)); });
    }

    /**
     * Asynchronously replaces a value with a @c new_value one only if existing
     * value equals to the specified @c old_value.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param old_value Value to be replaced.
     * @param new_value New value.
     * @param callback Callback. Called with a value indicating whether a
     *   specified record was replaced.
     */
    IGNITE_API void replace_async(transaction *tx, const key_type &key, const value_type &old_value,
        const value_type &new_value, ignite_callback<bool> callback);

    /**
     * Replaces a value with a @c new_value one only if existing value equals
     * to the specified @c old_value.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param old_value Value to be replaced.
     * @param new_value New value.
     * @return A value indicating whether a specified record was replaced.
     */
    IGNITE_API bool replace(
        transaction *tx, const key_type &key, const value_type &old_value, const value_type &new_value) {
        return sync<bool>([this, tx, &key, &old_value, &new_value](
                              auto callback) { replace_async(tx, key, old_value, new_value, std::move(callback)); });
    }

    /**
     * Asynchronously replaces a record with the given key if it exists
     * returning previous value.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param value Value.
     * @param callback Callback. Called with a previous value for the given key,
     *   or @c std::nullopt if it did not exist.
     */
    IGNITE_API void get_and_replace_async(transaction *tx, const key_type &key, const value_type &value,
        ignite_callback<std::optional<value_type>> callback);

    /**
     * Replaces a record with the given key if it exists returning previous
     * value.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param value Value.
     * @param callback A previous value for the given key, or @c std::nullopt if
     *   it did not exist.
     */
    [[nodiscard]] IGNITE_API std::optional<value_type> get_and_replace(
        transaction *tx, const key_type &key, const value_type &value) {
        return sync<std::optional<value_type>>(
            [this, tx, &key, &value](auto callback) { get_and_replace_async(tx, key, value, std::move(callback)); });
    }

private:
    /**
     * Constructor
     *
     * @param impl Implementation
     */
    explicit key_value_view(std::shared_ptr<detail::table_impl> impl)
        : m_impl(std::move(impl)) {}

    /** Implementation. */
    std::shared_ptr<detail::table_impl> m_impl;
};

/**
 * Key-Value view interface provides methods to access table records in form of separate key and value parts.
 */
template<typename K, typename V>
class key_value_view {
    friend class table;

public:
    typedef typename std::decay<K>::type key_type;
    typedef typename std::decay<V>::type value_type;

    // Deleted
    key_value_view(const key_value_view &) = delete;
    key_value_view &operator=(const key_value_view &) = delete;

    // Default
    key_value_view() = default;
    key_value_view(key_value_view &&) noexcept = default;
    key_value_view &operator=(key_value_view &&) noexcept = default;

    /**
     * Gets a value by key asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param callback Callback which is called on success with value if it
     *   exists and @c std::nullopt otherwise
     */
    void get_async(transaction *tx, const key_type &key, ignite_callback<std::optional<value_type>> callback) {
        m_delegate.get_async(tx, convert_to_tuple(key),
            [callback = std::move(callback)](auto res) { callback(convert_result<value_type>(std::move(res))); });
    }

    /**
     * Gets a value by key.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @return Value if exists and @c std::nullopt otherwise.
     */
    [[nodiscard]] std::optional<value_type> get(transaction *tx, const key_type &key) {
        return sync<std::optional<value_type>>(
            [this, tx, &key](auto callback) { get_async(tx, key, std::move(callback)); });
    }

    /**
     * Gets multiple values by keys asynchronously.
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
    void get_all_async(
        transaction *tx, std::vector<key_type> keys, ignite_callback<std::vector<std::optional<value_type>>> callback) {
        m_delegate.get_all_async(tx, values_to_tuples<key_type>(std::move(keys)),
            [callback = std::move(callback)](auto res) { callback(convert_result<value_type>(std::move(res))); });
    }

    /**
     * Gets multiple values by keys.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param keys Keys.
     * @return Resulting records with all columns filled from the table.
     *   The order of elements is guaranteed to be the same as the order of
     *   keys. If a record does not exist, the resulting element of the
     *   corresponding order is @c std::nullopt.
     */
    [[nodiscard]] std::vector<std::optional<value_type>> get_all(transaction *tx, std::vector<key_type> keys) {
        return sync<std::vector<std::optional<value_type>>>([this, tx, keys = std::move(keys)](auto callback) mutable {
            get_all_async(tx, std::move(keys), std::move(callback));
        });
    }

    /**
     * Asynchronously determines if the table contains a value for the specified key.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param callback Callback which is called on success with value
     *   indicating whether value exists or not.
     */
    void contains_async(transaction *tx, const key_type &key, ignite_callback<bool> callback) {
        m_delegate.contains_async(tx, convert_to_tuple(key), std::move(callback));
    }

    /**
     * Determines if the table contains an entry for the specified key.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @return Value indicating whether value exists or not.
     */
    [[nodiscard]] bool contains(transaction *tx, const key_type &key) {
        return sync<bool>([this, tx, &key](auto callback) { contains_async(tx, key, std::move(callback)); });
    }

    /**
     * Puts a value with a given key asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *  single operation is used.
     * @param key Key.
     * @param value Value.
     * @param callback Callback.
     */
    void put_async(transaction *tx, const key_type &key, const value_type &value, ignite_callback<void> callback) {
        m_delegate.put_async(tx, convert_to_tuple(key), convert_to_tuple(value), std::move(callback));
    }

    /**
     * Puts a value with a given key.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *  single operation is used.
     * @param key Key.
     * @param value Value.
     */
    void put(transaction *tx, const key_type &key, const value_type &value) {
        sync<void>([this, tx, &key, &value](auto callback) { put_async(tx, key, value, std::move(callback)); });
    }

    /**
     * Puts multiple key-value pairs asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param pairs Pairs to put.
     * @param callback Callback that is called on operation completion.
     */
    void put_all_async(
        transaction *tx, const std::vector<std::pair<key_type, value_type>> &pairs, ignite_callback<void> callback) {
        m_delegate.put_all_async(tx, values_to_tuples<key_type, value_type>(std::move(pairs)), std::move(callback));
    }

    /**
     * Puts multiple key-value pairs.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param pairs Pairs to put.
     */
    void put_all(transaction *tx, const std::vector<std::pair<key_type, value_type>> &pairs) {
        sync<void>([this, tx, pairs](auto callback) mutable { put_all_async(tx, pairs, std::move(callback)); });
    }

    /**
     * Puts a value with a given key and returns previous value for the key asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param value Value.
     * @param callback Callback. Called with a value which contains replaced
     *   value or @c std::nullopt if it did not exist.
     */
    void get_and_put_async(transaction *tx, const key_type &key, const value_type &value,
        ignite_callback<std::optional<value_type>> callback) {
        m_delegate.get_and_put_async(tx, convert_to_tuple(key), convert_to_tuple(value),
            [callback = std::move(callback)](auto res) { callback(convert_result<value_type>(std::move(res))); });
    }

    /**
     * Puts a value with a given key and returns previous value for the key.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param value Value.
     * @return A replaced value or @c std::nullopt if it did not exist.
     */
    [[nodiscard]] std::optional<value_type> get_and_put(transaction *tx, const key_type &key, const value_type &value) {
        return sync<std::optional<value_type>>(
            [this, tx, &key, &value](auto callback) { get_and_put_async(tx, key, value, std::move(callback)); });
    }

    /**
     * Asynchronously puts a value with a given key if the specified key is not present in the table.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param value Value.
     * @param callback Callback. Called with a value indicating whether the
     *   record was inserted. Equals @c false if a record with the same key
     *   already exists.
     */
    void put_if_absent_async(
        transaction *tx, const key_type &key, const value_type &value, ignite_callback<bool> callback) {
        m_delegate.put_if_absent_async(tx, convert_to_tuple(key), convert_to_tuple(value), std::move(callback));
    }

    /**
     * Puts a value with a given key if the specified key is not present in the table.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param value Value.
     */
    bool put_if_absent(transaction *tx, const key_type &key, const value_type &value) {
        return sync<bool>(
            [this, tx, &key, &value](auto callback) { put_if_absent_async(tx, key, value, std::move(callback)); });
    }

    /**
     * Removes a value with the specified key asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param callback Callback that is called on operation completion. Called with
     *   a value indicating whether a record with the specified key was deleted.
     */
    void remove_async(transaction *tx, const key_type &key, ignite_callback<bool> callback) {
        m_delegate.remove_async(tx, convert_to_tuple(key), std::move(callback));
    }

    /**
     * Removes a value with the specified key.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @return A value indicating whether a record with the specified key was deleted.
     */
    bool remove(transaction *tx, const key_type &key) {
        return sync<bool>([this, tx, &key](auto callback) { remove_async(tx, key, std::move(callback)); });
    }

    /**
     * Asynchronously removes a value with a given key from the table only if it is equal to the specified value.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param value Value.
     * @param callback Callback that is called on operation completion. Called with
     *   a value indicating whether a record with the specified key was deleted.
     */
    void remove_async(transaction *tx, const key_type &key, const value_type &value, ignite_callback<bool> callback) {
        m_delegate.remove_async(tx, convert_to_tuple(key), convert_to_tuple(value), std::move(callback));
    }

    /**
     * Removes a value with a given key from the table only if it is equal to the specified value.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param value Value.
     * @return A value indicating whether a record with the specified key was
     *   deleted.
     */
    bool remove(transaction *tx, const key_type &key, const value_type &value) {
        return sync<bool>(
            [this, tx, &key, &value](auto callback) { remove_async(tx, key, value, std::move(callback)); });
    }

    /**
     * Removes values with given keys from the table asynchronously. If one or
     * more keys do not exist, other values are still removed
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param keys Keys.
     * @param callback Callback that is called on operation completion. Called with
     *   records from @c keys that did not exist.
     */
    void remove_all_async(
        transaction *tx, std::vector<key_type> keys, ignite_callback<std::vector<key_type>> callback) {
        m_delegate.remove_all_async(tx, values_to_tuples<key_type>(std::move(keys)),
            [callback = std::move(callback)](auto res) { callback(convert_result<key_type>(std::move(res))); });
    }

    /**
     * Removes values with given keys from the table. If one or more keys
     * do not exist, other values are still removed
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param keys Keys.
     * @return Records from @c keys that did not exist.
     */
    std::vector<key_type> remove_all(transaction *tx, std::vector<key_type> keys) {
        return sync<std::vector<key_type>>([this, tx, keys = std::move(keys)](auto callback) mutable {
            remove_all_async(tx, std::move(keys), std::move(callback));
        });
    }

    /**
     * Removes records with given keys and values from the table asynchronously.
     * If one or more records do not exist, other records are still removed.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param pairs Pairs to remove.
     * @param callback Callback that is called on operation completion. Called with
     *   records from @c records that did not exist.
     */
    void remove_all_async(transaction *tx, const std::vector<std::pair<key_type, value_type>> &pairs,
        ignite_callback<std::vector<key_type>> callback) {
        m_delegate.remove_all_async(tx, values_to_tuples<key_type, value_type>(std::move(pairs)),
            [callback = std::move(callback)](auto res) { callback(convert_result<key_type>(std::move(res))); });
    }

    /**
     * Removes records with given keys and values from the table. If one or more
     * records do not exist, other records are still removed.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param pairs Pairs to remove.
     * @return Records from @c records that did not exist.
     */
    std::vector<key_type> remove_all(transaction *tx, std::vector<std::pair<key_type, value_type>> pairs) {
        return sync<std::vector<key_type>>([this, tx, pairs = std::move(pairs)](auto callback) mutable {
            remove_all_async(tx, std::move(pairs), std::move(callback));
        });
    }

    /**
     * Gets and removes a value associated with the given key asynchronously.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param callback Callback that is called on operation completion. Called with
     *   a removed record or @c std::nullopt if it did not exist.
     */
    void get_and_remove_async(
        transaction *tx, const key_type &key, ignite_callback<std::optional<value_type>> callback) {
        m_delegate.get_and_remove_async(tx, convert_to_tuple(key),
            [callback = std::move(callback)](auto res) { callback(convert_result<value_type>(std::move(res))); });
    }

    /**
     * Gets and removes a value associated with the given key.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key key.
     * @return A removed record or @c std::nullopt if it did not exist.
     */
    std::optional<value_type> get_and_remove(transaction *tx, const key_type &key) {
        return sync<std::optional<value_type>>(
            [this, tx, &key](auto callback) { get_and_remove_async(tx, key, std::move(callback)); });
    }

    /**
     * Asynchronously replaces a record with the specified key if it exists, otherwise does nothing.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param value Value.
     * @param callback Callback. Called with a value indicating whether a record
     *   with the specified key was replaced.
     */
    void replace_async(transaction *tx, const key_type &key, const value_type &value, ignite_callback<bool> callback) {
        m_delegate.replace_async(tx, convert_to_tuple(key), convert_to_tuple(value), std::move(callback));
    }

    /**
     * Replaces a record with the same key columns if it exists, otherwise does
     * nothing.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param value Value.
     * @return A value indicating whether a record with the specified key was
     *   replaced.
     */
    bool replace(transaction *tx, const key_type &key, const value_type &value) {
        return sync<bool>(
            [this, tx, &key, &value](auto callback) { replace_async(tx, key, value, std::move(callback)); });
    }

    /**
     * Asynchronously replaces a value with a @c new_value one only if existing value equals to the specified
     * @c old_value.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param old_value Value to be replaced.
     * @param new_value New value.
     * @param callback Callback. Called with a value indicating whether a
     *   specified record was replaced.
     */
    void replace_async(transaction *tx, const key_type &key, const value_type &old_value, const value_type &new_value,
        ignite_callback<bool> callback) {
        m_delegate.replace_async(
            tx, convert_to_tuple(key), convert_to_tuple(old_value), convert_to_tuple(new_value), std::move(callback));
    }

    /**
     * Replaces a value with a @c new_value one only if existing value equals
     * to the specified @c old_value.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param old_value Value to be replaced.
     * @param new_value New value.
     * @return A value indicating whether a specified record was replaced.
     */
    bool replace(transaction *tx, const key_type &key, const value_type &old_value, const value_type &new_value) {
        return sync<bool>([this, tx, &key, &old_value, &new_value](
                              auto callback) { replace_async(tx, key, old_value, new_value, std::move(callback)); });
    }

    /**
     * Asynchronously replaces a record with the given key if it exists
     * returning previous value.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param value Value.
     * @param callback Callback. Called with a previous value for the given key,
     *   or @c std::nullopt if it did not exist.
     */
    void get_and_replace_async(transaction *tx, const key_type &key, const value_type &value,
        ignite_callback<std::optional<value_type>> callback) {
        m_delegate.get_and_replace_async(tx, convert_to_tuple(key), convert_to_tuple(value),
            [callback = std::move(callback)](auto res) { callback(convert_result<value_type>(std::move(res))); });
    }

    /**
     * Replaces a record with the given key if it exists returning previous
     * value.
     *
     * @param tx Optional transaction. If nullptr implicit transaction for this
     *   single operation is used.
     * @param key Key.
     * @param value Value.
     * @param callback A previous value for the given key, or @c std::nullopt if
     *   it did not exist.
     */
    [[nodiscard]] std::optional<value_type> get_and_replace(
        transaction *tx, const key_type &key, const value_type &value) {
        return sync<std::optional<value_type>>(
            [this, tx, &key, &value](auto callback) { get_and_replace_async(tx, key, value, std::move(callback)); });
    }

private:
    /**
     * Constructor
     *
     * @param impl Implementation
     */
    explicit key_value_view(key_value_view<ignite_tuple, ignite_tuple> delegate)
        : m_delegate(std::move(delegate)) {}

    /** Delegate. */
    key_value_view<ignite_tuple, ignite_tuple> m_delegate;
};

} // namespace ignite
