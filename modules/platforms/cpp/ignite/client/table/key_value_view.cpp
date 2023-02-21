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

#include "ignite/client/table/key_value_view.h"
#include "ignite/client/detail/table/table_impl.h"

namespace ignite {

/**
 * Process multiple kv pairs by uniting key and value part of the tuple
 * to a single record.
 *
 * @param pairs Pairs.
 */
std::vector<ignite_tuple> unite_records(const std::vector<std::pair<ignite_tuple, ignite_tuple>>& pairs) {
    // TODO: IGNITE-18855 eliminate unnecessary tuple transformation;
    std::vector<ignite_tuple> records;
    records.reserve(pairs.size());
    for (const auto& pair : pairs)
        records.emplace_back(pair.first + pair.second);

    return records;
}

void key_value_view<ignite_tuple, ignite_tuple>::get_async(
    transaction *tx, const ignite_tuple &key, ignite_callback<std::optional<value_type>> callback) {
    if (0 == key.column_count())
        throw ignite_error("Tuple can not be empty");

    m_impl->get_async(tx, key, std::move(callback));
}

void key_value_view<ignite_tuple, ignite_tuple>::put_async(
    transaction *tx, const key_type &key, const value_type &value, ignite_callback<void> callback) {
    if (0 == key.column_count())
        throw ignite_error("Key tuple can not be empty");

    if (0 == value.column_count())
        throw ignite_error("Value tuple can not be empty");

    m_impl->upsert_async(tx, key + value, std::move(callback));
}

void key_value_view<ignite_tuple, ignite_tuple>::get_all_async(
    transaction *tx, std::vector<value_type> keys, ignite_callback<std::vector<std::optional<value_type>>> callback) {
    if (keys.empty()) {
        callback(std::vector<std::optional<value_type>>{});
        return;
    }

    m_impl->get_all_async(tx, std::move(keys), std::move(callback));
}

void key_value_view<ignite_tuple, ignite_tuple>::contains_async(
    transaction *tx, const ignite_tuple &key, ignite_callback<bool> callback) {
    if (0 == key.column_count())
        throw ignite_error("Tuple can not be empty");

    m_impl->contains_async(tx, key, std::move(callback));
}

void key_value_view<ignite_tuple, ignite_tuple>::put_all_async(
    transaction *tx, const std::vector<std::pair<key_type, value_type>>& pairs, ignite_callback<void> callback) {
    if (pairs.empty()) {
        callback({});
        return;
    }

    m_impl->upsert_all_async(tx, unite_records(pairs), std::move(callback));
}

void key_value_view<ignite_tuple, ignite_tuple>::get_and_put_async(
    transaction *tx, const key_type &key, const value_type &value, ignite_callback<std::optional<value_type>> callback) {
    if (0 == key.column_count())
        throw ignite_error("Key tuple can not be empty");

    if (0 == value.column_count())
        throw ignite_error("Value tuple can not be empty");

    m_impl->get_and_upsert_async(tx, key + value, std::move(callback));
}

void key_value_view<ignite_tuple, ignite_tuple>::put_if_absent_async(
    transaction *tx, const key_type &key, const value_type &value, ignite_callback<bool> callback) {
    if (0 == key.column_count())
        throw ignite_error("Key tuple can not be empty");

    if (0 == value.column_count())
        throw ignite_error("Value tuple can not be empty");

    m_impl->insert_async(tx, key + value, std::move(callback));
}

void key_value_view<ignite_tuple, ignite_tuple>::remove_async(
    transaction *tx, const ignite_tuple &key, ignite_callback<bool> callback) {
    if (0 == key.column_count())
        throw ignite_error("Tuple can not be empty");

    m_impl->remove_async(tx, key, std::move(callback));
}

void key_value_view<ignite_tuple, ignite_tuple>::remove_async(
    transaction *tx, const key_type &key, const value_type &value, ignite_callback<bool> callback) {
    if (0 == key.column_count())
        throw ignite_error("Key tuple can not be empty");

    if (0 == value.column_count())
        throw ignite_error("Value tuple can not be empty");

    m_impl->remove_exact_async(tx, key + value, std::move(callback));
}

void key_value_view<ignite_tuple, ignite_tuple>::remove_all_async(
    transaction *tx, std::vector<key_type> keys, ignite_callback<std::vector<value_type>> callback) {
    if (keys.empty()) {
        callback(std::vector<value_type>{});
        return;
    }

    m_impl->remove_all_async(tx, std::move(keys), std::move(callback));
}

void key_value_view<ignite_tuple, ignite_tuple>::remove_all_async(
    transaction *tx, const std::vector<std::pair<key_type, value_type>>& pairs, ignite_callback<std::vector<value_type>> callback) {
    if (pairs.empty()) {
        callback(std::vector<value_type>{});
        return;
    }

    m_impl->remove_all_exact_async(tx, unite_records(pairs), std::move(callback));
}

void key_value_view<ignite_tuple, ignite_tuple>::get_and_remove_async(
    transaction *tx, const ignite_tuple &key, ignite_callback<std::optional<value_type>> callback) {
    if (0 == key.column_count())
        throw ignite_error("Tuple can not be empty");

    m_impl->get_and_remove_async(tx, key, std::move(callback));
}

void key_value_view<ignite_tuple, ignite_tuple>::replace_async(
    transaction *tx, const key_type &key, const value_type &value, ignite_callback<bool> callback) {
    if (0 == key.column_count())
        throw ignite_error("Key tuple can not be empty");

    if (0 == value.column_count())
        throw ignite_error("Value tuple can not be empty");

    m_impl->replace_async(tx, key + value, std::move(callback));
}

void key_value_view<ignite_tuple, ignite_tuple>::replace_async(
    transaction *tx, const key_type &key, const value_type &old_value, const value_type &new_value, ignite_callback<bool> callback) {
    if (0 == key.column_count())
        throw ignite_error("Key tuple can not be empty");

    if (0 == old_value.column_count())
        throw ignite_error("Old value tuple can not be empty");

    if (0 == new_value.column_count())
        throw ignite_error("New value tuple can not be empty");

    m_impl->replace_async(tx, key + old_value, key + new_value, std::move(callback));
}

void key_value_view<ignite_tuple, ignite_tuple>::get_and_replace_async(
    transaction *tx, const key_type &key, const value_type &value, ignite_callback<std::optional<value_type>> callback) {
    if (0 == key.column_count())
        throw ignite_error("Key tuple can not be empty");

    if (0 == value.column_count())
        throw ignite_error("Value tuple can not be empty");

    m_impl->get_and_replace_async(tx, key + value, std::move(callback));
}

} // namespace ignite
