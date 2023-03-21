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
#include "ignite/client/detail/argument_check_utils.h"
#include "ignite/client/detail/table/table_impl.h"

namespace ignite {

/**
 * Process multiple kv pairs by uniting key and value part of the tuple
 * to a single record.
 *
 * @param pairs Pairs.
 */
std::vector<ignite_tuple> concat_records(const std::vector<std::pair<ignite_tuple, ignite_tuple>> &pairs) {
    // TODO: IGNITE-18855 eliminate unnecessary tuple transformation;
    std::vector<ignite_tuple> records;
    records.reserve(pairs.size());
    for (const auto &pair : pairs)
        records.emplace_back(detail::concat(pair.first, pair.second));

    return records;
}

void key_value_view<ignite_tuple, ignite_tuple>::get_async(
    transaction *tx, const ignite_tuple &key, ignite_callback<std::optional<value_type>> callback) {
    detail::arg_check::key_tuple_non_empty(key);

    m_impl->get_async(tx, key, std::move(callback));
}

void key_value_view<ignite_tuple, ignite_tuple>::put_async(
    transaction *tx, const key_type &key, const value_type &value, ignite_callback<void> callback) {
    detail::arg_check::key_tuple_non_empty(key);
    detail::arg_check::value_tuple_non_empty(value);

    m_impl->upsert_async(tx, detail::concat(key, value), std::move(callback));
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
    detail::arg_check::key_tuple_non_empty(key);

    m_impl->contains_async(tx, key, std::move(callback));
}

void key_value_view<ignite_tuple, ignite_tuple>::put_all_async(
    transaction *tx, const std::vector<std::pair<key_type, value_type>> &pairs, ignite_callback<void> callback) {
    if (pairs.empty()) {
        callback({});
        return;
    }

    m_impl->upsert_all_async(tx, concat_records(pairs), std::move(callback));
}

void key_value_view<ignite_tuple, ignite_tuple>::get_and_put_async(transaction *tx, const key_type &key,
    const value_type &value, ignite_callback<std::optional<value_type>> callback) {
    detail::arg_check::key_tuple_non_empty(key);
    detail::arg_check::value_tuple_non_empty(value);

    m_impl->get_and_upsert_async(tx, detail::concat(key, value), std::move(callback));
}

void key_value_view<ignite_tuple, ignite_tuple>::put_if_absent_async(
    transaction *tx, const key_type &key, const value_type &value, ignite_callback<bool> callback) {
    detail::arg_check::key_tuple_non_empty(key);
    detail::arg_check::value_tuple_non_empty(value);

    m_impl->insert_async(tx, detail::concat(key, value), std::move(callback));
}

void key_value_view<ignite_tuple, ignite_tuple>::remove_async(
    transaction *tx, const ignite_tuple &key, ignite_callback<bool> callback) {
    detail::arg_check::key_tuple_non_empty(key);

    m_impl->remove_async(tx, key, std::move(callback));
}

void key_value_view<ignite_tuple, ignite_tuple>::remove_async(
    transaction *tx, const key_type &key, const value_type &value, ignite_callback<bool> callback) {
    detail::arg_check::key_tuple_non_empty(key);
    detail::arg_check::value_tuple_non_empty(value);

    m_impl->remove_exact_async(tx, detail::concat(key, value), std::move(callback));
}

void key_value_view<ignite_tuple, ignite_tuple>::remove_all_async(
    transaction *tx, std::vector<key_type> keys, ignite_callback<std::vector<value_type>> callback) {
    if (keys.empty()) {
        callback(std::vector<value_type>{});
        return;
    }

    m_impl->remove_all_async(tx, std::move(keys), std::move(callback));
}

void key_value_view<ignite_tuple, ignite_tuple>::remove_all_async(transaction *tx,
    const std::vector<std::pair<key_type, value_type>> &pairs, ignite_callback<std::vector<value_type>> callback) {
    if (pairs.empty()) {
        callback(std::vector<value_type>{});
        return;
    }

    m_impl->remove_all_exact_async(tx, concat_records(pairs), std::move(callback));
}

void key_value_view<ignite_tuple, ignite_tuple>::get_and_remove_async(
    transaction *tx, const ignite_tuple &key, ignite_callback<std::optional<value_type>> callback) {
    detail::arg_check::key_tuple_non_empty(key);

    m_impl->get_and_remove_async(tx, key, std::move(callback));
}

void key_value_view<ignite_tuple, ignite_tuple>::replace_async(
    transaction *tx, const key_type &key, const value_type &value, ignite_callback<bool> callback) {
    detail::arg_check::key_tuple_non_empty(key);
    detail::arg_check::value_tuple_non_empty(value);

    m_impl->replace_async(tx, detail::concat(key, value), std::move(callback));
}

void key_value_view<ignite_tuple, ignite_tuple>::replace_async(transaction *tx, const key_type &key,
    const value_type &old_value, const value_type &new_value, ignite_callback<bool> callback) {
    detail::arg_check::key_tuple_non_empty(key);
    detail::arg_check::value_tuple_non_empty(old_value);
    detail::arg_check::value_tuple_non_empty(new_value);

    m_impl->replace_async(tx, detail::concat(key, old_value), detail::concat(key, new_value), std::move(callback));
}

void key_value_view<ignite_tuple, ignite_tuple>::get_and_replace_async(transaction *tx, const key_type &key,
    const value_type &value, ignite_callback<std::optional<value_type>> callback) {
    detail::arg_check::key_tuple_non_empty(key);
    detail::arg_check::value_tuple_non_empty(value);

    m_impl->get_and_replace_async(tx, detail::concat(key, value), std::move(callback));
}

} // namespace ignite
