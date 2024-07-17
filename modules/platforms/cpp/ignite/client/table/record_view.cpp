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

#include "ignite/client/table/record_view.h"
#include "ignite/client/detail/argument_check_utils.h"
#include "ignite/client/detail/table/table_impl.h"

namespace ignite {

void record_view<ignite_tuple>::get_async(
    transaction *tx, const ignite_tuple &key, ignite_callback<std::optional<value_type>> callback) {
    detail::arg_check::tuple_non_empty(key, "Tuple");

    m_impl->get_async(tx, key, std::move(callback));
}

void record_view<ignite_tuple>::upsert_async(
    transaction *tx, const ignite_tuple &record, ignite_callback<void> callback) {
    detail::arg_check::tuple_non_empty(record, "Tuple");

    m_impl->upsert_async(tx, record, std::move(callback));
}

void record_view<ignite_tuple>::get_all_async(
    transaction *tx, std::vector<value_type> keys, ignite_callback<std::vector<std::optional<value_type>>> callback) {
    if (keys.empty()) {
        callback(std::vector<std::optional<value_type>>{});
        return;
    }

    m_impl->get_all_async(tx, std::move(keys), std::move(callback));
}

void record_view<ignite_tuple>::upsert_all_async(
    transaction *tx, std::vector<value_type> records, ignite_callback<void> callback) {
    if (records.empty()) {
        callback({});
        return;
    }

    m_impl->upsert_all_async(tx, std::move(records), std::move(callback));
}

void record_view<ignite_tuple>::get_and_upsert_async(
    transaction *tx, const ignite_tuple &record, ignite_callback<std::optional<value_type>> callback) {
    detail::arg_check::tuple_non_empty(record, "Tuple");

    m_impl->get_and_upsert_async(tx, record, std::move(callback));
}

void record_view<ignite_tuple>::insert_async(
    transaction *tx, const ignite_tuple &record, ignite_callback<bool> callback) {
    detail::arg_check::tuple_non_empty(record, "Tuple");

    m_impl->insert_async(tx, record, std::move(callback));
}

void record_view<ignite_tuple>::insert_all_async(
    transaction *tx, std::vector<value_type> records, ignite_callback<std::vector<value_type>> callback) {
    if (records.empty()) {
        callback(std::vector<value_type>{});
        return;
    }

    m_impl->insert_all_async(tx, std::move(records), std::move(callback));
}

void record_view<ignite_tuple>::replace_async(
    transaction *tx, const ignite_tuple &record, ignite_callback<bool> callback) {
    detail::arg_check::tuple_non_empty(record, "Tuple");

    m_impl->replace_async(tx, record, std::move(callback));
}

void record_view<ignite_tuple>::replace_async(
    transaction *tx, const ignite_tuple &record, const ignite_tuple &new_record, ignite_callback<bool> callback) {
    detail::arg_check::tuple_non_empty(record, "Tuple");
    detail::arg_check::tuple_non_empty(new_record, "Tuple");

    m_impl->replace_async(tx, record, new_record, std::move(callback));
}

void record_view<ignite_tuple>::get_and_replace_async(
    transaction *tx, const ignite_tuple &record, ignite_callback<std::optional<value_type>> callback) {
    detail::arg_check::tuple_non_empty(record, "Tuple");

    m_impl->get_and_replace_async(tx, record, std::move(callback));
}

void record_view<ignite_tuple>::remove_async(transaction *tx, const ignite_tuple &key, ignite_callback<bool> callback) {
    detail::arg_check::tuple_non_empty(key, "Tuple");

    m_impl->remove_async(tx, key, std::move(callback));
}

void record_view<ignite_tuple>::remove_exact_async(
    transaction *tx, const ignite_tuple &record, ignite_callback<bool> callback) {
    detail::arg_check::tuple_non_empty(record, "Tuple");

    m_impl->remove_exact_async(tx, record, std::move(callback));
}

void record_view<ignite_tuple>::get_and_remove_async(
    transaction *tx, const ignite_tuple &key, ignite_callback<std::optional<value_type>> callback) {
    detail::arg_check::tuple_non_empty(key, "Tuple");

    m_impl->get_and_remove_async(tx, key, std::move(callback));
}

void record_view<ignite_tuple>::remove_all_async(
    transaction *tx, std::vector<value_type> keys, ignite_callback<std::vector<value_type>> callback) {
    if (keys.empty()) {
        callback(std::vector<value_type>{});
        return;
    }

    m_impl->remove_all_async(tx, std::move(keys), std::move(callback));
}

void record_view<ignite_tuple>::remove_all_exact_async(
    transaction *tx, std::vector<value_type> records, ignite_callback<std::vector<value_type>> callback) {
    if (records.empty()) {
        callback(std::vector<value_type>{});
        return;
    }

    m_impl->remove_all_exact_async(tx, std::move(records), std::move(callback));
}

} // namespace ignite
