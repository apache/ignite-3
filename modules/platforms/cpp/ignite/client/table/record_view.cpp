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
#include "ignite/client/detail/table/record_binary_view_impl.h"

namespace ignite {

void record_view<ignite_tuple>::get_async(transaction *tx, const ignite_tuple& key,
        ignite_callback<std::optional<value_type>> callback)
{
    m_impl->get_async(tx, key, std::move(callback));
}

void record_view<ignite_tuple>::upsert_async(transaction *tx, const ignite_tuple& record,
        ignite_callback<void> callback)
{
    m_impl->upsert_async(tx, record, std::move(callback));
}

void record_view<ignite_tuple>::get_all_async(transaction *tx, std::vector<value_type> keys,
        ignite_callback<std::vector<std::optional<value_type>>> callback)
{
    m_impl->get_all_async(tx, std::move(keys), std::move(callback));
}

void record_view<ignite_tuple>::upsert_all_async(transaction *tx, std::vector<value_type> records,
    ignite_callback<void> callback)
{
    m_impl->upsert_all_async(tx, std::move(records), std::move(callback));
}

} // namespace ignite
