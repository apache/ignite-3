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

#include <utility>

#include "common/utils.h"
#include "ignite/table/tables.h"

#include "table/tables_impl.h"

namespace ignite {

void Tables::getTableAsync(const std::string &name, ignite_callback<std::optional<Table>> callback) {
    getImpl().getTableAsync(name, std::move(callback));
}

Tables::Tables(std::shared_ptr<void> impl)
    : m_impl(std::move(impl)) {
}

detail::TablesImpl &Tables::getImpl() {
    return *((detail::TablesImpl *)(m_impl.get()));
}

const detail::TablesImpl &Tables::getImpl() const {
    return *((detail::TablesImpl *)(m_impl.get()));
}

} // namespace ignite
