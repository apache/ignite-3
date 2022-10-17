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

#include "tables.h"

#include "../detail/table/tables_impl.h"

#include <ignite/common/utils.h>

#include <utility>

namespace ignite {

void tables::get_table_async(const std::string &name, ignite_callback<std::optional<table>> callback) {
    m_impl->get_table_async(name, std::move(callback));
}

void tables::get_tables_async(ignite_callback<std::vector<table>> callback) {
    m_impl->get_tables_async(std::move(callback));
}

} // namespace ignite
