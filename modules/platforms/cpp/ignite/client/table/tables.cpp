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

#include "ignite/client/table/tables.h"
#include "ignite/client/detail/table/tables_impl.h"

namespace ignite {

std::optional<table> tables::get_table(std::string_view name) {
    return sync<std::optional<table>>([this, name](auto callback) { get_table_async(name, std::move(callback)); });
}

void tables::get_table_async(std::string_view name, ignite_callback<std::optional<table>> callback) {
    m_impl->get_table_async(name, std::move(callback));
}

std::vector<table> tables::get_tables() {
    return sync<std::vector<table>>([this](auto callback) { get_tables_async(std::move(callback)); });
}

void tables::get_tables_async(ignite_callback<std::vector<table>> callback) {
    m_impl->get_tables_async(std::move(callback));
}

} // namespace ignite
