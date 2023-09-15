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

#include "tables_impl.h"

#include <ignite/protocol/reader.h>
#include <ignite/protocol/writer.h>

namespace ignite::detail {

void tables_impl::get_table_async(std::string_view name, ignite_callback<std::optional<table>> callback) {
    auto writer_func = [&name](protocol::writer &writer) { writer.write(name); };

    auto reader_func = [name, conn = m_connection](protocol::reader &reader) mutable -> std::optional<table> {
        if (reader.try_read_nil())
            return std::nullopt;

        auto id = reader.read_int32();
        auto table0 = std::make_shared<table_impl>(std::string(name), id, std::move(conn));

        return std::make_optional(table(table0));
    };

    m_connection->perform_request<std::optional<table>>(
        protocol::client_operation::TABLE_GET, writer_func, std::move(reader_func), std::move(callback));
}

void tables_impl::get_tables_async(ignite_callback<std::vector<table>> callback) {
    auto reader_func = [conn = m_connection](protocol::reader &reader) -> std::vector<table> {
        if (reader.try_read_nil())
            return {};

        std::vector<table> tables;
        auto size = reader.read_int32();
        tables.reserve(size);

        for (std::int32_t table_idx = 0; table_idx < size; ++table_idx) {
            auto id = reader.read_int32();
            auto name = reader.read_string();
            tables.emplace_back(table{std::make_shared<table_impl>(std::move(name), id, conn)});
        }
        return tables;
    };

    m_connection->perform_request_rd<std::vector<table>>(
        protocol::client_operation::TABLES_GET, std::move(reader_func), std::move(callback));
}

} // namespace ignite::detail
