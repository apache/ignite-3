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

void tables_impl::get_table_async(const std::string &name, ignite_callback<std::optional<table>> callback) {
    auto reader_func = [name](protocol::reader &reader) -> std::optional<table> {
        if (reader.try_read_nil())
            return std::nullopt;

        auto id = reader.read_uuid();
        auto tableImpl = std::make_shared<table_impl>(name, id);

        return std::make_optional(table(tableImpl));
    };

    auto handler =
        std::make_shared<response_handler_impl<std::optional<table>>>(std::move(reader_func), std::move(callback));

    m_connection->perform_request(
        client_operation::TABLE_GET, [&name](protocol::writer &writer) { writer.write(name); }, std::move(handler));
}

void tables_impl::get_tables_async(ignite_callback<std::vector<table>> callback) {
    auto reader_func = [](protocol::reader &reader) -> std::vector<table> {
        if (reader.try_read_nil())
            return {};

        std::vector<table> tables;
        tables.reserve(reader.read_map_size());

        reader.read_map<uuid, std::string>([&tables](auto &&id, auto &&name) {
            auto tableImpl = std::make_shared<table_impl>(std::forward<std::string>(name), std::forward<uuid>(id));
            tables.push_back(table{tableImpl});
        });

        return tables;
    };

    auto handler =
        std::make_shared<response_handler_impl<std::vector<table>>>(std::move(reader_func), std::move(callback));

    m_connection->perform_request(client_operation::TABLES_GET, std::move(handler));
}

} // namespace ignite::detail
