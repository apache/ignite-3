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

#include "ignite/protocol/reader.h"
#include "ignite/protocol/writer.h"

namespace ignite::detail {

void TablesImpl::getTableAsync(const std::string &name, ignite_callback<std::optional<Table>> callback) {
    auto readerFunc = [name](protocol::reader &reader) -> std::optional<Table> {
        if (reader.try_read_nil())
            return std::nullopt;

        auto id = reader.read_uuid();
        auto tableImpl = std::make_shared<TableImpl>(name, id);

        return std::make_optional(Table(tableImpl));
    };

    auto handler =
        std::make_shared<ResponseHandlerImpl<std::optional<Table>>>(std::move(readerFunc), std::move(callback));

    m_connection->performRequest(
        ClientOperation::TABLE_GET, [&name](protocol::writer &writer) { writer.write(name); }, std::move(handler));
}

void TablesImpl::getTablesAsync(ignite_callback<std::vector<Table>> callback) {
    auto readerFunc = [](protocol::reader &reader) -> std::vector<Table> {
        if (reader.try_read_nil())
            return {};

        std::vector<Table> tables;
        tables.reserve(reader.read_map_size());

        reader.read_map<uuid, std::string>([&tables](auto &&id, auto &&name) {
            auto tableImpl = std::make_shared<TableImpl>(std::forward<std::string>(name), std::forward<uuid>(id));
            tables.push_back(Table{tableImpl});
        });

        return tables;
    };

    auto handler =
        std::make_shared<ResponseHandlerImpl<std::vector<Table>>>(std::move(readerFunc), std::move(callback));

    m_connection->performRequest(ClientOperation::TABLES_GET, std::move(handler));
}

} // namespace ignite::detail
