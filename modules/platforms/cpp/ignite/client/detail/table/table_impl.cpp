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

#include "ignite/client/detail/table/table_impl.h"

#include <ignite/protocol/reader.h>
#include <ignite/protocol/writer.h>

namespace ignite::detail {

void table_impl::get_latest_schema_async(const ignite_callback<std::shared_ptr<schema>> &callback) {
    auto latest_schema_version = m_latest_schema_version;

    if (latest_schema_version >= 0) {
        std::shared_ptr<schema> schema;
        {
            std::lock_guard<std::mutex> guard(m_schemas_mutex);
            schema = m_schemas[latest_schema_version];
        }
        callback({std::move(schema)});
        return;
    }

    load_schema_async(callback);
}

void table_impl::load_schema_async(ignite_callback<std::shared_ptr<schema>> callback) {
    auto writer_func = [&](protocol::writer &writer) {
        writer.write(m_id);
        writer.write_nil();
    };

    auto table = shared_from_this();
    auto reader_func = [table](protocol::reader &reader) mutable -> std::shared_ptr<schema> {
        auto schema_cnt = reader.read_array_size();
        if (!schema_cnt)
            throw ignite_error("Schema not found");

        std::shared_ptr<schema> last;
        reader.read_map_raw([&last, &table](const msgpack_object_kv& object) {
            last = schema::read(object);
            table->add_schema(last);
        });

        return std::move(last);
    };

    m_connection->perform_request<std::shared_ptr<schema>>(
            client_operation::TABLE_GET, writer_func, std::move(reader_func), std::move(callback));
}

} // namespace ignite::detail
