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

#include "sql_impl.h"

#include "ignite/client/detail/sql/result_set_impl.h"
#include "ignite/client/detail/utils.h"

#include <ignite/tuple/binary_tuple_builder.h>

namespace ignite::detail {

void sql_impl::execute_async(transaction *tx, const sql_statement &statement, std::vector<primitive> &&args,
    ignite_callback<result_set> &&callback) {
    auto tx0 = tx ? tx->m_impl : nullptr;

    auto writer_func = [&statement, &args, &tx0](protocol::writer &writer) {
        if (tx0)
            writer.write(tx0->get_id());
        else
            writer.write_nil();

        writer.write(statement.schema());
        writer.write(statement.page_size());
        writer.write(std::int64_t(statement.timeout().count()));
        writer.write_nil(); // Session timeout (unused, session is closed by the server immediately).

        const auto &properties = statement.properties();
        auto props_num = std::int32_t(properties.size());

        writer.write(props_num);

        binary_tuple_builder prop_builder{props_num * 4};

        prop_builder.start();
        for (const auto &property : properties) {
            prop_builder.claim_varlen(property.first);
            protocol::claim_primitive_with_type(prop_builder, property.second);
        }

        prop_builder.layout();
        for (const auto &property : properties) {
            prop_builder.append_varlen(property.first);
            protocol::append_primitive_with_type(prop_builder, property.second);
        }

        auto prop_data = prop_builder.build();
        writer.write_binary(prop_data);

        writer.write(statement.query());

        if (args.empty()) {
            writer.write_nil();
        } else {
            auto args_num = std::int32_t(args.size());

            writer.write(args_num);

            binary_tuple_builder args_builder{args_num * 3};

            args_builder.start();
            for (const auto &arg : args) {
                protocol::claim_primitive_with_type(args_builder, arg);
            }

            args_builder.layout();
            for (const auto &arg : args) {
                protocol::append_primitive_with_type(args_builder, arg);
            }

            auto args_data = args_builder.build();
            writer.write_binary(args_data);
        }

        // TODO IGNITE-20057 C++ client: Track observable timestamp
        writer.write(0); // observableTimestamp.
    };

    auto reader_func = [](std::shared_ptr<node_connection> channel, bytes_view msg) -> result_set {
        return result_set{std::make_shared<result_set_impl>(std::move(channel), msg)};
    };

    m_connection->perform_request_raw<result_set>(
        client_operation::SQL_EXEC, tx0.get(), writer_func, std::move(reader_func), std::move(callback));
}

} // namespace ignite::detail
