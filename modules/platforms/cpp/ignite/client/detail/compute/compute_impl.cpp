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

#include "ignite/client/detail/compute/compute_impl.h"
#include "ignite/client/detail/utils.h"
#include "ignite/tuple/binary_tuple_builder.h"

namespace ignite::detail {

/**
 * Write a collection of primitives as a binary tuple.
 *
 * @param writer Writer to use.
 * @param args Arguments.
 */
void write_primitives_as_binary_tuple(protocol::writer &writer, const std::vector<primitive> &args) {
    auto args_num = std::int32_t(args.size());

    writer.write(args_num);

    binary_tuple_builder args_builder{args_num * 3};

    args_builder.start();
    for (const auto &arg : args) {
        claim_primitive_with_type(args_builder, arg);
    }

    args_builder.layout();
    for (const auto &arg : args) {
        append_primitive_with_type(args_builder, arg);
    }

    auto args_data = args_builder.build();
    writer.write_binary(args_data);
}

/**
 * Read primitive from a stream, which is encoded as a binary tuple.
 *
 * @param reader Reader.
 * @return Value.
 */
std::optional<primitive> read_primitive_from_binary_tuple(protocol::reader &reader) {
    auto tuple_data = reader.read_binary();
    binary_tuple_parser parser(3, tuple_data);

    auto typ = ignite_type(binary_tuple_parser::get_int32(parser.get_next().value()));
    auto scale = binary_tuple_parser::get_int32(parser.get_next().value());
    return read_next_column(parser, typ, scale);
}

void compute_impl::execute_on_one_node(cluster_node node, std::string_view job_class_name,
    const std::vector<primitive> &args, ignite_callback<std::optional<primitive>> callback) {

    auto writer_func = [&node, job_class_name, args](protocol::writer &writer) {
        writer.write(node.get_name());
        writer.write(job_class_name);
        write_primitives_as_binary_tuple(writer, args);
    };

    auto reader_func = [](protocol::reader &reader) -> std::optional<primitive> {
        if (reader.try_read_nil())
            return std::nullopt;

        return read_primitive_from_binary_tuple(reader);
    };

    m_connection->perform_request<std::optional<primitive>>(
        client_operation::COMPUTE_EXECUTE, writer_func, std::move(reader_func), std::move(callback));
}

void compute_impl::execute_colocated_async(std::string_view table_name, const ignite_tuple &key, std::string_view job,
    const std::vector<primitive> &args, ignite_callback<std::optional<primitive>> callback) {
    m_tables->get_table_async(table_name,
        [table_name = std::string(table_name), callback = std::move(callback), key, job = std::string(job), args,
            conn = m_connection](auto &&res) mutable {
            if (res.has_error()) {
                callback({std::move(res.error())});
                return;
            }
            auto &table_opt = res.value();
            if (!table_opt) {
                callback({ignite_error("Table does not exist: '" + table_name + "'")});
                return;
            }

            auto table = table_impl::from_facade(*table_opt);
            table->template with_latest_schema_async<std::optional<primitive>>(std::move(callback),
                [table, key = std::move(key), job = std::move(job), args = std::move(args),
                    conn] // NOLINT(performance-move-const-arg)
                (const schema &sch, auto callback) mutable {
                    auto writer_func = [&key, &sch, &table, &job, &args](protocol::writer &writer) {
                        writer.write(table->get_id());
                        writer.write(sch.version);
                        write_tuple(writer, sch, key, true);
                        writer.write(job);
                        write_primitives_as_binary_tuple(writer, args);
                    };

                    auto reader_func = [](protocol::reader &reader) -> std::optional<primitive> {
                        if (reader.try_read_nil())
                            return std::nullopt;

                        return read_primitive_from_binary_tuple(reader);
                    };

                    conn->perform_request<std::optional<primitive>>(client_operation::COMPUTE_EXECUTE_COLOCATED,
                        writer_func, std::move(reader_func), std::move(callback));
                });
        });
}

} // namespace ignite::detail
