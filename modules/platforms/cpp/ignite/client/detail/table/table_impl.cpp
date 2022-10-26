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
#include "ignite/schema/binary_tuple_builder.h"
#include "ignite/schema/binary_tuple_parser.h"
#include "ignite/common/ignite_error.h"

#include <ignite/protocol/reader.h>
#include <ignite/protocol/writer.h>

namespace ignite::detail {

/**
 * Claim space for the column.
 *
 * @param builder Binary tuple builder.
 * @param typ Column type.
 * @param index Tuple field index.
 * @param tuple Tuple.
 */
void claim_column(binary_tuple_builder& builder, ignite_type typ, std::int32_t index, const ignite_tuple& tuple) {
    switch (typ) {
        case ignite_type::INT8:
            builder.claim_int8(tuple.get<std::int8_t>(index));
            break;
        case ignite_type::INT16:
            builder.claim_int16(tuple.get<std::int16_t>(index));
            break;
        case ignite_type::INT32:
            builder.claim_int32(tuple.get<std::int32_t>(index));
            break;
        case ignite_type::INT64:
            builder.claim_int64(tuple.get<std::int64_t>(index));
            break;
        case ignite_type::FLOAT:
            builder.claim_float(tuple.get<float>(index));
            break;
        case ignite_type::DOUBLE:
            builder.claim_double(tuple.get<double>(index));
            break;
        case ignite_type::UUID:
            builder.claim_uuid(tuple.get<uuid>(index));
            break;
        case ignite_type::STRING:
            builder.claim(SizeT(tuple.get<const std::string&>(index).size()));
            break;
        case ignite_type::BINARY:
            builder.claim(SizeT(tuple.get<const std::vector<std::byte>&>(index).size()));
            break;
        default:
            // TODO: Support other types
            throw ignite_error("Type with id " + std::to_string(int(typ)) + " is not yet supported");
    }
}

/**
 * Append column value to binary tuple.
 *
 * @param builder Binary tuple builder.
 * @param typ Column type.
 * @param index Tuple field index.
 * @param tuple Tuple.
 */
void append_column(binary_tuple_builder& builder, ignite_type typ, std::int32_t index, const ignite_tuple& tuple) {
    switch (typ) {
        case ignite_type::INT8:
            builder.append_int8(tuple.get<std::int8_t>(index));
            break;
        case ignite_type::INT16:
            builder.append_int16(tuple.get<std::int16_t>(index));
            break;
        case ignite_type::INT32:
            builder.append_int32(tuple.get<std::int32_t>(index));
            break;
        case ignite_type::INT64:
            builder.append_int64(tuple.get<std::int64_t>(index));
            break;
        case ignite_type::FLOAT:
            builder.append_float(tuple.get<float>(index));
            break;
        case ignite_type::DOUBLE:
            builder.append_double(tuple.get<double>(index));
            break;
        case ignite_type::UUID:
            builder.append_uuid(tuple.get<uuid>(index));
            break;
        case ignite_type::STRING: {
            const auto& str = tuple.get<const std::string&>(index);
            bytes_view view{reinterpret_cast<const std::byte*>(str.data()), str.size()};
            builder.append(typ, view);
            break;
        }
        case ignite_type::BINARY:
            builder.append(typ, tuple.get<const std::vector<std::byte>&>(index));
            break;
        default:
            // TODO: Support other types
            throw ignite_error("Type with id " + std::to_string(int(typ)) + " is not yet supported");
    }
}

/**
 * Read column value from binary tuple.
 *
 * @param parser Binary tuple parser.
 * @param typ Column type.
 * @return Column value.
 */
std::any read_next_column(binary_tuple_parser& parser, ignite_type typ) {
    auto val_opt = parser.get_next();
    if (!val_opt)
        return {};

    auto val = val_opt.value();

    switch (typ) {
        case ignite_type::INT8:
            return binary_tuple_parser::get_int8(val);
        case ignite_type::INT16:
            return binary_tuple_parser::get_int16(val);
        case ignite_type::INT32:
            return binary_tuple_parser::get_int32(val);
        case ignite_type::INT64:
            return binary_tuple_parser::get_int64(val);
        case ignite_type::FLOAT:
            return binary_tuple_parser::get_float(val);
        case ignite_type::DOUBLE:
            return binary_tuple_parser::get_double(val);
        case ignite_type::UUID:
            return binary_tuple_parser::get_uuid(val);
        case ignite_type::STRING:
            return std::string(reinterpret_cast<const char*>(val.data()), val.size());
        case ignite_type::BINARY:
            return std::vector<std::byte>(val);
        default:
            // TODO: Support other types
            throw ignite_error("Type with id " + std::to_string(int(typ)) + " is not yet supported");
    }
}

/**
 * Check transaction and throw an exception if it is not nullptr.
 *
 * @param tx Transaction.
 */
void transactions_not_implemented(transaction* tx) {
    // TODO: IGNITE-17604 Implement transactions
    if (tx)
        throw ignite_error("Transactions are not implemented");
}

/**
 * Serialize tuple using table schema.
 *
 * @param sch Schema.
 * @param tuple Tuple.
 * @param key_only Should only key fields be serialized.
 * @return Serialized binary tuple.
 */
std::vector<std::byte> pack_tuple(const schema& sch, const ignite_tuple& tuple, bool key_only) {
    auto count = std::int32_t(key_only ? sch.key_column_count : sch.columns.size());
    binary_tuple_builder builder{count};

    builder.start();

    for (std::int32_t i = 0; i < count; ++i) {
        const auto& col = sch.columns[i];
        auto col_idx = tuple.column_ordinal(col.name);

        if (col_idx >= 0)
            claim_column(builder, col.type, col_idx, tuple);
        else
            builder.claim(std::nullopt);
    }

    builder.layout();
    // TODO: Re-factor to optimize this
    for (std::int32_t i = 0; i < count; ++i) {
        const auto& col = sch.columns[i];
        auto col_idx = tuple.column_ordinal(col.name);

        if (col_idx >= 0)
            append_column(builder, col.type, col_idx, tuple);
        else
            builder.append(std::nullopt);
    }

    return builder.build();
}

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
            client_operation::SCHEMAS_GET, writer_func, std::move(reader_func), std::move(callback));
}

void table_impl::get_async(transaction *tx, ignite_tuple key, ignite_callback<std::optional<ignite_tuple>> callback) {
    transactions_not_implemented(tx);

    with_latest_schema_async<std::optional<ignite_tuple>>(std::move(callback),
        [this, key = std::move(key)] (const schema& sch, auto callback) mutable {
        auto writer_func = [this, &key, &sch] (protocol::writer &writer) {
            writer.write(m_id);
            writer.write_nil(); // TODO: IGNITE-17604: write transaction ID here
            writer.write(sch.version);

            auto tuple_data = pack_tuple(sch, key, true);
            writer.write_binary(tuple_data);
        };

        auto self = shared_from_this();
        auto reader_func = [self = std::move(self), key = std::move(key)] (protocol::reader &reader) -> std::optional<ignite_tuple> {
            auto schema_version = reader.read_object_nullable<std::int32_t>();
            std::shared_ptr<schema> sch;
            if (schema_version)
                sch = self->get_schema(schema_version.value());

            if (!sch)
                return std::nullopt;

            auto tuple_data = reader.read_binary();

            auto columns_cnt = std::int32_t(sch->columns.size());
            ignite_tuple res(columns_cnt);
            binary_tuple_parser parser(columns_cnt - sch->key_column_count, tuple_data);

            for (std::int32_t i = 0; i < columns_cnt; ++i) {
                auto& column = sch->columns[i];
                if (i < sch->key_column_count) {
                    res.set(column.name, key.get(column.name));
                } else {
                    res.set(column.name, read_next_column(parser, column.type));
                }
            }

            return res;
        };

        m_connection->perform_request<std::optional<ignite_tuple>>(
                client_operation::TUPLE_GET, writer_func, std::move(reader_func), std::move(callback));
    });
}

} // namespace ignite::detail
