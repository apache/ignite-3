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

#include "table_impl.h"

#include "ignite/client/detail/transaction/transaction_impl.h"
#include "ignite/client/detail/utils.h"
#include "ignite/client/table/table.h"

#include "ignite/client/detail/client_error_flags.h"
#include "ignite/common/ignite_error.h"
#include "ignite/protocol/bitset_span.h"
#include "ignite/protocol/reader.h"
#include "ignite/protocol/writer.h"
#include "ignite/tuple/binary_tuple_parser.h"

namespace ignite::detail {

/**
 * Write table operation header.
 *
 * @param writer Writer.
 * @param id Table ID.
 * @param sch Table schema.
 */
void write_table_operation_header(protocol::writer &writer, std::int32_t id, transaction_impl *tx, const schema &sch) {
    writer.write(id);

    if (!tx)
        writer.write_nil();
    else
        writer.write(tx->get_id());

    writer.write(sch.version);
}

/**
 * Read tuple.
 *
 * @param reader Reader.
 * @param sch Schema.
 * @return Tuple.
 */
ignite_tuple read_tuple(protocol::reader &reader, const schema *sch) {
    auto tuple_data = reader.read_binary();

    auto columns_cnt = std::int32_t(sch->columns.size());
    ignite_tuple res(columns_cnt);
    binary_tuple_parser parser(columns_cnt, tuple_data);

    for (std::int32_t i = 0; i < columns_cnt; ++i) {
        auto &column = sch->columns[i];
        res.set(column.name, protocol::read_next_column(parser, column.type, column.scale));
    }
    return res;
}

/**
 * Read tuple.
 *
 * @param reader Reader.
 * @param sch Schema.
 * @param key_only Should only key fields be read or not.
 * @return Tuple.
 */
ignite_tuple read_tuple(protocol::reader &reader, const schema *sch, bool key_only) {
    auto tuple_data = reader.read_binary();

    auto columns_cnt = std::int32_t(key_only ? sch->key_column_count : sch->columns.size());
    ignite_tuple res(columns_cnt);
    binary_tuple_parser parser(columns_cnt, tuple_data);

    for (std::int32_t i = 0; i < columns_cnt; ++i) {
        auto &column = sch->columns[i];
        res.set(column.name, protocol::read_next_column(parser, column.type, column.scale));
    }
    return res;
}

/**
 * Read tuples.
 *
 * @param reader Reader.
 * @param sch Schema.
 * @param key_only Should only key fields be read or not.
 * @return Tuples.
 */
std::vector<std::optional<ignite_tuple>> read_tuples_opt(protocol::reader &reader, const schema *sch, bool key_only) {
    if (reader.try_read_nil())
        return {};

    auto count = reader.read_int32();
    std::vector<std::optional<ignite_tuple>> res;
    res.reserve(std::size_t(count));

    for (std::int32_t i = 0; i < count; ++i) {
        auto exists = reader.read_bool();
        if (!exists)
            res.emplace_back(std::nullopt);
        else
            res.emplace_back(read_tuple(reader, sch, key_only));
    }

    return res;
}

/**
 * Read tuples.
 *
 * @param reader Reader.
 * @param sch Schema.
 * @param key_only Should only key fields be read or not.
 * @return Tuples.
 */
std::vector<ignite_tuple> read_tuples(protocol::reader &reader, const schema *sch, bool key_only) {
    if (reader.try_read_nil())
        return {};

    auto count = reader.read_int32();
    std::vector<ignite_tuple> res;
    res.reserve(std::size_t(count));

    for (std::int32_t i = 0; i < count; ++i)
        res.emplace_back(read_tuple(reader, sch, key_only));

    return res;
}

void table_impl::get_latest_schema_async(ignite_callback<std::shared_ptr<schema>> callback) {
    auto latest_schema_version = m_latest_schema_version;

    if (latest_schema_version >= 0) {
        std::shared_ptr<schema> schema;
        {
            std::lock_guard<std::mutex> guard(m_schemas_mutex);
            schema = m_schemas[latest_schema_version];
        }

        bool reload_schema = false;
        try {
            callback({std::move(schema)});
        }
        catch (ignite_error& err) {
            reload_schema = err.get_flags() & std::int32_t(error_flag::UNMAPPED_COLUMNS_PRESENT);
            if (!reload_schema)
                throw;
        }

        if (!reload_schema) {
            return;
        }
    }

    load_schema_async(std::move(callback));
}

void table_impl::load_schema_async(ignite_callback<std::shared_ptr<schema>> callback) {
    auto writer_func = [&](protocol::writer &writer) {
        writer.write(m_id);
        writer.write_nil();
    };

    auto table = shared_from_this();
    auto reader_func = [table](protocol::reader &reader) mutable -> std::shared_ptr<schema> {
        auto schema_cnt = reader.read_int32();
        if (!schema_cnt)
            throw ignite_error("Schema not found");

        std::shared_ptr<schema> last;
        for (std::int32_t schema_idx = 0; schema_idx < schema_cnt; ++schema_idx) {
            last = schema::read(reader);
            table->add_schema(last);
        }

        return last;
    };

    m_connection->perform_request<std::shared_ptr<schema>>(
        protocol::client_operation::SCHEMAS_GET, writer_func, std::move(reader_func), std::move(callback));
}

void table_impl::get_async(
    transaction *tx, const ignite_tuple &key, ignite_callback<std::optional<ignite_tuple>> callback) {

    with_latest_schema_async<std::optional<ignite_tuple>>(std::move(callback),
        [self = shared_from_this(), key = std::make_shared<ignite_tuple>(key), tx0 = to_impl(tx)](
            const schema &sch, auto callback) mutable {
            auto writer_func = [self, key, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_id, tx0.get(), sch);
                write_tuple(writer, sch, *key, true);
            };

            auto reader_func = [self, key](protocol::reader &reader) -> std::optional<ignite_tuple> {
                std::shared_ptr<schema> sch = self->get_schema(reader);

                if (reader.try_read_nil())
                    return std::nullopt;

                return read_tuple(reader, sch.get());
            };

            self->m_connection->perform_request<std::optional<ignite_tuple>>(protocol::client_operation::TUPLE_GET,
                tx0.get(), writer_func, std::move(reader_func), std::move(callback));
        });
}

void table_impl::contains_async(transaction *tx, const ignite_tuple &key, ignite_callback<bool> callback) {

    with_latest_schema_async<bool>(std::move(callback),
        [self = shared_from_this(), key = std::make_shared<ignite_tuple>(key), tx0 = to_impl(tx)](
            const schema &sch, auto callback) mutable {
            auto writer_func = [self, key, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_id, tx0.get(), sch);
                write_tuple(writer, sch, *key, true);
            };

            auto reader_func = [](protocol::reader &reader) -> bool {
                (void) reader.read_int32(); // Skip schema version.

                return reader.read_bool();
            };

            self->m_connection->perform_request<bool>(protocol::client_operation::TUPLE_CONTAINS_KEY, tx0.get(),
                writer_func, std::move(reader_func), std::move(callback));
        });
}

void table_impl::get_all_async(transaction *tx, std::vector<ignite_tuple> keys,
    ignite_callback<std::vector<std::optional<ignite_tuple>>> callback) {

    auto shared_keys = std::make_shared<std::vector<ignite_tuple>>(std::move(keys));
    with_latest_schema_async<std::vector<std::optional<ignite_tuple>>>(std::move(callback),
        [self = shared_from_this(), keys = shared_keys, tx0 = to_impl(tx)](const schema &sch, auto callback) mutable {
            auto writer_func = [self, keys, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_id, tx0.get(), sch);
                write_tuples(writer, sch, *keys, true);
            };

            auto reader_func = [self](protocol::reader &reader) -> std::vector<std::optional<ignite_tuple>> {
                std::shared_ptr<schema> sch = self->get_schema(reader);
                return read_tuples_opt(reader, sch.get(), false);
            };

            self->m_connection->perform_request<std::vector<std::optional<ignite_tuple>>>(
                protocol::client_operation::TUPLE_GET_ALL, tx0.get(), writer_func, std::move(reader_func),
                std::move(callback));
        });
}

void table_impl::upsert_async(transaction *tx, const ignite_tuple &record, ignite_callback<void> callback) {
    with_latest_schema_async<void>(std::move(callback),
        [self = shared_from_this(), record = ignite_tuple(record), tx0 = to_impl(tx)](
            const schema &sch, auto callback) mutable {
            auto writer_func = [self, &record, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_id, tx0.get(), sch);
                write_tuple(writer, sch, record, false);
            };

            self->m_connection->perform_request_wr(
                protocol::client_operation::TUPLE_UPSERT, tx0.get(), writer_func, std::move(callback));
        });
}

void table_impl::upsert_all_async(transaction *tx, std::vector<ignite_tuple> records, ignite_callback<void> callback) {
    auto shared_records = std::make_shared<std::vector<ignite_tuple>>(std::move(records));
    with_latest_schema_async<void>(std::move(callback),
        [self = shared_from_this(), records = shared_records, tx0 = to_impl(tx)](
            const schema &sch, auto callback) mutable {
            auto writer_func = [self, records, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_id, tx0.get(), sch);
                write_tuples(writer, sch, *records, false);
            };

            self->m_connection->perform_request_wr(
                protocol::client_operation::TUPLE_UPSERT_ALL, tx0.get(), writer_func, std::move(callback));
        });
}

void table_impl::get_and_upsert_async(
    transaction *tx, const ignite_tuple &record, ignite_callback<std::optional<ignite_tuple>> callback) {

    with_latest_schema_async<std::optional<ignite_tuple>>(std::move(callback),
        [self = shared_from_this(), record = std::make_shared<ignite_tuple>(record), tx0 = to_impl(tx)](
            const schema &sch, auto callback) {
            auto writer_func = [self, record, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_id, tx0.get(), sch);
                write_tuple(writer, sch, *record, false);
            };

            auto reader_func = [self, record](protocol::reader &reader) -> std::optional<ignite_tuple> {
                std::shared_ptr<schema> sch = self->get_schema(reader);

                if (reader.try_read_nil())
                    return std::nullopt;

                return read_tuple(reader, sch.get());
            };

            self->m_connection->perform_request<std::optional<ignite_tuple>>(
                protocol::client_operation::TUPLE_GET_AND_UPSERT, tx0.get(), writer_func, std::move(reader_func),
                std::move(callback));
        });
}

void table_impl::insert_async(transaction *tx, const ignite_tuple &record, ignite_callback<bool> callback) {
    with_latest_schema_async<bool>(std::move(callback),
        [self = shared_from_this(), record = ignite_tuple(record), tx0 = to_impl(tx)](
            const schema &sch, auto callback) mutable {
            auto writer_func = [self, &record, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_id, tx0.get(), sch);
                write_tuple(writer, sch, record, false);
            };

            auto reader_func = [](protocol::reader &reader) -> bool {
                (void) reader.read_int32(); // Skip schema version.

                return reader.read_bool();
            };

            self->m_connection->perform_request<bool>(protocol::client_operation::TUPLE_INSERT, tx0.get(), writer_func,
                std::move(reader_func), std::move(callback));
        });
}

void table_impl::insert_all_async(
    transaction *tx, std::vector<ignite_tuple> records, ignite_callback<std::vector<ignite_tuple>> callback) {

    auto shared_records = std::make_shared<std::vector<ignite_tuple>>(std::move(records));
    with_latest_schema_async<std::vector<ignite_tuple>>(std::move(callback),
        [self = shared_from_this(), records = shared_records, tx0 = to_impl(tx)](
            const schema &sch, auto callback) mutable {
            auto writer_func = [self, records, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_id, tx0.get(), sch);
                write_tuples(writer, sch, *records, false);
            };

            auto reader_func = [self, records](protocol::reader &reader) -> std::vector<ignite_tuple> {
                std::shared_ptr<schema> sch = self->get_schema(reader);
                return read_tuples(reader, sch.get(), false);
            };

            self->m_connection->perform_request<std::vector<ignite_tuple>>(protocol::client_operation::TUPLE_INSERT_ALL,
                tx0.get(), writer_func, std::move(reader_func), std::move(callback));
        });
}

void table_impl::replace_async(transaction *tx, const ignite_tuple &record, ignite_callback<bool> callback) {
    with_latest_schema_async<bool>(std::move(callback),
        [self = shared_from_this(), record = ignite_tuple(record), tx0 = to_impl(tx)](
            const schema &sch, auto callback) mutable {
            auto writer_func = [self, &record, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_id, tx0.get(), sch);
                write_tuple(writer, sch, record, false);
            };

            auto reader_func = [](protocol::reader &reader) -> bool {
                (void) reader.read_int32(); // Skip schema version.

                return reader.read_bool();
            };

            self->m_connection->perform_request<bool>(protocol::client_operation::TUPLE_REPLACE, tx0.get(), writer_func,
                std::move(reader_func), std::move(callback));
        });
}

void table_impl::replace_async(
    transaction *tx, const ignite_tuple &record, const ignite_tuple &new_record, ignite_callback<bool> callback) {
    with_latest_schema_async<bool>(std::move(callback),
        [self = shared_from_this(), record = ignite_tuple(record), new_record = ignite_tuple(new_record),
            tx0 = to_impl(tx)](const schema &sch, auto callback) mutable {
            auto writer_func = [self, &record, &new_record, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_id, tx0.get(), sch);
                write_tuple(writer, sch, record, false);
                write_tuple(writer, sch, new_record, false);
            };

            auto reader_func = [](protocol::reader &reader) -> bool {
                (void) reader.read_int32(); // Skip schema version.

                return reader.read_bool();
            };

            self->m_connection->perform_request<bool>(protocol::client_operation::TUPLE_REPLACE_EXACT, tx0.get(),
                writer_func, std::move(reader_func), std::move(callback));
        });
}

void table_impl::get_and_replace_async(
    transaction *tx, const ignite_tuple &record, ignite_callback<std::optional<ignite_tuple>> callback) {

    with_latest_schema_async<std::optional<ignite_tuple>>(std::move(callback),
        [self = shared_from_this(), record = std::make_shared<ignite_tuple>(record), tx0 = to_impl(tx)](
            const schema &sch, auto callback) mutable {
            auto writer_func = [self, record, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_id, tx0.get(), sch);
                write_tuple(writer, sch, *record, false);
            };

            auto reader_func = [self, record](protocol::reader &reader) -> std::optional<ignite_tuple> {
                std::shared_ptr<schema> sch = self->get_schema(reader);

                if (reader.try_read_nil())
                    return std::nullopt;

                return read_tuple(reader, sch.get());
            };

            self->m_connection->perform_request<std::optional<ignite_tuple>>(
                protocol::client_operation::TUPLE_GET_AND_REPLACE, tx0.get(), writer_func, std::move(reader_func),
                std::move(callback));
        });
}

void table_impl::remove_async(transaction *tx, const ignite_tuple &key, ignite_callback<bool> callback) {
    with_latest_schema_async<bool>(std::move(callback),
        [self = shared_from_this(), record = ignite_tuple(key), tx0 = to_impl(tx)](
            const schema &sch, auto callback) mutable {
            auto writer_func = [self, &record, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_id, tx0.get(), sch);
                write_tuple(writer, sch, record, true);
            };

            auto reader_func = [](protocol::reader &reader) -> bool {
                (void) reader.read_int32(); // Skip schema version.

                return reader.read_bool();
            };

            self->m_connection->perform_request<bool>(protocol::client_operation::TUPLE_DELETE, tx0.get(), writer_func,
                std::move(reader_func), std::move(callback));
        });
}

void table_impl::remove_exact_async(transaction *tx, const ignite_tuple &record, ignite_callback<bool> callback) {
    with_latest_schema_async<bool>(std::move(callback),
        [self = shared_from_this(), record = ignite_tuple(record), tx0 = to_impl(tx)](
            const schema &sch, auto callback) mutable {
            auto writer_func = [self, &record, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_id, tx0.get(), sch);
                write_tuple(writer, sch, record, false);
            };

            auto reader_func = [](protocol::reader &reader) -> bool {
                (void) reader.read_int32(); // Skip schema version.

                return reader.read_bool();
            };

            self->m_connection->perform_request<bool>(protocol::client_operation::TUPLE_DELETE_EXACT, tx0.get(),
                writer_func, std::move(reader_func), std::move(callback));
        });
}

void table_impl::get_and_remove_async(
    transaction *tx, const ignite_tuple &key, ignite_callback<std::optional<ignite_tuple>> callback) {

    with_latest_schema_async<std::optional<ignite_tuple>>(std::move(callback),
        [self = shared_from_this(), record = std::make_shared<ignite_tuple>(key), tx0 = to_impl(tx)](
            const schema &sch, auto callback) mutable {
            auto writer_func = [self, record, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_id, tx0.get(), sch);
                write_tuple(writer, sch, *record, true);
            };

            auto reader_func = [self, record](protocol::reader &reader) -> std::optional<ignite_tuple> {
                std::shared_ptr<schema> sch = self->get_schema(reader);

                if (reader.try_read_nil())
                    return std::nullopt;

                return read_tuple(reader, sch.get());
            };

            self->m_connection->perform_request<std::optional<ignite_tuple>>(
                protocol::client_operation::TUPLE_GET_AND_DELETE, tx0.get(), writer_func, std::move(reader_func),
                std::move(callback));
        });
}

void table_impl::remove_all_async(
    transaction *tx, std::vector<ignite_tuple> keys, ignite_callback<std::vector<ignite_tuple>> callback) {

    with_latest_schema_async<std::vector<ignite_tuple>>(std::move(callback),
        [self = shared_from_this(), keys = std::move(keys), tx0 = to_impl(tx)](const schema &sch, auto callback) {
            auto writer_func = [self, &keys, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_id, tx0.get(), sch);
                write_tuples(writer, sch, keys, true);
            };

            auto reader_func = [self](protocol::reader &reader) -> std::vector<ignite_tuple> {
                std::shared_ptr<schema> sch = self->get_schema(reader);
                return read_tuples(reader, sch.get(), true);
            };

            self->m_connection->perform_request<std::vector<ignite_tuple>>(protocol::client_operation::TUPLE_DELETE_ALL,
                tx0.get(), writer_func, std::move(reader_func), std::move(callback));
        });
}

void table_impl::remove_all_exact_async(
    transaction *tx, std::vector<ignite_tuple> records, ignite_callback<std::vector<ignite_tuple>> callback) {

    with_latest_schema_async<std::vector<ignite_tuple>>(std::move(callback),
        [self = shared_from_this(), records = std::move(records), tx0 = to_impl(tx)](const schema &sch, auto callback) {
            auto writer_func = [self, &records, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_id, tx0.get(), sch);
                write_tuples(writer, sch, records, false);
            };

            auto reader_func = [self](protocol::reader &reader) -> std::vector<ignite_tuple> {
                std::shared_ptr<schema> sch = self->get_schema(reader);
                return read_tuples(reader, sch.get(), false);
            };

            self->m_connection->perform_request<std::vector<ignite_tuple>>(
                protocol::client_operation::TUPLE_DELETE_ALL_EXACT, tx0.get(), writer_func, std::move(reader_func),
                std::move(callback));
        });
}

std::shared_ptr<table_impl> table_impl::from_facade(table &tb) {
    return tb.m_impl;
}

} // namespace ignite::detail
