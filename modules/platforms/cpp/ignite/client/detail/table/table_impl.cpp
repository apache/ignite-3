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

void table_impl::load_latest_schema_async(ignite_callback<std::shared_ptr<schema>> callback) {
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
        } catch (ignite_error &err) {
            reload_schema = err.get_flags() & std::int32_t(error_flag::UNMAPPED_COLUMNS_PRESENT);
            if (!reload_schema)
                throw;
        }

        if (!reload_schema) {
            return;
        }
    }

    load_schema_async(std::nullopt, std::move(callback));
}

/**
 * Make a handler function for a case when it may require to update schema to complete operation.
 *
 * @tparam T Result type.
 * @param self Table shared reference.
 * @param uc User callback.
 * @return Handler function.
 */
template<typename T>
std::function<void(ignite_result<bytes_view>)> make_schema_handler_function(std::shared_ptr<table_impl> self,
    ignite_callback<T> uc, std::function<void(protocol::reader &, const schema &, ignite_callback<T>)> &&func) {
    return [self = std::move(self), uc = std::move(uc), rf = std::move(func)](ignite_result<bytes_view> res) mutable {
        if (res.has_error()) {
            uc(std::move(res).error());
            return;
        }

        auto msg = res.value();
        protocol::reader reader(msg);
        auto schema_ver = reader.read_int32();
        std::shared_ptr<schema> sch = self->get_schema(schema_ver);
        if (sch) {
            rf(reader, *sch, std::move(uc));
            return;
        }

        msg.remove_prefix(reader.position());
        std::vector<std::byte> msg_copy(msg);

        self->with_schema_async<T>(schema_ver, std::move(uc),
            [msg = std::move(msg_copy), rf = std::move(rf)](const schema &sch, auto uc) mutable {
                protocol::reader reader(msg);
                rf(reader, sch, std::move(uc));
            });
    };
}

void table_impl::load_schema_async(
    std::optional<std::int32_t> version, ignite_callback<std::shared_ptr<schema>> callback) {
    auto writer_func = [&](protocol::writer &writer) {
        writer.write(m_table_id);

        if (!version) {
            writer.write_nil();
        } else {
            // Number of requested schemas.
            writer.write(1);
            writer.write(*version);
        }
    };

    auto table = shared_from_this();
    auto reader_func = [table](protocol::reader &reader) mutable -> std::shared_ptr<schema> {
        auto schema_cnt = reader.read_int32();
        if (!schema_cnt)
            throw ignite_error("Schema not found");

        std::shared_ptr<schema> sch;
        for (std::int32_t schema_idx = 0; schema_idx < schema_cnt; ++schema_idx) {
            sch = schema::read(reader);
            table->add_schema(sch);
        }

        return sch;
    };

    m_connection->perform_request<std::shared_ptr<schema>>(
        protocol::client_operation::SCHEMAS_GET, writer_func, std::move(reader_func), std::move(callback));
}

void table_impl::get_async(
    transaction *tx, const ignite_tuple &key, ignite_callback<std::optional<ignite_tuple>> callback) {

    with_proper_schema_async<std::optional<ignite_tuple>>(std::move(callback),
        [self = shared_from_this(), key = std::make_shared<ignite_tuple>(key), tx0 = to_impl(tx)](
            const schema &sch, auto callback) mutable {
            auto writer_func = [self, key, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_table_id, tx0.get(), sch);
                write_tuple(writer, sch, *key, true);
            };

            auto handle_func = make_schema_handler_function<std::optional<ignite_tuple>>(
                self, std::move(callback), [](protocol::reader &reader, const schema &sch, auto callback) mutable {
                    callback(read_tuple_opt(reader, &sch));
                });

            self->m_connection->perform_request_raw(
                protocol::client_operation::TUPLE_GET, tx0.get(), writer_func, std::move(handle_func));
        });
}

void table_impl::contains_async(transaction *tx, const ignite_tuple &key, ignite_callback<bool> callback) {

    with_proper_schema_async<bool>(std::move(callback),
        [self = shared_from_this(), key = std::make_shared<ignite_tuple>(key), tx0 = to_impl(tx)](
            const schema &sch, auto callback) mutable {
            auto writer_func = [self, key, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_table_id, tx0.get(), sch);
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
    with_proper_schema_async<std::vector<std::optional<ignite_tuple>>>(std::move(callback),
        [self = shared_from_this(), keys = shared_keys, tx0 = to_impl(tx)](const schema &sch, auto callback) mutable {
            auto writer_func = [self, keys, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_table_id, tx0.get(), sch);
                write_tuples(writer, sch, *keys, true);
            };

            auto handle_func = make_schema_handler_function<std::vector<std::optional<ignite_tuple>>>(
                self, std::move(callback), [](protocol::reader &reader, const schema &sch, auto callback) mutable {
                    callback(read_tuples_opt(reader, &sch, false));
                });

            self->m_connection->perform_request_raw(
                protocol::client_operation::TUPLE_GET_ALL, tx0.get(), writer_func, std::move(handle_func));
        });
}

void table_impl::upsert_async(transaction *tx, const ignite_tuple &record, ignite_callback<void> callback) {
    with_proper_schema_async<void>(std::move(callback),
        [self = shared_from_this(), record = ignite_tuple(record), tx0 = to_impl(tx)](
            const schema &sch, auto callback) mutable {
            auto writer_func = [self, &record, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_table_id, tx0.get(), sch);
                write_tuple(writer, sch, record, false);
            };

            self->m_connection->perform_request_wr(
                protocol::client_operation::TUPLE_UPSERT, tx0.get(), writer_func, std::move(callback));
        });
}

void table_impl::upsert_all_async(transaction *tx, std::vector<ignite_tuple> records, ignite_callback<void> callback) {
    auto shared_records = std::make_shared<std::vector<ignite_tuple>>(std::move(records));
    with_proper_schema_async<void>(std::move(callback),
        [self = shared_from_this(), records = shared_records, tx0 = to_impl(tx)](
            const schema &sch, auto callback) mutable {
            auto writer_func = [self, records, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_table_id, tx0.get(), sch);
                write_tuples(writer, sch, *records, false);
            };

            self->m_connection->perform_request_wr(
                protocol::client_operation::TUPLE_UPSERT_ALL, tx0.get(), writer_func, std::move(callback));
        });
}

void table_impl::get_and_upsert_async(
    transaction *tx, const ignite_tuple &record, ignite_callback<std::optional<ignite_tuple>> callback) {

    with_proper_schema_async<std::optional<ignite_tuple>>(std::move(callback),
        [self = shared_from_this(), record = std::make_shared<ignite_tuple>(record), tx0 = to_impl(tx)](
            const schema &sch, auto callback) {
            auto writer_func = [self, record, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_table_id, tx0.get(), sch);
                write_tuple(writer, sch, *record, false);
            };

            auto handle_func = make_schema_handler_function<std::optional<ignite_tuple>>(
                self, std::move(callback), [](protocol::reader &reader, const schema &sch, auto callback) mutable {
                    callback(read_tuple_opt(reader, &sch));
                });

            self->m_connection->perform_request_raw(
                protocol::client_operation::TUPLE_GET_AND_UPSERT, tx0.get(), writer_func, std::move(handle_func));
        });
}

void table_impl::insert_async(transaction *tx, const ignite_tuple &record, ignite_callback<bool> callback) {
    with_proper_schema_async<bool>(std::move(callback),
        [self = shared_from_this(), record = ignite_tuple(record), tx0 = to_impl(tx)](
            const schema &sch, auto callback) mutable {
            auto writer_func = [self, &record, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_table_id, tx0.get(), sch);
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
    with_proper_schema_async<std::vector<ignite_tuple>>(std::move(callback),
        [self = shared_from_this(), records = shared_records, tx0 = to_impl(tx)](
            const schema &sch, auto callback) mutable {
            auto writer_func = [self, records, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_table_id, tx0.get(), sch);
                write_tuples(writer, sch, *records, false);
            };

            auto handle_func = make_schema_handler_function<std::vector<ignite_tuple>>(
                self, std::move(callback), [](protocol::reader &reader, const schema &sch, auto callback) mutable {
                    callback(read_tuples(reader, &sch, false));
                });

            self->m_connection->perform_request_raw(
                protocol::client_operation::TUPLE_INSERT_ALL, tx0.get(), writer_func, std::move(handle_func));
        });
}

void table_impl::replace_async(transaction *tx, const ignite_tuple &record, ignite_callback<bool> callback) {
    with_proper_schema_async<bool>(std::move(callback),
        [self = shared_from_this(), record = ignite_tuple(record), tx0 = to_impl(tx)](
            const schema &sch, auto callback) mutable {
            auto writer_func = [self, &record, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_table_id, tx0.get(), sch);
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
    with_proper_schema_async<bool>(std::move(callback),
        [self = shared_from_this(), record = ignite_tuple(record), new_record = ignite_tuple(new_record),
            tx0 = to_impl(tx)](const schema &sch, auto callback) mutable {
            auto writer_func = [self, &record, &new_record, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_table_id, tx0.get(), sch);
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

    with_proper_schema_async<std::optional<ignite_tuple>>(std::move(callback),
        [self = shared_from_this(), record = std::make_shared<ignite_tuple>(record), tx0 = to_impl(tx)](
            const schema &sch, auto callback) mutable {
            auto writer_func = [self, record, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_table_id, tx0.get(), sch);
                write_tuple(writer, sch, *record, false);
            };

            auto handle_func = make_schema_handler_function<std::optional<ignite_tuple>>(
                self, std::move(callback), [](protocol::reader &reader, const schema &sch, auto callback) mutable {
                    callback(read_tuple_opt(reader, &sch));
                });

            self->m_connection->perform_request_raw(
                protocol::client_operation::TUPLE_GET_AND_REPLACE, tx0.get(), writer_func, std::move(handle_func));
        });
}

void table_impl::remove_async(transaction *tx, const ignite_tuple &key, ignite_callback<bool> callback) {
    with_proper_schema_async<bool>(std::move(callback),
        [self = shared_from_this(), record = ignite_tuple(key), tx0 = to_impl(tx)](
            const schema &sch, auto callback) mutable {
            auto writer_func = [self, &record, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_table_id, tx0.get(), sch);
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
    with_proper_schema_async<bool>(std::move(callback),
        [self = shared_from_this(), record = ignite_tuple(record), tx0 = to_impl(tx)](
            const schema &sch, auto callback) mutable {
            auto writer_func = [self, &record, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_table_id, tx0.get(), sch);
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

    with_proper_schema_async<std::optional<ignite_tuple>>(std::move(callback),
        [self = shared_from_this(), record = std::make_shared<ignite_tuple>(key), tx0 = to_impl(tx)](
            const schema &sch, auto callback) mutable {
            auto writer_func = [self, record, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_table_id, tx0.get(), sch);
                write_tuple(writer, sch, *record, true);
            };

            auto handle_func = make_schema_handler_function<std::optional<ignite_tuple>>(
                self, std::move(callback), [](protocol::reader &reader, const schema &sch, auto callback) mutable {
                    callback(read_tuple_opt(reader, &sch));
                });

            self->m_connection->perform_request_raw(
                protocol::client_operation::TUPLE_GET_AND_DELETE, tx0.get(), writer_func, std::move(handle_func));
        });
}

void table_impl::remove_all_async(
    transaction *tx, std::vector<ignite_tuple> keys, ignite_callback<std::vector<ignite_tuple>> callback) {

    with_proper_schema_async<std::vector<ignite_tuple>>(std::move(callback),
        [self = shared_from_this(), keys = std::move(keys), tx0 = to_impl(tx)](const schema &sch, auto callback) {
            auto writer_func = [self, &keys, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_table_id, tx0.get(), sch);
                write_tuples(writer, sch, keys, true);
            };

            auto handle_func = make_schema_handler_function<std::vector<ignite_tuple>>(
                self, std::move(callback), [](protocol::reader &reader, const schema &sch, auto callback) mutable {
                    callback(read_tuples(reader, &sch, true));
                });

            self->m_connection->perform_request_raw(
                protocol::client_operation::TUPLE_DELETE_ALL, tx0.get(), writer_func, std::move(handle_func));
        });
}

void table_impl::remove_all_exact_async(
    transaction *tx, std::vector<ignite_tuple> records, ignite_callback<std::vector<ignite_tuple>> callback) {

    with_proper_schema_async<std::vector<ignite_tuple>>(std::move(callback),
        [self = shared_from_this(), records = std::move(records), tx0 = to_impl(tx)](const schema &sch, auto callback) {
            auto writer_func = [self, &records, &sch, &tx0](protocol::writer &writer) {
                write_table_operation_header(writer, self->m_table_id, tx0.get(), sch);
                write_tuples(writer, sch, records, false);
            };

            auto handle_func = make_schema_handler_function<std::vector<ignite_tuple>>(
                self, std::move(callback), [](protocol::reader &reader, const schema &sch, auto callback) mutable {
                    callback(read_tuples(reader, &sch, false));
                });

            self->m_connection->perform_request_raw(
                protocol::client_operation::TUPLE_DELETE_ALL_EXACT, tx0.get(), writer_func, std::move(handle_func));
        });
}

std::shared_ptr<table_impl> table_impl::from_facade(table &tb) {
    return tb.m_impl;
}

} // namespace ignite::detail
