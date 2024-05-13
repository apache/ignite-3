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

#include "ignite/odbc/query/data_query.h"
#include "ignite/odbc/log.h"
#include "ignite/odbc/odbc_error.h"
#include "ignite/odbc/query/cursor.h"
#include "ignite/tuple/binary_tuple_builder.h"

#include <memory>
#include <utility>

namespace {
using namespace ignite;

// TODO: IGNITE-19968 Avoid storing row columns in primitives, read them directly from binary tuple.
/**
 * Put a primitive into a buffer.
 *
 * @param buffer ODBC buffer.
 * @param value Value to put.
 * @return Conversion result.
 */
conversion_result put_primitive_to_buffer(application_data_buffer &buffer, const primitive &value) {
    if (value.is_null())
        return buffer.put_null();

    switch (value.get_type()) {
        case ignite_type::STRING:
            return buffer.put_string(value.get<std::string>());

        case ignite_type::INT8:
            return buffer.put_int8(value.get<std::int8_t>());

        case ignite_type::INT16:
            return buffer.put_int16(value.get<std::int16_t>());

        case ignite_type::INT32:
            return buffer.put_int32(value.get<std::int32_t>());

        case ignite_type::INT64:
            return buffer.put_int64(value.get<std::int64_t>());

        case ignite_type::DECIMAL:
            return buffer.put_decimal(value.get<big_decimal>());

        case ignite_type::FLOAT:
            return buffer.put_float(value.get<float>());

        case ignite_type::DOUBLE:
            return buffer.put_double(value.get<double>());

        case ignite_type::BOOLEAN:
            return buffer.put_bool(value.get<bool>());

        case ignite_type::UUID:
            return buffer.put_uuid(value.get<uuid>());

        case ignite_type::DATE:
            return buffer.put_date(value.get<ignite_date>());

        case ignite_type::TIMESTAMP:
            return buffer.put_timestamp(value.get<ignite_timestamp>());

        case ignite_type::TIME:
            return buffer.put_time(value.get<ignite_time>());

        case ignite_type::DATETIME:
            return buffer.put_date_time(value.get<ignite_date_time>());

        case ignite_type::BITMASK:
            return buffer.put_bitmask(value.get<bit_array>());

        case ignite_type::BYTE_ARRAY:
            return buffer.put_binary_data(value.get<std::vector<std::byte>>());

        case ignite_type::PERIOD:
        case ignite_type::DURATION:
        case ignite_type::NUMBER:
        default:
            // TODO: IGNITE-19969 implement support for period, duration and big_integer
            return conversion_result::AI_UNSUPPORTED_CONVERSION;
    }
}

} // anonymous namespace

namespace ignite {

data_query::data_query(diagnosable_adapter &m_diag, sql_connection &m_connection, std::string sql,
    const parameter_set &params, std::int32_t &timeout)
    : query(m_diag, query_type::DATA)
    , m_connection(m_connection)
    , m_query(std::move(sql))
    , m_params(params)
    , m_timeout(timeout) {
}

data_query::~data_query() {
    internal_close();
}

sql_result data_query::execute() {
    internal_close();

    return make_request_execute();
}

const column_meta_vector *data_query::get_meta() {
    if (!m_result_meta_available) {
        update_meta();

        if (!m_result_meta_available)
            return nullptr;
    }

    return &m_result_meta;
}

const sql_parameter *data_query::get_sql_param(std::int16_t idx) {
    if (idx > 0 && static_cast<std::size_t>(idx) <= m_params_meta.size())
        return &m_params_meta.at(idx - 1);

    return nullptr;
}

sql_result data_query::fetch_next_row(column_binding_map &column_bindings) {
    if (!m_executed) {
        m_diag.add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query was not executed.");

        return sql_result::AI_ERROR;
    }

    if (!m_has_rowset || !m_cursor)
        return sql_result::AI_NO_DATA;

    m_cursor->next(m_result_meta);

    if (!has_more_rows())
        return sql_result::AI_NO_DATA;

    if (!m_cursor->has_data()) {
        std::unique_ptr<result_page> page;
        auto result = make_request_fetch(page);

        if (result != sql_result::AI_SUCCESS)
            return result;

        m_cursor->update_data(std::move(page));
        m_cursor->next(m_result_meta);
    }

    if (!m_cursor->has_data())
        return sql_result::AI_NO_DATA;

    auto row = m_cursor->get_row();
    assert(!row.empty());

    for (std::size_t i = 0; i < row.size(); ++i) {
        // Column indexing starts from 1 in ODBC.
        auto column_idx = std::int32_t(i + 1);
        auto it = column_bindings.find(column_idx);
        if (it == column_bindings.end())
            continue;

        auto conv_res = put_primitive_to_buffer(it->second, row[i]);
        auto result = process_conversion_result(conv_res, m_cursor->get_result_set_pos(), column_idx);

        if (result == sql_result::AI_ERROR)
            return sql_result::AI_ERROR;
    }

    return sql_result::AI_SUCCESS;
}

sql_result data_query::get_column(std::uint16_t column_idx, application_data_buffer &buffer) {
    if (!m_executed) {
        m_diag.add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query was not executed.");

        return sql_result::AI_ERROR;
    }

    if (!m_has_rowset || !has_more_rows() || !m_cursor)
        return sql_result::AI_NO_DATA;

    auto row = m_cursor->get_row();
    if (row.empty()) {
        m_diag.add_status_record(sql_state::S24000_INVALID_CURSOR_STATE,
            "Cursor is in a wrong position. "
            "It is either have reached the end of the result set or no data was yet fetched.");

        return sql_result::AI_ERROR;
    }

    auto conv_res = put_primitive_to_buffer(buffer, row[column_idx - 1]);
    sql_result result = process_conversion_result(conv_res, m_cursor->get_result_set_pos(), column_idx);

    return result;
}

sql_result data_query::close() {
    return internal_close();
}

sql_result data_query::internal_close() {
    if (!m_cursor)
        return sql_result::AI_SUCCESS;

    sql_result result = sql_result::AI_SUCCESS;

    if (!is_closed_remotely())
        result = make_request_close();

    if (result == sql_result::AI_SUCCESS) {
        m_cursor.reset();
        m_rows_affected = -1;
        m_executed = false;
    }

    return result;
}

bool data_query::is_data_available() const {
    return m_has_more_pages || (m_cursor && m_cursor->has_data());
}

std::int64_t data_query::affected_rows() const {
    return m_rows_affected;
}

sql_result data_query::next_result_set() {
    // TODO: IGNITE-19855 Multiple queries execution is not supported.
    internal_close();
    return sql_result::AI_NO_DATA;
}

sql_result data_query::make_request_execute() {
    auto &schema = m_connection.get_schema();

    auto success = m_diag.catch_errors([&] {
        auto tx = m_connection.get_transaction_id();
        if (!tx && !m_connection.is_auto_commit()) {
            // Starting transaction if it's not started already.
            m_connection.transaction_start();

            tx = m_connection.get_transaction_id();
            assert(tx);
        }

        bool single = m_params.get_param_set_size() <= 1;
        auto client_op = single ? protocol::client_operation::SQL_EXEC : protocol::client_operation::SQL_EXEC_BATCH;

        auto response = m_connection.sync_request(client_op, [&](protocol::writer &writer) {
            if (tx)
                writer.write(*tx);
            else
                writer.write_nil();

            writer.write(schema);
            writer.write(m_connection.get_configuration().get_page_size().get_value());
            writer.write(std::int64_t(m_connection.get_timeout()) * 1000);
            writer.write_nil(); // Session timeout (unused, session is closed by the server immediately).
            writer.write_nil(); // TODO: IGNITE-21605 Time zone id.

            // Properties are not used for now.
            writer.write(0);
            binary_tuple_builder prop_builder{0};
            prop_builder.start();
            prop_builder.layout();
            auto prop_data = prop_builder.build();
            writer.write_binary(prop_data);

            writer.write(m_query);

            if (single) {
                m_params.write(writer);
            } else {
                m_params.write(writer, 0, m_params.get_param_set_size(), true);
            }

            writer.write(m_connection.get_observable_timestamp());
        });

        m_connection.mark_transaction_non_empty();

        auto reader = std::make_unique<protocol::reader>(response.get_bytes_view());
        m_query_id = reader->read_object_nullable<std::int64_t>();

        m_has_rowset = reader->read_bool();
        m_has_more_pages = reader->read_bool();
        m_was_applied = reader->read_bool();
        if (single) {
            m_rows_affected = reader->read_int64();

            if (m_has_rowset) {
                auto columns = read_result_set_meta(*reader);
                set_resultset_meta(std::move(columns));
                auto page = std::make_unique<result_page>(std::move(response), std::move(reader));
                m_cursor = std::make_unique<cursor>(std::move(page));
            }

            m_executed = true;
        } else {
            auto affected_rows = reader->read_int64_array();
            auto status_ptr = m_params.get_params_status_ptr();

            m_rows_affected = 0;
            for (auto &ar : affected_rows) {
                m_rows_affected += ar;
            }
            m_params.set_params_processed(affected_rows.size());

            if (status_ptr) {
                for (auto i = 0; i < m_params.get_param_set_size(); i++) {
                    status_ptr[i] = (std::size_t(i) < affected_rows.size()) ? SQL_PARAM_SUCCESS : SQL_PARAM_ERROR;
                }
            }

            // Batch query, set attribute if it's set
            if (auto affected = m_params.get_params_processed_ptr(); affected) {
                *affected = m_rows_affected;
            }

            m_executed = true;

            // Check error if this is a batch query
            if (auto error_code = reader->read_int16_nullable(); error_code) {
                auto error_message = reader->read_string();
                throw odbc_error(error_code_to_sql_state(error::code(error_code.value())), error_message);
            } else {
                reader->skip(); // error message
            }
        }
    });

    return success ? sql_result::AI_SUCCESS : sql_result::AI_ERROR;
}

sql_result data_query::make_request_close() {
    if (!m_query_id)
        return sql_result::AI_SUCCESS;

    LOG_MSG("Closing cursor: " << *m_query_id);

    auto success = m_diag.catch_errors([&] {
        UNUSED_VALUE m_connection.sync_request(
            protocol::client_operation::SQL_CURSOR_CLOSE, [&](protocol::writer &writer) { writer.write(*m_query_id); });
    });

    return success ? sql_result::AI_SUCCESS : sql_result::AI_ERROR;
}

sql_result data_query::make_request_fetch(std::unique_ptr<result_page> &page) {
    if (!m_query_id) {
        m_diag.add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Cursor already closed");
        return sql_result::AI_ERROR;
    }

    network::data_buffer_owning response;
    auto success = m_diag.catch_errors([&] {
        response = m_connection.sync_request(protocol::client_operation::SQL_CURSOR_NEXT_PAGE,
            [&](protocol::writer &writer) { writer.write(*m_query_id); });

        auto reader = std::make_unique<protocol::reader>(response.get_bytes_view());
        page = std::make_unique<result_page>(std::move(response), std::move(reader));
    });

    return success ? sql_result::AI_SUCCESS : sql_result::AI_ERROR;
}

sql_result data_query::update_meta() {
    auto &schema = m_connection.get_schema();

    auto success = m_diag.catch_errors([&] {
        auto tx = m_connection.get_transaction_id();
        auto response =
            m_connection.sync_request(protocol::client_operation::SQL_QUERY_META, [&](protocol::writer &writer) {
                if (tx)
                    writer.write(*tx);
                else
                    writer.write_nil();

                writer.write(schema);
                writer.write(m_query);
            });

        if (tx) {
            m_connection.mark_transaction_non_empty();
        }

        auto reader = std::make_unique<protocol::reader>(response.get_bytes_view());
        auto num = reader->read_int32();

        if (num < 0) {
            throw odbc_error(
                sql_state::SHY000_GENERAL_ERROR, "Unexpected number of parameters: " + std::to_string(num));
        }

        std::vector<sql_parameter> params;
        params.reserve(num);

        for (std::int32_t i = 0; i < num; ++i) {
            sql_parameter param{};
            param.nullable = reader->read_bool();
            param.data_type = ignite_type(reader->read_int32());
            param.scale = reader->read_int32();
            param.precision = reader->read_int32();

            params.emplace_back(param);
        }

        set_params_meta(std::move(params));

        auto columns = read_result_set_meta(*reader);
        set_resultset_meta(std::move(columns));
    });

    return success ? sql_result::AI_SUCCESS : sql_result::AI_ERROR;
}

sql_result data_query::process_conversion_result(
    conversion_result conv_res, std::int32_t row_idx, std::int32_t column_idx) {
    switch (conv_res) {
        case conversion_result::AI_SUCCESS: {
            return sql_result::AI_SUCCESS;
        }

        case conversion_result::AI_NO_DATA: {
            return sql_result::AI_NO_DATA;
        }

        case conversion_result::AI_VARLEN_DATA_TRUNCATED: {
            m_diag.add_status_record(sql_state::S01004_DATA_TRUNCATED,
                "Buffer is too small for the column data. Truncated from the right.", row_idx, column_idx);

            return sql_result::AI_SUCCESS_WITH_INFO;
        }

        case conversion_result::AI_FRACTIONAL_TRUNCATED: {
            m_diag.add_status_record(sql_state::S01S07_FRACTIONAL_TRUNCATION,
                "Buffer is too small for the column data. Fraction truncated.", row_idx, column_idx);

            return sql_result::AI_SUCCESS_WITH_INFO;
        }

        case conversion_result::AI_INDICATOR_NEEDED: {
            m_diag.add_status_record(sql_state::S22002_INDICATOR_NEEDED,
                "Indicator is needed but not supplied for the column buffer.", row_idx, column_idx);

            return sql_result::AI_SUCCESS_WITH_INFO;
        }

        case conversion_result::AI_UNSUPPORTED_CONVERSION: {
            m_diag.add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                "Data conversion is not supported.", row_idx, column_idx);

            return sql_result::AI_SUCCESS_WITH_INFO;
        }

        case conversion_result::AI_FAILURE:
        default: {
            m_diag.add_status_record(
                sql_state::S01S01_ERROR_IN_ROW, "Can not retrieve row column.", row_idx, column_idx);

            break;
        }
    }

    return sql_result::AI_ERROR;
}

void data_query::set_resultset_meta(column_meta_vector value) {
    m_result_meta = std::move(value);
    m_result_meta_available = true;

    for (size_t i = 0; i < m_result_meta.size(); ++i) {
        column_meta &meta = m_result_meta.at(i);
        LOG_MSG("[" << i << "] SchemaName: " << meta.get_schema_name());
        LOG_MSG("[" << i << "] TableName:  " << meta.get_table_name());
        LOG_MSG("[" << i << "] ColumnName: " << meta.get_column_name());
        LOG_MSG("[" << i << "] ColumnType: " << static_cast<int32_t>(meta.get_data_type()));
    }
}

void data_query::set_params_meta(std::vector<sql_parameter> value) {
    m_params_meta = std::move(value);
    m_params_meta_available = true;

    for (size_t i = 0; i < m_params_meta.size(); ++i) {
        sql_parameter &meta = m_params_meta.at(i);
        LOG_MSG("[" << i << "] ParamType: " << meta.data_type);
        LOG_MSG("[" << i << "] Scale:     " << meta.scale);
        LOG_MSG("[" << i << "] Precision: " << meta.precision);
        LOG_MSG("[" << i << "] Nullable:  " << (meta.nullable ? "true" : "false"));
    }
}
} // namespace ignite
