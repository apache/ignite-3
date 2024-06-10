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

#include <utility>

#include "ignite/odbc/log.h"
#include "ignite/odbc/odbc_error.h"
#include "ignite/odbc/query/column_metadata_query.h"
#include "ignite/odbc/sql_connection.h"
#include "ignite/odbc/type_traits.h"

namespace {

enum class result_column {
    /** Catalog name. NULL if not applicable to the data source. */
    TABLE_CAT = 1,

    /** Schema name. NULL if not applicable to the data source. */
    TABLE_SCHEM,

    /** Table name. */
    TABLE_NAME,

    /** Column name. */
    COLUMN_NAME,

    /** SQL data type. */
    DATA_TYPE,

    /** Data source-dependent data type name. */
    TYPE_NAME,

    /** Column size. */
    COLUMN_SIZE,

    /** The length in bytes of data transferred on fetch. */
    BUFFER_LENGTH,

    /** The total number of significant digits to the right of the decimal point. */
    DECIMAL_DIGITS,

    /** Precision. */
    NUM_PREC_RADIX,

    /** Nullability of the data in column. */
    NULLABLE,

    /** A description of the column. */
    REMARKS
};

using namespace ignite;

/**
 * Reads result set metadata.
 *
 * @param reader Reader.
 * @return Result set meta columns.
 */
std::vector<odbc_column_meta> read_column_meta(protocol::reader &reader) {
    auto size = reader.read_int32();

    std::vector<odbc_column_meta> columns;
    columns.reserve(size);

    for (std::int32_t column_idx = 0; column_idx < size; ++column_idx) {
        auto status = reader.read_int32();
        assert(status == 0);

        auto err_msg = reader.read_string_nullable();
        assert(!err_msg);

        odbc_column_meta column{};
        column.label = reader.read_string();
        column.schema = reader.read_string_nullable();
        column.table = reader.read_string_nullable();
        column.column = reader.read_string_nullable();

        column.data_type = ignite_type(reader.read_int32());
        column.data_type_name = reader.read_string();
        reader.skip(); // data_type_class
        column.nullable = reader.read_bool();
        column.precision = reader.read_int32();
        column.scale = reader.read_int32();

        columns.emplace_back(std::move(column));
    }

    return columns;
}

} // anonymous namespace

namespace ignite {

column_metadata_query::column_metadata_query(
    diagnosable_adapter &diag, sql_connection &connection, std::string schema, std::string table, std::string column)
    : query(diag, query_type::COLUMN_METADATA)
    , m_connection(connection)
    , m_schema(std::move(schema))
    , m_table(std::move(table))
    , m_column(std::move(column)) {
    m_columns_meta.reserve(12);

    const std::string sch;
    const std::string tbl;

    m_columns_meta.emplace_back(sch, tbl, "TABLE_CAT", ignite_type::STRING);
    m_columns_meta.emplace_back(sch, tbl, "TABLE_SCHEM", ignite_type::STRING);
    m_columns_meta.emplace_back(sch, tbl, "TABLE_NAME", ignite_type::STRING);
    m_columns_meta.emplace_back(sch, tbl, "COLUMN_NAME", ignite_type::STRING);
    m_columns_meta.emplace_back(sch, tbl, "DATA_TYPE", ignite_type::INT16);
    m_columns_meta.emplace_back(sch, tbl, "TYPE_NAME", ignite_type::STRING);
    m_columns_meta.emplace_back(sch, tbl, "COLUMN_SIZE", ignite_type::INT32);
    m_columns_meta.emplace_back(sch, tbl, "BUFFER_LENGTH", ignite_type::INT32);
    m_columns_meta.emplace_back(sch, tbl, "DECIMAL_DIGITS", ignite_type::INT16);
    m_columns_meta.emplace_back(sch, tbl, "NUM_PREC_RADIX", ignite_type::INT16);
    m_columns_meta.emplace_back(sch, tbl, "NULLABLE", ignite_type::INT16);
    m_columns_meta.emplace_back(sch, tbl, "REMARKS", ignite_type::STRING);
}

sql_result column_metadata_query::execute() {
    if (m_executed)
        close();

    sql_result result = make_request_get_columns_meta();

    if (result == sql_result::AI_SUCCESS) {
        m_executed = true;
        m_fetched = false;

        m_cursor = m_meta.begin();
    }

    return result;
}

sql_result column_metadata_query::fetch_next_row(column_binding_map &column_bindings) {
    if (!m_executed) {
        m_diag.add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query was not executed.");

        return sql_result::AI_ERROR;
    }

    if (!m_fetched)
        m_fetched = true;
    else
        ++m_cursor;

    if (m_cursor == m_meta.end())
        return sql_result::AI_NO_DATA;

    for (auto &binding : column_bindings)
        get_column(binding.first, binding.second);

    return sql_result::AI_SUCCESS;
}

sql_result column_metadata_query::get_column(std::uint16_t column_idx, application_data_buffer &buffer) {
    if (!m_executed) {
        m_diag.add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query was not executed.");

        return sql_result::AI_ERROR;
    }

    if (m_cursor == m_meta.end()) {
        m_diag.add_status_record(sql_state::S24000_INVALID_CURSOR_STATE, "Cursor has reached end of the result set.");

        return sql_result::AI_ERROR;
    }

    const auto &current_column = *m_cursor;
    switch (result_column(column_idx)) {
        case result_column::TABLE_CAT: {
            buffer.put_null();
            break;
        }

        case result_column::TABLE_SCHEM: {
            buffer.put_string(current_column.schema);
            break;
        }

        case result_column::TABLE_NAME: {
            buffer.put_string(current_column.table);
            break;
        }

        case result_column::COLUMN_NAME: {
            buffer.put_string(current_column.column);
            break;
        }

        case result_column::DATA_TYPE: {
            buffer.put_int16(ignite_type_to_sql_type(current_column.data_type));
            break;
        }

        case result_column::TYPE_NAME: {
            buffer.put_string(current_column.data_type_name);
            break;
        }

        case result_column::COLUMN_SIZE: {
            if (current_column.data_type == ignite_type::DECIMAL || current_column.data_type == ignite_type::NUMBER) {
                buffer.put_int16(std::int16_t(current_column.precision));
                break;
            }

            std::int32_t column_size = ignite_type_max_column_size(current_column.data_type);
            if (column_size < 0)
                buffer.put_null();
            else
                buffer.put_int32(column_size);
            break;
        }

        case result_column::BUFFER_LENGTH: {
            buffer.put_null();
            break;
        }

        case result_column::DECIMAL_DIGITS: {
            std::int32_t dec_digits = ignite_type_decimal_digits(current_column.data_type, current_column.scale);
            if (dec_digits < 0)
                buffer.put_null();
            else
                buffer.put_int16(std::int16_t(dec_digits));
            break;
        }

        case result_column::NUM_PREC_RADIX: {
            auto radix = std::int16_t(ignite_type_num_precision_radix(current_column.data_type));
            if (radix)
                buffer.put_int16(radix);
            else
                buffer.put_null();
            break;
        }

        case result_column::NULLABLE: {
            buffer.put_int16(std::int16_t(current_column.nullable ? SQL_NULLABLE : SQL_NO_NULLS));
            break;
        }

        case result_column::REMARKS: {
            buffer.put_string(current_column.label);
            break;
        }

        default:
            break;
    }

    return sql_result::AI_SUCCESS;
}

sql_result column_metadata_query::close() {
    m_meta.clear();

    m_executed = false;

    return sql_result::AI_SUCCESS;
}

sql_result column_metadata_query::make_request_get_columns_meta() {
    auto success = m_diag.catch_errors([&] {
        auto response =
            m_connection.sync_request(protocol::client_operation::JDBC_COLUMN_META, [&](protocol::writer &writer) {
                writer.write(m_schema);
                writer.write(m_table);
                writer.write(m_column);
            });

        protocol::reader reader{response.get_bytes_view()};

        auto status = reader.read_int32();
        auto err_msg = reader.read_string_nullable();
        if (err_msg)
            throw odbc_error(response_status_to_sql_state(status), *err_msg);

        m_meta = read_column_meta(reader);

        m_executed = true;
    });

    if (!success)
        return sql_result::AI_ERROR;

    size_t i = 0;
    for (const auto &meta : m_meta) {
        LOG_MSG("\n[" << i << "] SchemaName:     " << (meta.schema ? *meta.schema : "NULL") << "\n[" << i
                      << "] TableName:      " << (meta.table ? *meta.table : "NULL") << "\n[" << i
                      << "] ColumnName:     " << (meta.column ? *meta.column : "NULL") << "\n[" << i
                      << "] ColumnType:     " << static_cast<std::int32_t>(meta.data_type));
        ++i;
    }

    return sql_result::AI_SUCCESS;
}

} // namespace ignite
