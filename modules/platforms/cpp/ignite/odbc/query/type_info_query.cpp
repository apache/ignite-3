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

#include "ignite/odbc/query/type_info_query.h"
#include "ignite/odbc/system/odbc_constants.h"
#include "ignite/odbc/type_traits.h"

#include <cassert>

namespace {

enum class result_column {
    /** Data source-dependent data-type name. */
    TYPE_NAME = 1,

    /** SQL data type. */
    DATA_TYPE,

    /** The maximum column size that the server supports for this data type. */
    COLUMN_SIZE,

    /** Character or characters used to prefix a literal. */
    LITERAL_PREFIX,

    /** Character or characters used to terminate a literal. */
    LITERAL_SUFFIX,

    /**
     * A list of keywords, separated by commas, corresponding to each
     * parameter that the application may specify in parentheses when using
     * the name that is returned in the TYPE_NAME field.
     */
    CREATE_PARAMS,

    /** Whether the data type accepts a NULL value. */
    NULLABLE,

    /**
     * Whether a character data type is case-sensitive in collations and
     * comparisons.
     */
    CASE_SENSITIVE,

    /** How the data type is used in a WHERE clause. */
    SEARCHABLE,

    /** Whether the data type is unsigned. */
    UNSIGNED_ATTRIBUTE,

    /** Whether the data type has predefined fixed precision and scale. */
    FIXED_PREC_SCALE,

    /** Whether the data type is auto-incrementing. */
    AUTO_UNIQUE_VALUE,

    /**
     * Localized version of the data source–dependent name of the data
     * type.
     */
    LOCAL_TYPE_NAME,

    /** The minimum scale of the data type on the data source. */
    MINIMUM_SCALE,

    /** The maximum scale of the data type on the data source. */
    MAXIMUM_SCALE,

    /**
     * The value of the SQL data type as it appears in the SQL_DESC_TYPE
     * field of the descriptor.
     */
    SQL_DATA_TYPE,

    /**
     * When the value of SQL_DATA_TYPE is SQL_DATETIME or SQL_INTERVAL,
     * this column contains the datetime/interval sub-code.
     */
    SQL_DATETIME_SUB,

    /**
     * If the data type is an approximate numeric type, this column
     * contains the value 2 to indicate that COLUMN_SIZE specifies a number
     * of bits.
     */
    NUM_PREC_RADIX,

    /**
     * If the data type is an interval data type, then this column contains
     * the value of the interval leading precision.
     */
    INTERVAL_PRECISION
};

} // anonymous namespace

namespace ignite {

type_info_query::type_info_query(diagnosable_adapter &diag, std::int16_t sql_type)
    : query(diag, query_type::TYPE_INFO) {
    m_columns_meta.reserve(19);

    const std::string sch;
    const std::string tbl;

    m_columns_meta.emplace_back(sch, tbl, "TYPE_NAME", ignite_type::STRING);
    m_columns_meta.emplace_back(sch, tbl, "DATA_TYPE", ignite_type::INT16);
    m_columns_meta.emplace_back(sch, tbl, "COLUMN_SIZE", ignite_type::INT32);
    m_columns_meta.emplace_back(sch, tbl, "LITERAL_PREFIX", ignite_type::STRING);
    m_columns_meta.emplace_back(sch, tbl, "LITERAL_SUFFIX", ignite_type::STRING);
    m_columns_meta.emplace_back(sch, tbl, "CREATE_PARAMS", ignite_type::STRING);
    m_columns_meta.emplace_back(sch, tbl, "NULLABLE", ignite_type::INT16);
    m_columns_meta.emplace_back(sch, tbl, "CASE_SENSITIVE", ignite_type::INT16);
    m_columns_meta.emplace_back(sch, tbl, "SEARCHABLE", ignite_type::INT16);
    m_columns_meta.emplace_back(sch, tbl, "UNSIGNED_ATTRIBUTE", ignite_type::INT16);
    m_columns_meta.emplace_back(sch, tbl, "FIXED_PREC_SCALE", ignite_type::INT16);
    m_columns_meta.emplace_back(sch, tbl, "AUTO_UNIQUE_VALUE", ignite_type::INT16);
    m_columns_meta.emplace_back(sch, tbl, "LOCAL_TYPE_NAME", ignite_type::STRING);
    m_columns_meta.emplace_back(sch, tbl, "MINIMUM_SCALE", ignite_type::INT16);
    m_columns_meta.emplace_back(sch, tbl, "MAXIMUM_SCALE", ignite_type::INT16);
    m_columns_meta.emplace_back(sch, tbl, "SQL_DATA_TYPE", ignite_type::INT16);
    m_columns_meta.emplace_back(sch, tbl, "SQL_DATETIME_SUB", ignite_type::INT16);
    m_columns_meta.emplace_back(sch, tbl, "NUM_PREC_RADIX", ignite_type::INT32);
    m_columns_meta.emplace_back(sch, tbl, "INTERVAL_PRECISION", ignite_type::INT16);

    assert(is_sql_type_supported(sql_type) || sql_type == SQL_ALL_TYPES);

    if (sql_type == SQL_ALL_TYPES) {
        m_types.push_back(ignite_type::BOOLEAN);
        m_types.push_back(ignite_type::INT8);
        m_types.push_back(ignite_type::INT16);
        m_types.push_back(ignite_type::INT32);
        m_types.push_back(ignite_type::INT64);
        m_types.push_back(ignite_type::FLOAT);
        m_types.push_back(ignite_type::DOUBLE);
        m_types.push_back(ignite_type::DECIMAL);
        m_types.push_back(ignite_type::DATE);
        m_types.push_back(ignite_type::TIME);
        m_types.push_back(ignite_type::DATETIME);
        m_types.push_back(ignite_type::TIMESTAMP);
        m_types.push_back(ignite_type::UUID);
        m_types.push_back(ignite_type::BITMASK);
        m_types.push_back(ignite_type::STRING);
        m_types.push_back(ignite_type::BYTE_ARRAY);
        // TODO: IGNITE-19969 implement support for period, duration and big_integer
    } else
        m_types.push_back(sql_type_to_ignite_type(sql_type));
}

sql_result type_info_query::execute() {
    m_cursor = m_types.begin();

    m_executed = true;
    m_fetched = false;

    return sql_result::AI_SUCCESS;
}

sql_result type_info_query::fetch_next_row(column_binding_map &column_bindings) {
    if (!m_executed) {
        m_diag.add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query was not executed.");
        return sql_result::AI_ERROR;
    }

    if (!m_fetched)
        m_fetched = true;
    else
        ++m_cursor;

    if (m_cursor == m_types.end())
        return sql_result::AI_NO_DATA;

    for (auto &pair : column_bindings)
        get_column(pair.first, pair.second);

    return sql_result::AI_SUCCESS;
}

sql_result type_info_query::get_column(std::uint16_t column_idx, application_data_buffer &buffer) {
    if (!m_executed) {
        m_diag.add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query was not executed.");
        return sql_result::AI_ERROR;
    }

    if (m_cursor == m_types.end()) {
        m_diag.add_status_record(sql_state::S24000_INVALID_CURSOR_STATE, "Cursor has reached end of the result set.");
        return sql_result::AI_ERROR;
    }

    auto current_type = *m_cursor;

    switch (result_column(column_idx)) {
        case result_column::TYPE_NAME: {
            buffer.put_string(ignite_type_to_sql_type_name(current_type));

            break;
        }

        case result_column::DATA_TYPE:
        case result_column::SQL_DATA_TYPE: {
            buffer.put_int16(ignite_type_to_sql_type(current_type));

            break;
        }

        case result_column::COLUMN_SIZE: {
            buffer.put_int32(ignite_type_max_column_size(current_type));

            break;
        }

        case result_column::LITERAL_PREFIX: {
            auto prefix = ignite_type_literal_prefix(current_type);
            if (!prefix)
                buffer.put_null();
            else
                buffer.put_string(*prefix);

            break;
        }

        case result_column::LITERAL_SUFFIX: {
            auto suffix = ignite_type_literal_suffix(current_type);
            if (!suffix)
                buffer.put_null();
            else
                buffer.put_string(*suffix);

            break;
        }

        case result_column::CREATE_PARAMS: {
            if (current_type == ignite_type::DECIMAL || current_type == ignite_type::NUMBER)
                buffer.put_string("precision,scale");
            else
                buffer.put_null();

            break;
        }

        case result_column::NULLABLE: {
            buffer.put_int32(ignite_type_nullability(current_type));

            break;
        }

        case result_column::CASE_SENSITIVE: {
            if (current_type == ignite_type::STRING)
                buffer.put_int16(SQL_TRUE);
            else
                buffer.put_int16(SQL_FALSE);

            break;
        }

        case result_column::SEARCHABLE: {
            buffer.put_int16(SQL_SEARCHABLE);

            break;
        }

        case result_column::UNSIGNED_ATTRIBUTE: {
            buffer.put_int16(is_ignite_type_unsigned(current_type));

            break;
        }

        case result_column::FIXED_PREC_SCALE:
        case result_column::AUTO_UNIQUE_VALUE: {
            buffer.put_int16(SQL_FALSE);

            break;
        }

        case result_column::LOCAL_TYPE_NAME: {
            buffer.put_null();

            break;
        }

        case result_column::MINIMUM_SCALE:
        case result_column::MAXIMUM_SCALE: {
            buffer.put_int16(std::int16_t(ignite_type_decimal_digits(current_type, -1)));

            break;
        }

        case result_column::SQL_DATETIME_SUB: {
            buffer.put_null();

            break;
        }

        case result_column::NUM_PREC_RADIX: {
            buffer.put_int32(ignite_type_num_precision_radix(current_type));

            break;
        }

        case result_column::INTERVAL_PRECISION: {
            buffer.put_null();

            break;
        }

        default:
            break;
    }

    return sql_result::AI_SUCCESS;
}

sql_result type_info_query::close() {
    m_cursor = m_types.end();

    m_executed = false;

    return sql_result::AI_SUCCESS;
}

} // namespace ignite
