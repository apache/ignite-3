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

#include "ignite/odbc/type_traits.h"
#include "ignite/odbc/sql_connection.h"
#include "ignite/odbc/log.h"
#include "ignite/odbc/odbc_error.h"
#include "ignite/odbc/query/column_metadata_query.h"

namespace
{

enum class result_column
{
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

} // anonymous namespace


namespace ignite
{

column_metadata_query::column_metadata_query(diagnosable_adapter& diag, sql_connection& connection, std::string schema,
    std::string table, std::string column)
    : query(diag, query_type::COLUMN_METADATA)
    , m_connection(connection)
    , m_schema(std::move(schema))
    , m_table(std::move(table))
    , m_column(std::move(column))
{
    m_columns_meta.reserve(12);

    const std::string sch;
    const std::string tbl;

    m_columns_meta.emplace_back(sch, tbl, "TABLE_CAT",      ignite_type::STRING);
    m_columns_meta.emplace_back(sch, tbl, "TABLE_SCHEM",    ignite_type::STRING);
    m_columns_meta.emplace_back(sch, tbl, "TABLE_NAME",     ignite_type::STRING);
    m_columns_meta.emplace_back(sch, tbl, "COLUMN_NAME",    ignite_type::STRING);
    m_columns_meta.emplace_back(sch, tbl, "DATA_TYPE",      ignite_type::INT16);
    m_columns_meta.emplace_back(sch, tbl, "TYPE_NAME",      ignite_type::STRING);
    m_columns_meta.emplace_back(sch, tbl, "COLUMN_SIZE",    ignite_type::INT32);
    m_columns_meta.emplace_back(sch, tbl, "BUFFER_LENGTH",  ignite_type::INT32);
    m_columns_meta.emplace_back(sch, tbl, "DECIMAL_DIGITS", ignite_type::INT16);
    m_columns_meta.emplace_back(sch, tbl, "NUM_PREC_RADIX", ignite_type::INT16);
    m_columns_meta.emplace_back(sch, tbl, "NULLABLE",       ignite_type::INT16);
    m_columns_meta.emplace_back(sch, tbl, "REMARKS",        ignite_type::STRING);
}

sql_result column_metadata_query::execute()
{
    if (m_executed)
        close();

    sql_result result = make_request_get_columns_meta();

    if (result == sql_result::AI_SUCCESS)
    {
        m_executed = true;
        m_fetched = false;

        m_cursor = m_meta.begin();
    }

    return result;
}

sql_result column_metadata_query::fetch_next_row(column_binding_map & column_bindings)
{
    if (!m_executed)
    {
        m_diag.add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query was not executed.");

        return sql_result::AI_ERROR;
    }

    if (!m_fetched)
        m_fetched = true;
    else
        ++m_cursor;

    if (m_cursor == m_meta.end())
        return sql_result::AI_NO_DATA;

    column_binding_map::iterator it;

    for (it = column_bindings.begin(); it != column_bindings.end(); ++it)
        get_column(it->first, it->second);

    return sql_result::AI_SUCCESS;
}

sql_result column_metadata_query::get_column(std::uint16_t column_idx, application_data_buffer & buffer)
{
    if (!m_executed)
    {
        m_diag.add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query was not executed.");

        return sql_result::AI_ERROR;
    }

    if (m_cursor == m_meta.end())
    {
        m_diag.add_status_record(sql_state::S24000_INVALID_CURSOR_STATE,
            "Cursor has reached end of the result set.");

        return sql_result::AI_ERROR;
    }

    const column_meta& current_column = *m_cursor;
    auto column_type = current_column.get_data_type();

    switch (result_column(column_idx))
    {
        case result_column::TABLE_CAT:
        {
            buffer.put_null();
            break;
        }

        case result_column::TABLE_SCHEM:
        {
            buffer.put_string(current_column.get_schema_name());
            break;
        }

        case result_column::TABLE_NAME:
        {
            buffer.put_string(current_column.get_table_name());
            break;
        }

        case result_column::COLUMN_NAME:
        {
            buffer.put_string(current_column.get_column_name());
            break;
        }

        case result_column::DATA_TYPE:
        {
            buffer.put_int16(ignite_type_to_sql_type(column_type));
            break;
        }

        case result_column::TYPE_NAME:
        {
            buffer.put_string(ignite_type_to_sql_type_name(current_column.get_data_type()));
            break;
        }

        case result_column::COLUMN_SIZE:
        {
            buffer.put_int16(std::int16_t(ignite_type_max_column_size(column_type)));
            break;
        }

        case result_column::BUFFER_LENGTH:
        {
            buffer.put_int16(std::int16_t(ignite_type_transfer_length(column_type)));
            break;
        }

        case result_column::DECIMAL_DIGITS:
        {
            std::int32_t dec_digits = ignite_type_decimal_digits(column_type);
            if (dec_digits < 0)
                buffer.put_null();
            else
                buffer.put_int16(static_cast<std::int16_t>(dec_digits));
            break;
        }

        case result_column::NUM_PREC_RADIX:
        {
            buffer.put_int16(std::int16_t(ignite_type_num_precision_radix(column_type)));
            break;
        }

        case result_column::NULLABLE:
        {
            buffer.put_int16(ignite_type_nullability(column_type));
            break;
        }

        case result_column::REMARKS:
        {
            buffer.put_null();
            break;
        }

        default:
            break;
    }

    return sql_result::AI_SUCCESS;
}

sql_result column_metadata_query::close()
{
    m_meta.clear();

    m_executed = false;

    return sql_result::AI_SUCCESS;
}

sql_result column_metadata_query::make_request_get_columns_meta()
{
    for (size_t i = 0; i < m_meta.size(); ++i)
    {
        LOG_MSG("\n[" << i << "] SchemaName:     " << m_meta[i].get_schema_name()
             << "\n[" << i << "] TableName:      " << m_meta[i].get_table_name()
             << "\n[" << i << "] ColumnName:     " << m_meta[i].get_column_name()
             << "\n[" << i << "] ColumnType:     " << static_cast<std::int32_t>(m_meta[i].get_data_type()));
    }

    return sql_result::AI_SUCCESS;
}

} // namespace ignite
