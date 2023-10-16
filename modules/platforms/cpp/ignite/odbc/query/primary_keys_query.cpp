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
#include "ignite/odbc/query/primary_keys_query.h"

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

    /** Column sequence number in key. */
    KEY_SEQ,

    /** Primary key name. */
    PK_NAME
};

} // anonymous namespace

namespace ignite
{

primary_keys_query::primary_keys_query(diagnosable_adapter &diag, sql_connection &connection,
    std::string catalog, std::string schema, std::string table)
    : query(diag, query_type::PRIMARY_KEYS)
    , m_connection(connection)
    , m_catalog(std::move(catalog))
    , m_schema(std::move(schema))
    , m_table(std::move(table))
{
    m_columns_meta.reserve(6);

    const std::string sch;
    const std::string tbl;

    m_columns_meta.emplace_back(sch, tbl, "TABLE_CAT", ignite_type::STRING);
    m_columns_meta.emplace_back(sch, tbl, "TABLE_SCHEM", ignite_type::STRING);
    m_columns_meta.emplace_back(sch, tbl, "TABLE_NAME", ignite_type::STRING);
    m_columns_meta.emplace_back(sch, tbl, "COLUMN_NAME", ignite_type::STRING);
    m_columns_meta.emplace_back(sch, tbl, "KEY_SEQ", ignite_type::INT16);
    m_columns_meta.emplace_back(sch, tbl, "PK_NAME", ignite_type::STRING);
}

sql_result primary_keys_query::execute()
{
    if (m_executed)
        close();

    // TODO: Implement properly
//    m_meta.push_back(meta::PrimaryKeyMeta(catalog, schema, table, "_KEY", 1, "_KEY"));

    m_executed = true;

    cursor = m_meta.begin();

    return sql_result::AI_SUCCESS;
}

sql_result primary_keys_query::fetch_next_row(column_binding_map &column_bindings)
{
    if (!m_executed)
    {
        m_diag.add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query was not executed.");

        return sql_result::AI_ERROR;
    }

    if (cursor == m_meta.end())
        return sql_result::AI_NO_DATA;

    for (auto &binding : column_bindings)
        get_column(binding.first, binding.second);

    ++cursor;

    return sql_result::AI_SUCCESS;
}

sql_result primary_keys_query::get_column(uint16_t column_idx, application_data_buffer& buffer)
{
    if (!m_executed)
    {
        m_diag.add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query was not executed.");

        return sql_result::AI_ERROR;
    }

    if (cursor == m_meta.end())
        return sql_result::AI_NO_DATA;

    auto &current_column = *cursor;

    switch (result_column(column_idx))
    {
        case result_column::TABLE_CAT:
        {
            buffer.put_string(current_column.get_catalog_name());
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

        case result_column::KEY_SEQ:
        {
            buffer.put_int16(current_column.get_key_seq());
            break;
        }

        case result_column::PK_NAME:
        {
            buffer.put_string(current_column.get_key_name());
            break;
        }

        default:
            break;
    }

    return sql_result::AI_SUCCESS;
}

sql_result primary_keys_query::close()
{
    m_meta.clear();

    m_executed = false;

    return sql_result::AI_SUCCESS;
}

} // namespace ignite
