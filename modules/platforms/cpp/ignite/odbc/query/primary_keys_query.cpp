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
#include "ignite/odbc/query/primary_keys_query.h"
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

    /** Column sequence number in key. */
    KEY_SEQ,

    /** Primary key name. */
    PK_NAME
};

using namespace ignite;

/**
 * Reads primary keys.
 *
 * @param reader Reader.
 * @return Primary keys meta.
 */
primary_key_meta_vector read_key_meta(protocol::reader &reader) {
    auto has_no_data = reader.try_read_nil();
    if (has_no_data)
        return {};

    auto tables_num = reader.read_int32();

    primary_key_meta_vector keys;
    keys.reserve(tables_num);

    for (std::int32_t table_idx = 0; table_idx < tables_num; ++table_idx) {
        auto schema_name = reader.read_string();
        auto table_name = reader.read_string();
        auto key_name = reader.read_string();

        auto have_no_fields = reader.try_read_nil();
        if (have_no_fields)
            continue;

        auto fields_num = reader.read_int32();
        for (std::int32_t field_idx = 0; field_idx < fields_num; ++field_idx) {
            auto field_name = reader.read_string();

            keys.emplace_back("", schema_name, table_name, field_name, std::int16_t(field_idx + 1), key_name);
        }
    }

    return keys;
}

} // anonymous namespace

namespace ignite {

primary_keys_query::primary_keys_query(
    diagnosable_adapter &diag, sql_connection &connection, std::string schema, std::string table)
    : query(diag, query_type::PRIMARY_KEYS)
    , m_connection(connection)
    , m_schema(std::move(schema))
    , m_table(std::move(table)) {
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

sql_result primary_keys_query::execute() {
    if (m_executed)
        close();

    sql_result result = make_request_get_primary_keys();

    if (result == sql_result::AI_SUCCESS) {
        m_executed = true;
        m_fetched = false;

        m_cursor = m_meta.begin();
    }

    return result;
}

sql_result primary_keys_query::make_request_get_primary_keys() {
    auto success = m_diag.catch_errors([&] {
        auto response =
            m_connection.sync_request(protocol::client_operation::JDBC_PK_META, [&](protocol::writer &writer) {
                writer.write(m_schema);
                writer.write(m_table);
            });

        protocol::reader reader{response.get_bytes_view()};
        bool has_result_set = reader.read_bool();

        auto status = reader.read_int32();
        auto err_msg = reader.read_string_nullable();
        if (err_msg)
            throw odbc_error(response_status_to_sql_state(status), *err_msg);

        if (has_result_set)
            m_meta = read_key_meta(reader);

        m_executed = true;
    });

    if (!success)
        return sql_result::AI_ERROR;

    std::size_t i = 0;
    for (const auto &meta : m_meta) {
        LOG_MSG("[" << i << "] SchemaName:  " << meta.get_schema_name());
        LOG_MSG("[" << i << "] TableName:   " << meta.get_table_name());
        LOG_MSG("[" << i << "] ColumnName:  " << meta.get_column_name());
        LOG_MSG("[" << i << "] KeyName:     " << meta.get_key_name());
        LOG_MSG("[" << i << "] KeySequence: " << meta.get_key_seq());
        ++i;
    }

    return sql_result::AI_SUCCESS;
}

sql_result primary_keys_query::fetch_next_row(column_binding_map &column_bindings) {
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

sql_result primary_keys_query::get_column(std::uint16_t column_idx, application_data_buffer &buffer) {
    if (!m_executed) {
        m_diag.add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query was not executed.");

        return sql_result::AI_ERROR;
    }

    if (m_cursor == m_meta.end())
        return sql_result::AI_NO_DATA;

    auto &current_column = *m_cursor;

    switch (result_column(column_idx)) {
        case result_column::TABLE_CAT: {
            buffer.put_string(current_column.get_catalog_name());
            break;
        }

        case result_column::TABLE_SCHEM: {
            buffer.put_string(current_column.get_schema_name());
            break;
        }

        case result_column::TABLE_NAME: {
            buffer.put_string(current_column.get_table_name());
            break;
        }

        case result_column::COLUMN_NAME: {
            buffer.put_string(current_column.get_column_name());
            break;
        }

        case result_column::KEY_SEQ: {
            buffer.put_int16(current_column.get_key_seq());
            break;
        }

        case result_column::PK_NAME: {
            buffer.put_string(current_column.get_key_name());
            break;
        }

        default:
            break;
    }

    return sql_result::AI_SUCCESS;
}

sql_result primary_keys_query::close() {
    m_meta.clear();

    m_executed = false;

    return sql_result::AI_SUCCESS;
}

} // namespace ignite
