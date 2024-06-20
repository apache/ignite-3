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
#include "ignite/odbc/query/table_metadata_query.h"
#include "ignite/odbc/sql_connection.h"
#include "ignite/odbc/string_utils.h"
#include "ignite/odbc/type_traits.h"

namespace {

enum class result_column {
    /** Catalog name. NULL if not applicable to the data source. */
    TABLE_CAT = 1,

    /** Schema name. NULL if not applicable to the data source. */
    TABLE_SCHEM,

    /** Table name. */
    TABLE_NAME,

    /** Table type. */
    TABLE_TYPE,

    /** A description of the column. */
    REMARKS
};

} // namespace

namespace ignite {

table_metadata_query::table_metadata_query(diagnosable_adapter &diag, sql_connection &connection, std::string catalog,
    std::string schema, std::string table, std::string table_type)
    : query(diag, query_type::TABLE_METADATA)
    , m_connection(connection)
    , m_catalog(std::move(catalog))
    , m_schema(std::move(schema))
    , m_table(std::move(table))
    , m_table_type(std::move(table_type)) {
    m_columns_meta.reserve(5);

    const std::string sch;
    const std::string tbl;

    m_columns_meta.emplace_back(sch, tbl, "TABLE_CAT", ignite_type::STRING);
    m_columns_meta.emplace_back(sch, tbl, "TABLE_SCHEM", ignite_type::STRING);
    m_columns_meta.emplace_back(sch, tbl, "TABLE_NAME", ignite_type::STRING);
    m_columns_meta.emplace_back(sch, tbl, "TABLE_TYPE", ignite_type::STRING);
    m_columns_meta.emplace_back(sch, tbl, "REMARKS", ignite_type::STRING);
}

sql_result table_metadata_query::execute() {
    if (m_executed)
        close();

    sql_result result = make_request_get_tables_meta();

    if (result == sql_result::AI_SUCCESS) {
        m_executed = true;
        m_fetched = false;

        m_cursor = m_meta.begin();
    }

    return result;
}

const column_meta_vector *table_metadata_query::get_meta() {
    return &m_columns_meta;
}

sql_result table_metadata_query::fetch_next_row(column_binding_map &column_bindings) {
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

sql_result table_metadata_query::get_column(std::uint16_t column_idx, application_data_buffer &buffer) {
    if (!m_executed) {
        m_diag.add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query was not executed.");

        return sql_result::AI_ERROR;
    }

    if (m_cursor == m_meta.end()) {
        m_diag.add_status_record(sql_state::S24000_INVALID_CURSOR_STATE, "Cursor has reached end of the result set.");

        return sql_result::AI_ERROR;
    }

    const table_meta &current_column = *m_cursor;

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

        case result_column::TABLE_TYPE: {
            buffer.put_string(current_column.get_table_type());
            break;
        }

        case result_column::REMARKS: {
            buffer.put_null();
            break;
        }

        default:
            break;
    }

    return sql_result::AI_SUCCESS;
}

sql_result table_metadata_query::close() {
    m_meta.clear();

    m_executed = false;

    return sql_result::AI_SUCCESS;
}

bool table_metadata_query::is_data_available() const {
    return m_cursor != m_meta.end();
}

int64_t table_metadata_query::affected_rows() const {
    return 0;
}

sql_result table_metadata_query::next_result_set() {
    return sql_result::AI_NO_DATA;
}

sql_result table_metadata_query::make_request_get_tables_meta() {
    auto table_types = split(m_table_type, ',');

    auto success = m_diag.catch_errors([&] {
        auto response =
            m_connection.sync_request(protocol::client_operation::JDBC_TABLE_META, [&](protocol::writer &writer) {
                writer.write(m_schema);
                writer.write(m_table);

                writer.write(static_cast<int32_t>(table_types.size()));
                for (auto table_type : table_types) {
                    writer.write(table_type);
                }
            });

        protocol::reader reader{response.get_bytes_view()};

        auto status = reader.read_int32();
        auto err_msg = reader.read_string_nullable();
        if (err_msg)
            throw odbc_error(response_status_to_sql_state(status), *err_msg);

        m_meta = read_table_meta_vector(reader);

        m_executed = true;
    });

    if (!success)
        return sql_result::AI_ERROR;

    int idx = 0;
    for (auto &column : m_meta) {
        LOG_MSG("\n[" << idx << "] catalog_name: " << column.get_catalog_name() << "\n[" << idx << "] schema_name:  "
                      << column.get_schema_name() << "\n[" << idx << "] table_name:   " << column.get_table_name()
                      << "\n[" << idx << "] table_type:   " << column.get_table_type());
        ++idx;
    }

    return sql_result::AI_SUCCESS;
}

} // namespace ignite
