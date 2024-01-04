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

#include "ignite/odbc/query/special_columns_query.h"
#include "ignite/odbc/type_traits.h"

namespace ignite {

special_columns_query::special_columns_query(diagnosable_adapter &diag, std::int16_t type, std::string catalog,
    std::string schema, std::string table, std::int16_t scope, std::int16_t nullable)
    : query(diag, query_type::SPECIAL_COLUMNS)
    , m_type(type)
    , m_catalog(std::move(catalog))
    , m_schema(std::move(schema))
    , m_table(std::move(table))
    , m_scope(scope)
    , m_nullable(nullable) {
    m_columns_meta.reserve(8);

    const std::string sch;
    const std::string tbl;

    m_columns_meta.emplace_back(sch, tbl, "SCOPE", ignite_type::INT16);
    m_columns_meta.emplace_back(sch, tbl, "COLUMN_NAME", ignite_type::STRING);
    m_columns_meta.emplace_back(sch, tbl, "DATA_TYPE", ignite_type::INT16);
    m_columns_meta.emplace_back(sch, tbl, "TYPE_NAME", ignite_type::STRING);
    m_columns_meta.emplace_back(sch, tbl, "COLUMN_SIZE", ignite_type::INT32);
    m_columns_meta.emplace_back(sch, tbl, "BUFFER_LENGTH", ignite_type::INT32);
    m_columns_meta.emplace_back(sch, tbl, "DECIMAL_DIGITS", ignite_type::INT16);
    m_columns_meta.emplace_back(sch, tbl, "PSEUDO_COLUMN", ignite_type::INT16);
}

sql_result special_columns_query::execute() {
    m_executed = true;

    return sql_result::AI_SUCCESS;
}

sql_result special_columns_query::fetch_next_row(column_binding_map &) {
    if (!m_executed) {
        m_diag.add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query was not executed.");

        return sql_result::AI_ERROR;
    }

    return sql_result::AI_NO_DATA;
}

sql_result special_columns_query::get_column(uint16_t, application_data_buffer &) {
    if (!m_executed) {
        m_diag.add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query was not executed.");

        return sql_result::AI_ERROR;
    }

    return sql_result::AI_NO_DATA;
}

sql_result special_columns_query::close() {
    m_executed = false;

    return sql_result::AI_SUCCESS;
}

} // namespace ignite
