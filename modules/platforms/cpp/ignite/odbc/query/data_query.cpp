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

#include <memory>
#include <utility>

#include "ignite/odbc/log.h"
#include "ignite/odbc/odbc_error.h"
#include "ignite/odbc/query/data_query.h"
#include "ignite/odbc/query/cursor.h"
#include "ignite/odbc/query/result_page.h"

namespace ignite
{

data_query::data_query(diagnosable_adapter &m_diag, sql_connection &m_connection, std::string sql,
    const parameter_set &params, std::int32_t &timeout)
    : query(m_diag, query_type::DATA)
    , m_connection(m_connection)
    , m_sql(std::move(sql))
    , m_params(params)
    , m_timeout(timeout) { }

data_query::~data_query()
{
    internal_close();
}

sql_result data_query::execute()
{
    internal_close();

    return make_request_execute();
}

const column_meta_vector *data_query::get_meta()
{
    if (!m_result_meta_available) {
        make_request_resultset_meta();
        if (!m_result_meta_available)
            return nullptr;
    }

    return &m_result_meta;
}

sql_result data_query::fetch_next_row(column_binding_map &column_bindings)
{
    // TODO: IGNITE-19213 Implement data fetching
    m_diag.add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED, "Data fetching is not implemented");
    return sql_result::AI_ERROR;
}

sql_result data_query::get_column(std::uint16_t column_idx, application_data_buffer &buffer)
{
    // TODO: IGNITE-19213 Implement data fetching
    m_diag.add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED, "Data fetching is not implemented");
    return sql_result::AI_ERROR;
}

sql_result data_query::close()
{
    return internal_close();
}

sql_result data_query::internal_close()
{
    if (!m_cursor)
        return sql_result::AI_SUCCESS;

    sql_result result = sql_result::AI_SUCCESS;

    if (!is_closed_remotely())
        result = make_request_close();

    if (result == sql_result::AI_SUCCESS)
    {
        m_cursor.reset();
        m_rows_affected = -1;
    }

    return result;
}

bool data_query::is_data_available() const
{
    return m_cursor.get() && m_cursor->has_data();
}

std::int64_t data_query::affected_rows() const
{
    return m_rows_affected;
}

sql_result data_query::next_result_set()
{
    // TODO: IGNITE-19855 Multiple queries execution is not supported.
    internal_close();
    return sql_result::AI_NO_DATA;
}

bool data_query::is_closed_remotely() const
{
    return m_rows_affected < 0 || m_cursor->is_closed_remotely();
}

sql_result data_query::make_request_execute()
{
    const std::string& schema = m_connection.get_schema();

    query_execute_request req(schema, m_sql, m_params, m_timeout, m_connection.is_auto_commit());
    query_execute_response rsp;

    try
    {
        m_connection.sync_message(req, rsp);
    }
    catch (const odbc_error& err)
    {
        m_diag.add_status_record(err);

        return sql_result::AI_ERROR;
    }
    catch (const ignite_error& err)
    {
        m_diag.add_status_record(err.get_text());

        return sql_result::AI_ERROR;
    }

    if (rsp.get_status() != response_status::SUCCESS)
    {
        LOG_MSG("Error: " << rsp.get_error());

        m_diag.add_status_record(response_status_to_sql_state(rsp.get_status()), rsp.get_error());

        return sql_result::AI_ERROR;
    }

    m_rows_affected = rsp.get_affected_rows();
    set_resultset_meta(rsp.get_meta());

    LOG_MSG("Query id: " << rsp.get_query_id());
    LOG_MSG("Affected Rows: " << m_rows_affected);

    m_cursor = std::make_unique<cursor>(rsp.get_query_id());

    return sql_result::AI_SUCCESS;
}

sql_result data_query::make_request_close()
{
    query_close_request req(m_cursor->get_query_id());
    query_close_response rsp;

    try
    {
        m_connection.sync_message(req, rsp);
    }
    catch (const odbc_error& err)
    {
        m_diag.add_status_record(err);

        return sql_result::AI_ERROR;
    }
    catch (const ignite_error& err)
    {
        m_diag.add_status_record(err.get_text());

        return sql_result::AI_ERROR;
    }

    LOG_MSG("Query id: " << rsp.GetQueryId());

    if (rsp.GetStatus() != response_status::SUCCESS)
    {
        LOG_MSG("Error: " << rsp.GetError());

        m_diag.add_status_record(response_status_to_sql_state(rsp.get_status()), rsp.get_error());

        return sql_result::AI_ERROR;
    }

    return sql_result::AI_SUCCESS;
}

sql_result data_query::make_request_fetch()
{
    // TODO: IGNITE-19213 Implement data fetching
    m_diag.add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED, "Data fetching is not implemented");
    return sql_result::AI_ERROR;
}

sql_result data_query::make_request_resultset_meta()
{
    // TODO: IGNITE-19854 Implement metadata fetching for the non-executed query.
    m_diag.add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
        "Metadata for non-executed queries is not supported");

    return sql_result::AI_ERROR;
}

sql_result data_query::process_conversion_result(conversion_result conv_res, std::int32_t row_idx,
    std::int32_t column_idx)
{
    switch (conv_res)
    {
        case conversion_result::AI_SUCCESS:
        {
            return sql_result::AI_SUCCESS;
        }

        case conversion_result::AI_NO_DATA:
        {
            return sql_result::AI_NO_DATA;
        }

        case conversion_result::AI_VARLEN_DATA_TRUNCATED:
        {
            m_diag.add_status_record(sql_state::S01004_DATA_TRUNCATED,
                "Buffer is too small for the column data. Truncated from the right.", row_idx, column_idx);

            return sql_result::AI_SUCCESS_WITH_INFO;
        }

        case conversion_result::AI_FRACTIONAL_TRUNCATED:
        {
            m_diag.add_status_record(sql_state::S01S07_FRACTIONAL_TRUNCATION,
                "Buffer is too small for the column data. Fraction truncated.", row_idx, column_idx);

            return sql_result::AI_SUCCESS_WITH_INFO;
        }

        case conversion_result::AI_INDICATOR_NEEDED:
        {
            m_diag.add_status_record(sql_state::S22002_INDICATOR_NEEDED,
                "Indicator is needed but not supplied for the column buffer.", row_idx, column_idx);

            return sql_result::AI_SUCCESS_WITH_INFO;
        }

        case conversion_result::AI_UNSUPPORTED_CONVERSION:
        {
            m_diag.add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                "Data conversion is not supported.", row_idx, column_idx);

            return sql_result::AI_SUCCESS_WITH_INFO;
        }

        case conversion_result::AI_FAILURE:
        default:
        {
            m_diag.add_status_record(sql_state::S01S01_ERROR_IN_ROW,
                "Can not retrieve row column.", row_idx, column_idx);

            break;
        }
    }

    return sql_result::AI_ERROR;
}

void data_query::set_resultset_meta(const column_meta_vector& value)
{
    m_result_meta.assign(value.begin(), value.end());
    m_result_meta_available = true;

    for (size_t i = 0; i < m_result_meta.size(); ++i)
    {
        column_meta& meta = m_result_meta.at(i);
        LOG_MSG("\n[" << i << "] SchemaName:     " << meta.get_schema_name()
            <<  "\n[" << i << "] TypeName:       " << meta.get_table_name()
            <<  "\n[" << i << "] ColumnName:     " << meta.get_column_name()
            <<  "\n[" << i << "] ColumnType:     " << static_cast<int32_t>(meta.get_data_type()));
    }
}

} // namespace ignite
