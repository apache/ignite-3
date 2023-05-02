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

#include "ignite/odbc/query/data_query.h"
#include "ignite/odbc/connection.h"
#include "ignite/odbc/log.h"
#include "ignite/odbc/message.h"
#include "ignite/odbc/odbc_error.h"

namespace ignite
{

data_query::~data_query()
{
    internal_close();
}

sql_result data_query::execute()
{
    if (m_cursor.get())
        internal_close();

    return make_request_execute();
}

const column_meta_vector* data_query::get_meta()
{
    if (!m_result_meta_available)
    {
        make_request_resultset_meta();

        if (!m_result_meta_available)
            return 0;
    }

    return &m_result_meta;
}

sql_result data_query::fetch_next_row(column_binding_map& column_bindings)
{
    if (!m_cursor.get())
    {
        m_diag.add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query was not executed.");

        return sql_result::AI_ERROR;
    }

    if (!m_cursor->HasData())
        return sql_result::AI_NO_DATA;

    m_cursor->Increment();

    if (m_cursor->NeedDataUpdate())
    {
        if (m_cached_next_page.get())
            m_cursor->UpdateData(m_cached_next_page);
        else
        {
            sql_result result = make_request_fetch();

            if (result != sql_result::AI_SUCCESS)
                return result;
        }
    }

    if (!m_cursor->HasData())
        return sql_result::AI_NO_DATA;

    Row* row = m_cursor->GetRow();

    if (!row)
    {
        m_diag.add_status_record("Unknown error.");

        return sql_result::AI_ERROR;
    }

    for (std::int32_t i = 1; i < row->get_size() + 1; ++i)
    {
        column_binding_map::iterator it = column_bindings.find(i);

        if (it == column_bindings.end())
            continue;

        conversion_result convRes = row->ReadColumnToBuffer(i, it->second);

        sql_result result = process_conversion_result(convRes, 0, i);

        if (result == sql_result::AI_ERROR)
            return sql_result::AI_ERROR;
    }

    return sql_result::AI_SUCCESS;
}

sql_result data_query::get_column(uint16_t column_idx, application_data_buffer& buffer)
{
    if (!m_cursor.get())
    {
        m_diag.add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query was not executed.");

        return sql_result::AI_ERROR;
    }

    Row* row = m_cursor->GetRow();

    if (!row)
    {
        m_diag.add_status_record(sql_state::S24000_INVALID_CURSOR_STATE,
            "Cursor has reached end of the result set.");

        return sql_result::AI_ERROR;
    }

    conversion_result convRes = row->ReadColumnToBuffer(column_idx, buffer);

    sql_result result = process_conversion_result(convRes, 0, column_idx);

    return result;
}

sql_result data_query::close()
{
    return internal_close();
}

sql_result data_query::internal_close()
{
    if (!m_cursor.get())
        return sql_result::AI_SUCCESS;

    sql_result result = sql_result::AI_SUCCESS;

    if (!is_closed_remotely())
        result = make_request_close();

    if (result == sql_result::AI_SUCCESS)
    {
        m_cursor.reset();
        m_rows_affected_idx = 0;
        m_rows_affected.clear();
    }

    return result;
}

bool data_query::is_data_available() const
{
    return m_cursor.get() && m_cursor->HasData();
}

int64_t data_query::affected_rows() const
{
    int64_t affected = m_rows_affected_idx < m_rows_affected.size() ? m_rows_affected[m_rows_affected_idx] : 0;

    if (affected >= 0)
        return affected;

    return m_connection.GetConfiguration().get_page_size();
}

sql_result data_query::next_result_set()
{
    if (m_rows_affected_idx + 1 >= m_rows_affected.size())
    {
        internal_close();

        return sql_result::AI_NO_DATA;
    }

    sql_result res = make_request_more_results();

    if (res == sql_result::AI_SUCCESS)
        ++m_rows_affected_idx;

    return res;
}

bool data_query::is_closed_remotely() const
{
    for (size_t i = 0; i < m_rows_affected.size(); ++i)
    {
        if (m_rows_affected[i] < 0)
            return false;
    }

    return true;
}

sql_result data_query::make_request_execute()
{
    const std::string& schema = m_connection.GetSchema();

    QueryExecuteRequest req(schema, sql, params, timeout, m_connection.IsAutoCommit());
    QueryExecuteResponse rsp;

    try
    {
        m_connection.SyncMessage(req, rsp);
    }
    catch (const odbc_error& err)
    {
        m_diag.add_status_record(err);

        return sql_result::AI_ERROR;
    }
    catch (const IgniteError& err)
    {
        m_diag.add_status_record(err.GetText());

        return sql_result::AI_ERROR;
    }

    if (rsp.get_state() != response_status::SUCCESS)
    {
        LOG_MSG("Error: " << rsp.get_error());

        m_diag.add_status_record(response_status_to_sql_state(rsp.get_state()), rsp.get_error());

        return sql_result::AI_ERROR;
    }

    m_rows_affected = rsp.GetAffectedRows();
    set_resultset_meta(rsp.get_meta());

    LOG_MSG("Query id: " << rsp.GetQueryId());
    LOG_MSG("Affected Rows list size: " << m_rows_affected.size());

    m_cursor.reset(new Cursor(rsp.GetQueryId()));

    m_rows_affected_idx = 0;

    return sql_result::AI_SUCCESS;
}

sql_result data_query::make_request_close()
{
    QueryCloseRequest req(m_cursor->GetQueryId());
    QueryCloseResponse rsp;

    try
    {
        m_connection.SyncMessage(req, rsp);
    }
    catch (const odbc_error& err)
    {
        m_diag.add_status_record(err);

        return sql_result::AI_ERROR;
    }
    catch (const IgniteError& err)
    {
        m_diag.add_status_record(err.GetText());

        return sql_result::AI_ERROR;
    }

    LOG_MSG("Query id: " << rsp.GetQueryId());

    if (rsp.get_state() != response_status::SUCCESS)
    {
        LOG_MSG("Error: " << rsp.get_error());

        m_diag.add_status_record(response_status_to_sql_state(rsp.get_state()), rsp.get_error());

        return sql_result::AI_ERROR;
    }

    return sql_result::AI_SUCCESS;
}

sql_result data_query::make_request_fetch()
{
    std::unique_ptr<ResultPage> resultPage(new ResultPage());

    QueryFetchRequest req(m_cursor->GetQueryId(), m_connection.GetConfiguration().get_page_size());
    QueryFetchResponse rsp(*resultPage);

    try
    {
        m_connection.SyncMessage(req, rsp);
    }
    catch (const odbc_error& err)
    {
        m_diag.add_status_record(err);

        return sql_result::AI_ERROR;
    }
    catch (const IgniteError& err)
    {
        m_diag.add_status_record(err.GetText());

        return sql_result::AI_ERROR;
    }

    if (rsp.get_state() != response_status::SUCCESS)
    {
        LOG_MSG("Error: " << rsp.get_error());

        m_diag.add_status_record(response_status_to_sql_state(rsp.get_state()), rsp.get_error());

        return sql_result::AI_ERROR;
    }

    LOG_MSG("Page size:    " << resultPage->get_size());
    LOG_MSG("Page is last: " << resultPage->IsLast());

    m_cursor->UpdateData(resultPage);

    return sql_result::AI_SUCCESS;
}

sql_result data_query::make_request_more_results()
{
    std::unique_ptr<ResultPage> resultPage(new ResultPage());

    QueryMoreResultsRequest req(m_cursor->GetQueryId(), m_connection.GetConfiguration().get_page_size());
    QueryMoreResultsResponse rsp(*resultPage);

    try
    {
        m_connection.SyncMessage(req, rsp);
    }
    catch (const odbc_error& err)
    {
        m_diag.add_status_record(err);

        return sql_result::AI_ERROR;
    }
    catch (const IgniteError& err)
    {
        m_diag.add_status_record(err.GetText());

        return sql_result::AI_ERROR;
    }

    if (rsp.get_state() != response_status::SUCCESS)
    {
        LOG_MSG("Error: " << rsp.get_error());

        m_diag.add_status_record(response_status_to_sql_state(rsp.get_state()), rsp.get_error());

        return sql_result::AI_ERROR;
    }

    LOG_MSG("Page size:    " << resultPage->get_size());
    LOG_MSG("Page is last: " << resultPage->IsLast());

    m_cached_next_page = resultPage;
    m_cursor.reset(new Cursor(rsp.GetQueryId()));

    return sql_result::AI_SUCCESS;
}

sql_result data_query::make_request_resultset_meta()
{
    const std::string& schema = m_connection.GetSchema();

    QueryGetResultsetMetaRequest req(schema, sql);
    QueryGetResultsetMetaResponse rsp;

    try
    {
        m_connection.SyncMessage(req, rsp);
    }
    catch (const odbc_error& err)
    {
        m_diag.add_status_record(err);

        return sql_result::AI_ERROR;
    }
    catch (const IgniteError& err)
    {
        m_diag.add_status_record(err.GetText());

        return sql_result::AI_ERROR;
    }

    if (rsp.get_state() != response_status::SUCCESS)
    {
        LOG_MSG("Error: " << rsp.get_error());

        m_diag.add_status_record(response_status_to_sql_state(rsp.get_state()), rsp.get_error());

        return sql_result::AI_ERROR;
    }

    set_resultset_meta(rsp.get_meta());

    return sql_result::AI_SUCCESS;
}

sql_result data_query::process_conversion_result(conversion_result convRes, std::int32_t rowIdx,
    std::int32_t column_idx)
{
    switch (convRes)
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
                "Buffer is too small for the column data. Truncated from the right.", rowIdx, column_idx);

            return sql_result::AI_SUCCESS_WITH_INFO;
        }

        case conversion_result::AI_FRACTIONAL_TRUNCATED:
        {
            m_diag.add_status_record(sql_state::S01S07_FRACTIONAL_TRUNCATION,
                "Buffer is too small for the column data. Fraction truncated.", rowIdx, column_idx);

            return sql_result::AI_SUCCESS_WITH_INFO;
        }

        case conversion_result::AI_INDICATOR_NEEDED:
        {
            m_diag.add_status_record(sql_state::S22002_INDICATOR_NEEDED,
                "Indicator is needed but not suplied for the column buffer.", rowIdx, column_idx);

            return sql_result::AI_SUCCESS_WITH_INFO;
        }

        case conversion_result::AI_UNSUPPORTED_CONVERSION:
        {
            m_diag.add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                "Data conversion is not supported.", rowIdx, column_idx);

            return sql_result::AI_SUCCESS_WITH_INFO;
        }

        case conversion_result::AI_FAILURE:
        default:
        {
            m_diag.add_status_record(sql_state::S01S01_ERROR_IN_ROW,
                "Can not retrieve row column.", rowIdx, column_idx);

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
            <<  "\n[" << i << "] ColumnType:     " << static_cast<std::int32_t>(meta.get_data_type()));
    }
}

} // namespace ignite

