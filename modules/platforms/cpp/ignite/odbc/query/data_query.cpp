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

#include "data_query.h"
#include "../connection.h"
#include "../log.h"
#include "../message.h"
#include "../odbc_error.h"
#include "batch_query.h"

namespace ignite
{
    namespace odbc
    {
        namespace query
        {
            DataQuery::DataQuery(DiagnosableAdapter& diag, Connection& connection, const std::string& sql,
                const parameter_set& params, int32_t& timeout) :
                Query(diag, QueryType::DATA),
                connection(connection),
                sql(sql),
                params(params),
                resultMetaAvailable(false),
                resultMeta(),
                cursor(),
                rowsAffected(),
                rowsAffectedIdx(0),
                cachedNextPage(),
                timeout(timeout)
            {
                // No-op.
            }

            DataQuery::~DataQuery()
            {
                InternalClose();
            }

            sql_result DataQuery::Execute()
            {
                if (cursor.get())
                    InternalClose();

                return MakeRequestExecute();
            }

            const meta::ColumnMetaVector* DataQuery::GetMeta()
            {
                if (!resultMetaAvailable)
                {
                    MakeRequestResultsetMeta();

                    if (!resultMetaAvailable)
                        return 0;
                }

                return &resultMeta;
            }

            sql_result DataQuery::FetchNextRow(column_binding_map& columnBindings)
            {
                if (!cursor.get())
                {
                    diag.AddStatusRecord(sql_state::SHY010_SEQUENCE_ERROR, "Query was not executed.");

                    return sql_result::AI_ERROR;
                }

                if (!cursor->HasData())
                    return sql_result::AI_NO_DATA;

                cursor->Increment();

                if (cursor->NeedDataUpdate())
                {
                    if (cachedNextPage.get())
                        cursor->UpdateData(cachedNextPage);
                    else
                    {
                        sql_result result = MakeRequestFetch();

                        if (result != sql_result::AI_SUCCESS)
                            return result;
                    }
                }

                if (!cursor->HasData())
                    return sql_result::AI_NO_DATA;

                Row* row = cursor->GetRow();

                if (!row)
                {
                    diag.AddStatusRecord("Unknown error.");

                    return sql_result::AI_ERROR;
                }

                for (int32_t i = 1; i < row->get_size() + 1; ++i)
                {
                    column_binding_map::iterator it = columnBindings.find(i);

                    if (it == columnBindings.end())
                        continue;

                    conversion_result convRes = row->ReadColumnToBuffer(i, it->second);

                    sql_result result = ProcessConversionResult(convRes, 0, i);

                    if (result == sql_result::AI_ERROR)
                        return sql_result::AI_ERROR;
                }

                return sql_result::AI_SUCCESS;
            }

            sql_result DataQuery::GetColumn(uint16_t columnIdx, application_data_buffer& buffer)
            {
                if (!cursor.get())
                {
                    diag.AddStatusRecord(sql_state::SHY010_SEQUENCE_ERROR, "Query was not executed.");

                    return sql_result::AI_ERROR;
                }

                Row* row = cursor->GetRow();

                if (!row)
                {
                    diag.AddStatusRecord(sql_state::S24000_INVALID_CURSOR_STATE,
                        "Cursor has reached end of the result set.");

                    return sql_result::AI_ERROR;
                }

                conversion_result convRes = row->ReadColumnToBuffer(columnIdx, buffer);

                sql_result result = ProcessConversionResult(convRes, 0, columnIdx);

                return result;
            }

            sql_result DataQuery::Close()
            {
                return InternalClose();
            }

            sql_result DataQuery::InternalClose()
            {
                if (!cursor.get())
                    return sql_result::AI_SUCCESS;

                sql_result result = sql_result::AI_SUCCESS;

                if (!IsClosedRemotely())
                    result = MakeRequestClose();

                if (result == sql_result::AI_SUCCESS)
                {
                    cursor.reset();

                    rowsAffectedIdx = 0;

                    rowsAffected.clear();
                }

                return result;
            }

            bool DataQuery::DataAvailable() const
            {
                return cursor.get() && cursor->HasData();
            }

            int64_t DataQuery::AffectedRows() const
            {
                int64_t affected = rowsAffectedIdx < rowsAffected.size() ? rowsAffected[rowsAffectedIdx] : 0;

                if (affected >= 0)
                    return affected;

                return connection.GetConfiguration().GetPageSize();
            }

            sql_result DataQuery::NextResultSet()
            {
                if (rowsAffectedIdx + 1 >= rowsAffected.size())
                {
                    InternalClose();

                    return sql_result::AI_NO_DATA;
                }

                sql_result res = MakeRequestMoreResults();

                if (res == sql_result::AI_SUCCESS)
                    ++rowsAffectedIdx;

                return res;
            }

            bool DataQuery::IsClosedRemotely() const
            {
                for (size_t i = 0; i < rowsAffected.size(); ++i)
                {
                    if (rowsAffected[i] < 0)
                        return false;
                }

                return true;
            }

            sql_result DataQuery::MakeRequestExecute()
            {
                const std::string& schema = connection.GetSchema();

                QueryExecuteRequest req(schema, sql, params, timeout, connection.IsAutoCommit());
                QueryExecuteResponse rsp;

                try
                {
                    connection.SyncMessage(req, rsp);
                }
                catch (const odbc_error& err)
                {
                    diag.AddStatusRecord(err);

                    return sql_result::AI_ERROR;
                }
                catch (const IgniteError& err)
                {
                    diag.AddStatusRecord(err.GetText());

                    return sql_result::AI_ERROR;
                }

                if (rsp.get_state() != response_status::SUCCESS)
                {
                    LOG_MSG("Error: " << rsp.GetError());

                    diag.AddStatusRecord(response_status_to_sql_state(rsp.get_state()), rsp.GetError());

                    return sql_result::AI_ERROR;
                }

                rowsAffected = rsp.GetAffectedRows();
                SetResultsetMeta(rsp.GetMeta());

                LOG_MSG("Query id: " << rsp.GetQueryId());
                LOG_MSG("Affected Rows list size: " << rowsAffected.size());

                cursor.reset(new Cursor(rsp.GetQueryId()));

                rowsAffectedIdx = 0;

                return sql_result::AI_SUCCESS;
            }

            sql_result DataQuery::MakeRequestClose()
            {
                QueryCloseRequest req(cursor->GetQueryId());
                QueryCloseResponse rsp;

                try
                {
                    connection.SyncMessage(req, rsp);
                }
                catch (const odbc_error& err)
                {
                    diag.AddStatusRecord(err);

                    return sql_result::AI_ERROR;
                }
                catch (const IgniteError& err)
                {
                    diag.AddStatusRecord(err.GetText());

                    return sql_result::AI_ERROR;
                }

                LOG_MSG("Query id: " << rsp.GetQueryId());

                if (rsp.get_state() != response_status::SUCCESS)
                {
                    LOG_MSG("Error: " << rsp.GetError());

                    diag.AddStatusRecord(response_status_to_sql_state(rsp.get_state()), rsp.GetError());

                    return sql_result::AI_ERROR;
                }

                return sql_result::AI_SUCCESS;
            }

            sql_result DataQuery::MakeRequestFetch()
            {
                std::auto_ptr<ResultPage> resultPage(new ResultPage());

                QueryFetchRequest req(cursor->GetQueryId(), connection.GetConfiguration().GetPageSize());
                QueryFetchResponse rsp(*resultPage);

                try
                {
                    connection.SyncMessage(req, rsp);
                }
                catch (const odbc_error& err)
                {
                    diag.AddStatusRecord(err);

                    return sql_result::AI_ERROR;
                }
                catch (const IgniteError& err)
                {
                    diag.AddStatusRecord(err.GetText());

                    return sql_result::AI_ERROR;
                }

                if (rsp.get_state() != response_status::SUCCESS)
                {
                    LOG_MSG("Error: " << rsp.GetError());

                    diag.AddStatusRecord(response_status_to_sql_state(rsp.get_state()), rsp.GetError());

                    return sql_result::AI_ERROR;
                }

                LOG_MSG("Page size:    " << resultPage->get_size());
                LOG_MSG("Page is last: " << resultPage->IsLast());

                cursor->UpdateData(resultPage);

                return sql_result::AI_SUCCESS;
            }

            sql_result DataQuery::MakeRequestMoreResults()
            {
                std::auto_ptr<ResultPage> resultPage(new ResultPage());

                QueryMoreResultsRequest req(cursor->GetQueryId(), connection.GetConfiguration().GetPageSize());
                QueryMoreResultsResponse rsp(*resultPage);

                try
                {
                    connection.SyncMessage(req, rsp);
                }
                catch (const odbc_error& err)
                {
                    diag.AddStatusRecord(err);

                    return sql_result::AI_ERROR;
                }
                catch (const IgniteError& err)
                {
                    diag.AddStatusRecord(err.GetText());

                    return sql_result::AI_ERROR;
                }

                if (rsp.get_state() != response_status::SUCCESS)
                {
                    LOG_MSG("Error: " << rsp.GetError());

                    diag.AddStatusRecord(response_status_to_sql_state(rsp.get_state()), rsp.GetError());

                    return sql_result::AI_ERROR;
                }

                LOG_MSG("Page size:    " << resultPage->get_size());
                LOG_MSG("Page is last: " << resultPage->IsLast());

                cachedNextPage = resultPage;
                cursor.reset(new Cursor(rsp.GetQueryId()));

                return sql_result::AI_SUCCESS;
            }

            sql_result DataQuery::MakeRequestResultsetMeta()
            {
                const std::string& schema = connection.GetSchema();

                QueryGetResultsetMetaRequest req(schema, sql);
                QueryGetResultsetMetaResponse rsp;

                try
                {
                    connection.SyncMessage(req, rsp);
                }
                catch (const odbc_error& err)
                {
                    diag.AddStatusRecord(err);

                    return sql_result::AI_ERROR;
                }
                catch (const IgniteError& err)
                {
                    diag.AddStatusRecord(err.GetText());

                    return sql_result::AI_ERROR;
                }

                if (rsp.get_state() != response_status::SUCCESS)
                {
                    LOG_MSG("Error: " << rsp.GetError());

                    diag.AddStatusRecord(response_status_to_sql_state(rsp.get_state()), rsp.GetError());

                    return sql_result::AI_ERROR;
                }

                SetResultsetMeta(rsp.GetMeta());

                return sql_result::AI_SUCCESS;
            }

            sql_result DataQuery::ProcessConversionResult(conversion_result convRes, int32_t rowIdx,
                int32_t columnIdx)
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
                        diag.AddStatusRecord(sql_state::S01004_DATA_TRUNCATED,
                            "Buffer is too small for the column data. Truncated from the right.", rowIdx, columnIdx);

                        return sql_result::AI_SUCCESS_WITH_INFO;
                    }

                    case conversion_result::AI_FRACTIONAL_TRUNCATED:
                    {
                        diag.AddStatusRecord(sql_state::S01S07_FRACTIONAL_TRUNCATION,
                            "Buffer is too small for the column data. Fraction truncated.", rowIdx, columnIdx);

                        return sql_result::AI_SUCCESS_WITH_INFO;
                    }

                    case conversion_result::AI_INDICATOR_NEEDED:
                    {
                        diag.AddStatusRecord(sql_state::S22002_INDICATOR_NEEDED,
                            "Indicator is needed but not suplied for the column buffer.", rowIdx, columnIdx);

                        return sql_result::AI_SUCCESS_WITH_INFO;
                    }

                    case conversion_result::AI_UNSUPPORTED_CONVERSION:
                    {
                        diag.AddStatusRecord(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                            "Data conversion is not supported.", rowIdx, columnIdx);

                        return sql_result::AI_SUCCESS_WITH_INFO;
                    }

                    case conversion_result::AI_FAILURE:
                    default:
                    {
                        diag.AddStatusRecord(sql_state::S01S01_ERROR_IN_ROW,
                            "Can not retrieve row column.", rowIdx, columnIdx);

                        break;
                    }
                }

                return sql_result::AI_ERROR;
            }

            void DataQuery::SetResultsetMeta(const meta::ColumnMetaVector& value)
            {
                resultMeta.assign(value.begin(), value.end());
                resultMetaAvailable = true;

                for (size_t i = 0; i < resultMeta.size(); ++i)
                {
                    meta::ColumnMeta& meta = resultMeta.at(i);
                    LOG_MSG("\n[" << i << "] SchemaName:     " << meta.GetSchemaName()
                        <<  "\n[" << i << "] TypeName:       " << meta.GetTableName()
                        <<  "\n[" << i << "] ColumnName:     " << meta.GetColumnName()
                        <<  "\n[" << i << "] ColumnType:     " << static_cast<int32_t>(meta.GetDataType()));
                }
            }
        }
    }
}

