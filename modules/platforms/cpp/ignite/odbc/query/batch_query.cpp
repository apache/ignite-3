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

#include "batch_query.h"
#include "../connection.h"
#include "../log.h"
#include "../message.h"
#include "../odbc_error.h"

namespace ignite
{
    namespace odbc
    {
        namespace query
        {
            BatchQuery::BatchQuery(DiagnosableAdapter& diag, Connection& connection, const std::string& sql,
                const parameter_set& params, int32_t& timeout) :
                Query(diag, QueryType::BATCH),
                connection(connection),
                sql(sql),
                params(params),
                resultMeta(),
                rowsAffected(),
                rowsAffectedIdx(0),
                executed(false),
                timeout(timeout)
            {
                // No-op.
            }

            BatchQuery::~BatchQuery()
            {
                // No-op.
            }

            sql_result BatchQuery::Execute()
            {
                if (executed)
                    Close();

                int32_t maxPageSize = connection.GetConfiguration().GetPageSize();
                int32_t row_num = params.get_param_set_size();
                sql_result res;

                int32_t processed = 0;

                rowsAffected.clear();
                rowsAffected.reserve(static_cast<size_t>(params.get_param_set_size()));

                do {
                    int32_t currentPageSize = std::min(maxPageSize, row_num - processed);
                    bool lastPage = currentPageSize == row_num - processed;

                    res = MakeRequestExecuteBatch(processed, processed + currentPageSize, lastPage);

                    processed += currentPageSize;
                } while ((res == sql_result::AI_SUCCESS || res == sql_result::AI_SUCCESS_WITH_INFO) && processed < row_num);

                params.set_params_processed(static_cast<SQLULEN>(rowsAffected.size()));

                return res;
            }

            const meta::ColumnMetaVector* BatchQuery::GetMeta()
            {
                return &resultMeta;
            }

            sql_result BatchQuery::FetchNextRow(column_binding_map&)
            {
                if (!executed)
                {
                    diag.AddStatusRecord(sql_state::SHY010_SEQUENCE_ERROR, "Query was not executed.");

                    return sql_result::AI_ERROR;
                }

                return sql_result::AI_NO_DATA;
            }

            sql_result BatchQuery::GetColumn(uint16_t, application_data_buffer&)
            {
                if (!executed)
                {
                    diag.AddStatusRecord(sql_state::SHY010_SEQUENCE_ERROR, "Query was not executed.");

                    return sql_result::AI_ERROR;
                }

                diag.AddStatusRecord(sql_state::S24000_INVALID_CURSOR_STATE,
                    "Cursor has reached end of the result set.");

                return sql_result::AI_ERROR;
            }

            sql_result BatchQuery::Close()
            {
                executed = false;
                rowsAffected.clear();
                rowsAffectedIdx = 0;

                return sql_result::AI_SUCCESS;
            }

            bool BatchQuery::DataAvailable() const
            {
                return false;
            }

            int64_t BatchQuery::AffectedRows() const
            {
                int64_t affected = rowsAffectedIdx < rowsAffected.size() ? rowsAffected[rowsAffectedIdx] : 0;
                return affected < 0 ? 0 : affected;
            }

            sql_result BatchQuery::NextResultSet()
            {
                if (rowsAffectedIdx + 1 >= rowsAffected.size())
                {
                    Close();
                    return sql_result::AI_NO_DATA;
                }

                ++rowsAffectedIdx;

                return sql_result::AI_SUCCESS;
            }

            sql_result BatchQuery::MakeRequestExecuteBatch(SQLULEN begin, SQLULEN end, bool last)
            {
                const std::string& schema = connection.GetSchema();

                QueryExecuteBatchRequest req(schema, sql, params, begin, end, last, timeout, connection.IsAutoCommit());
                QueryExecuteBatchResponse rsp;

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

                const std::vector<int64_t>& rowsLastTime = rsp.GetAffectedRows();

                for (size_t i = 0; i < rowsLastTime.size(); ++i)
                {
                    int64_t idx = static_cast<int64_t>(i + rowsAffected.size());

                    params.set_params_status(idx, rowsLastTime[i] < 0 ? SQL_PARAM_ERROR : SQL_PARAM_SUCCESS);
                }

                rowsAffected.insert(rowsAffected.end(), rowsLastTime.begin(), rowsLastTime.end());
                LOG_MSG("Affected rows list size: " << rowsAffected.size());

                if (!rsp.get_error_message().empty())
                {
                    LOG_MSG("Error: " << rsp.get_error_message());
                    LOG_MSG("Sets Processed: " << rowsAffected.size());

                    diag.AddStatusRecord(response_status_to_sql_state(rsp.GetErrorCode()), rsp.get_error_message(),
                        static_cast<int32_t>(rowsAffected.size()), 0);

                    return sql_result::AI_SUCCESS_WITH_INFO;
                }

                return sql_result::AI_SUCCESS;
            }
        }
    }
}

