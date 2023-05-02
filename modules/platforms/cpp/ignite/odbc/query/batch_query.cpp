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
            BatchQuery::BatchQuery(diagnosable_adapter& diag, connection& connection, const std::string& sql,
                const parameter_set& params, int32_t& timeout) :
                Query(diag, QueryType::BATCH),
                connection(connection),
                sql(sql),
                params(params),
                resultMeta(),
                rows_affected(),
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

                int32_t maxPageSize = connection.GetConfiguration().get_page_size();
                int32_t row_num = params.get_param_set_size();
                sql_result res;

                int32_t processed = 0;

                rows_affected.clear();
                rows_affected.reserve(static_cast<size_t>(params.get_param_set_size()));

                do {
                    int32_t currentPageSize = std::min(maxPageSize, row_num - processed);
                    bool lastPage = currentPageSize == row_num - processed;

                    res = MakeRequestExecuteBatch(processed, processed + currentPageSize, lastPage);

                    processed += currentPageSize;
                } while ((res == sql_result::AI_SUCCESS || res == sql_result::AI_SUCCESS_WITH_INFO) && processed < row_num);

                params.set_params_processed(static_cast<SQLULEN>(rows_affected.size()));

                return res;
            }

            const meta::column_meta_vector* BatchQuery::GetMeta()
            {
                return &resultMeta;
            }

            sql_result BatchQuery::FetchNextRow(column_binding_map&)
            {
                if (!executed)
                {
                    diag.add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query was not executed.");

                    return sql_result::AI_ERROR;
                }

                return sql_result::AI_NO_DATA;
            }

            sql_result BatchQuery::GetColumn(uint16_t, application_data_buffer&)
            {
                if (!executed)
                {
                    diag.add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query was not executed.");

                    return sql_result::AI_ERROR;
                }

                diag.add_status_record(sql_state::S24000_INVALID_CURSOR_STATE,
                    "Cursor has reached end of the result set.");

                return sql_result::AI_ERROR;
            }

            sql_result BatchQuery::Close()
            {
                executed = false;
                rows_affected.clear();
                rowsAffectedIdx = 0;

                return sql_result::AI_SUCCESS;
            }

            bool BatchQuery::DataAvailable() const
            {
                return false;
            }

            int64_t BatchQuery::AffectedRows() const
            {
                int64_t affected = rowsAffectedIdx < rows_affected.size() ? rows_affected[rowsAffectedIdx] : 0;
                return affected < 0 ? 0 : affected;
            }

            sql_result BatchQuery::NextResultSet()
            {
                if (rowsAffectedIdx + 1 >= rows_affected.size())
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
                    diag.add_status_record(err);

                    return sql_result::AI_ERROR;
                }
                catch (const IgniteError& err)
                {
                    diag.add_status_record(err.GetText());

                    return sql_result::AI_ERROR;
                }

                if (rsp.get_state() != response_status::SUCCESS)
                {
                    LOG_MSG("Error: " << rsp.GetError());

                    diag.add_status_record(response_status_to_sql_state(rsp.get_state()), rsp.GetError());

                    return sql_result::AI_ERROR;
                }

                const std::vector<int64_t>& rowsLastTime = rsp.GetAffectedRows();

                for (size_t i = 0; i < rowsLastTime.size(); ++i)
                {
                    int64_t idx = static_cast<int64_t>(i + rows_affected.size());

                    params.set_params_status(idx, rowsLastTime[i] < 0 ? SQL_PARAM_ERROR : SQL_PARAM_SUCCESS);
                }

                rows_affected.insert(rows_affected.end(), rowsLastTime.begin(), rowsLastTime.end());
                LOG_MSG("Affected rows list size: " << rows_affected.size());

                if (!rsp.get_error_message().empty())
                {
                    LOG_MSG("Error: " << rsp.get_error_message());
                    LOG_MSG("Sets Processed: " << rows_affected.size());

                    diag.add_status_record(response_status_to_sql_state(rsp.GetErrorCode()), rsp.get_error_message(),
                        static_cast<int32_t>(rows_affected.size()), 0);

                    return sql_result::AI_SUCCESS_WITH_INFO;
                }

                return sql_result::AI_SUCCESS;
            }
        }
    }
}

