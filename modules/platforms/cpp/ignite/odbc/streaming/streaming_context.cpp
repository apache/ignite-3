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

#include "../connection.h"
#include "../log.h"
#include "../message.h"
#include "../sql/sql_set_streaming_command.h"

#include "streaming_context.h"

namespace ignite
{
    namespace odbc
    {
        namespace streaming
        {
            StreamingContext::StreamingContext() :
                connection(0),
                batchSize(0),
                order(0),
                enabled(false),
                currentBatch()
            {
                // No-op.
            }

            StreamingContext::~StreamingContext()
            {
                // No-op.
            }

            sql_result StreamingContext::Enable(const SqlSetStreamingCommand& cmd)
            {
                sql_result res = sql_result::AI_SUCCESS;

                if (enabled)
                    res = Disable();

                if (res != sql_result::AI_SUCCESS)
                    return res;

                batchSize = cmd.GetBatchSize();

                enabled = true;

                order = 0;

                return sql_result::AI_SUCCESS;
            }

            sql_result StreamingContext::Disable()
            {
                LOG_MSG("Disabling streaming context.");

                sql_result res = sql_result::AI_SUCCESS;

                if (enabled)
                    res = Flush(true);

                enabled = false;

                return res;
            }

            sql_result StreamingContext::Execute(const std::string& sql, const app::ParameterSet& params)
            {
                assert(enabled);

                currentBatch.AddRow(sql, params);

                if (currentBatch.GetSize() < batchSize)
                    return sql_result::AI_SUCCESS;

                return Flush(false);
            }

            sql_result StreamingContext::Flush(bool last)
            {
                LOG_MSG("Flushing data");

                if (currentBatch.GetSize() == 0 && !last)
                    return sql_result::AI_SUCCESS;

                sql_result res = MakeRequestStreamingBatch(last);

                currentBatch.Clear();

                return res;
            }

            sql_result StreamingContext::MakeRequestStreamingBatch(bool last)
            {
                assert(connection != 0);

                const std::string& schema = connection->GetSchema();

                StreamingBatchRequest req(schema, currentBatch, last, order);
                StreamingBatchResponse rsp;

                try
                {
                    connection->SyncMessage(req, rsp);
                }
                catch (const odbc_error& err)
                {
                    connection->AddStatusRecord(err);

                    return sql_result::AI_ERROR;
                }
                catch (const IgniteError& err)
                {
                    connection->AddStatusRecord(err.GetText());

                    return sql_result::AI_ERROR;
                }

                currentBatch.Clear();

                if (rsp.get_state() != response_status::SUCCESS)
                {
                    LOG_MSG("Error: " << rsp.GetError());

                    connection->AddStatusRecord(response_status_to_sql_state(rsp.get_state()), rsp.GetError());

                    return sql_result::AI_ERROR;
                }

                if (rsp.GetErrorCode() != response_status::SUCCESS)
                {
                    LOG_MSG("Error: " << rsp.get_error_message());

                    connection->AddStatusRecord(response_status_to_sql_state(rsp.GetErrorCode()), rsp.get_error_message());

                    return sql_result::AI_ERROR;
                }

                assert(order == rsp.GetOrder());

                ++order;

                return sql_result::AI_SUCCESS;
            }
        }
    }
}

