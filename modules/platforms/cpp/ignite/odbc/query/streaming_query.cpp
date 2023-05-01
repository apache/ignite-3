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

#include "streaming_query.h"
#include "../connection.h"
#include "../log.h"
#include "../message.h"
#include "../sql/sql_set_streaming_command.h"

namespace ignite
{
    namespace odbc
    {
        namespace query
        {
            StreamingQuery::StreamingQuery(
                DiagnosableAdapter& diag,
                Connection& connection,
                const parameter_set& params) :
                Query(diag, QueryType::STREAMING),
                connection(connection),
                params(params)
            {
                // No-op.
            }

            StreamingQuery::~StreamingQuery()
            {
                // No-op.
            }

            sql_result StreamingQuery::Execute()
            {
                return connection.GetStreamingContext().Execute(sql, params);
            }

            sql_result StreamingQuery::FetchNextRow(column_binding_map&)
            {
                return sql_result::AI_NO_DATA;
            }

            sql_result StreamingQuery::GetColumn(uint16_t, application_data_buffer&)
            {
                diag.AddStatusRecord(sql_state::S24000_INVALID_CURSOR_STATE, "Column is not available.");

                return sql_result::AI_ERROR;
            }

            sql_result StreamingQuery::Close()
            {
                return sql_result::AI_SUCCESS;
            }

            bool StreamingQuery::DataAvailable() const
            {
                return false;
            }

            int64_t StreamingQuery::AffectedRows() const
            {
                return 0;
            }

            sql_result StreamingQuery::NextResultSet()
            {
                return sql_result::AI_NO_DATA;
            }
        }
    }
}

