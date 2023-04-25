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

#ifndef _IGNITE_ODBC_STREAMING_STREAMING_CONTEXT
#define _IGNITE_ODBC_STREAMING_STREAMING_CONTEXT

#include "ignite/odbc/app/parameter_set.h"
#include "ignite/odbc/query/query.h"

#include "ignite/odbc/streaming/streaming_batch.h"

namespace ignite
{
    namespace odbc
    {
        /** Set streaming forward-declaration. */
        class SqlSetStreamingCommand;

        /** Connection forward-declaration. */
        class Connection;

        namespace streaming
        {
            /**
             * Streaming Query.
             */
            class StreamingContext
            {
            public:
                /**
                 * Default constructor.
                 */
                StreamingContext();

                /**
                 * Set connection for streaming.
                 *
                 * @param connection Connection for streaming.
                 */
                void SetConnection(Connection& connection)
                {
                    this->connection = &connection;
                }

                /**
                 * Destructor.
                 */
                ~StreamingContext();

                /**
                 * Enable streaming.
                 *
                 * @param cmd Set streaming command.
                 * @return Result.
                 */
                sql_result Enable(const SqlSetStreamingCommand& cmd);

                /**
                 * Disable streaming.
                 *
                 * @return Result.
                 */
                sql_result Disable();

                /**
                 * Check if the streaming is enabled.
                 *
                 * @return @c true if enabled.
                 */
                bool IsEnabled() const
                {
                    return enabled;
                }

                /**
                 * Execute query.
                 *
                 * @param sql SQL.
                 * @param params SQL params.
                 * @return True on success.
                 */
                sql_result Execute(const std::string& sql, const app::ParameterSet& params);

            private:
                IGNITE_NO_COPY_ASSIGNMENT(StreamingContext);

                /**
                 * Flush collected streaming data to remote server.
                 *
                 * @param last Last page indicator.
                 * @return Operation result.
                 */
                sql_result Flush(bool last);

                /**
                 * Send batch request.
                 *
                 * @param last Last page flag.
                 * @return Result.
                 */
                sql_result MakeRequestStreamingBatch(bool last);

                /** Connection associated with the statement. */
                Connection* connection;

                /** Batch size. */
                int32_t batchSize;

                /** Order. */
                int64_t order;

                /** Streaming enabled. */
                bool enabled;

                /** Current batch. */
                StreamingBatch currentBatch;
            };
        }
    }
}

#endif //_IGNITE_ODBC_STREAMING_STREAMING_CONTEXT
