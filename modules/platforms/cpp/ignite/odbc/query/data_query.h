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

#ifndef _IGNITE_ODBC_QUERY_DATA_QUERY
#define _IGNITE_ODBC_QUERY_DATA_QUERY

#include "ignite/odbc/app/parameter_set.h"
#include "ignite/odbc/cursor.h"
#include "ignite/odbc/query/query.h"

namespace ignite
{
    namespace odbc
    {
        /** Connection forward-declaration. */
        class connection;

        namespace query
        {
            /**
             * Query.
             */
            class DataQuery : public Query
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param diag Diagnostics collector.
                 * @param connection Associated connection.
                 * @param sql SQL query string.
                 * @param params SQL params.
                 * @param timeout Timeout.
                 */
                DataQuery(diagnosable_adapter& diag, connection& connection, const std::string& sql,
                    const parameter_set& params, int32_t& timeout);

                /**
                 * Destructor.
                 */
                virtual ~DataQuery();

                /**
                 * Execute query.
                 *
                 * @return True on success.
                 */
                virtual sql_result Execute();

                /**
                 * Get column metadata.
                 *
                 * @return Column metadata.
                 */
                virtual const meta::column_meta_vector* GetMeta();

                /**
                 * Fetch next result row to application buffers.
                 *
                 * @param columnBindings Application buffers to put data to.
                 * @return Operation result.
                 */
                virtual sql_result FetchNextRow(column_binding_map& columnBindings);

                /**
                 * Get data of the specified column in the result set.
                 *
                 * @param columnIdx Column index.
                 * @param buffer Buffer to put column data to.
                 * @return Operation result.
                 */
                virtual sql_result GetColumn(uint16_t columnIdx, application_data_buffer& buffer);

                /**
                 * Close query.
                 *
                 * @return Result.
                 */
                virtual sql_result Close();

                /**
                 * Check if data is available.
                 *
                 * @return True if data is available.
                 */
                virtual bool DataAvailable() const;

                /**
                 * Get number of rows affected by the statement.
                 *
                 * @return Number of rows affected by the statement.
                 */
                virtual int64_t AffectedRows() const;

                /**
                 * Move to the next result set.
                 *
                 * @return Operaion result.
                 */
                virtual sql_result NextResultSet();

                /**
                 * Get SQL query string.
                 *
                 * @return SQL query string.
                 */
                const std::string& GetSql() const
                {
                    return sql;
                }

            private:
                IGNITE_NO_COPY_ASSIGNMENT(DataQuery);

                /**
                 * Check whether all cursors are closed remotely.
                 *
                 * @return true, if all cursors closed remotely.
                 */
                bool IsClosedRemotely() const;

                /**
                 * Make query prepare request and use response to set internal
                 * state.
                 *
                 * @return Result.
                 */
                sql_result MakeRequestPrepare();

                /**
                 * Make query execute request and use response to set internal
                 * state.
                 *
                 * @return Result.
                 */
                sql_result MakeRequestExecute();

                /**
                 * Make query close request.
                 *
                 * @return Result.
                 */
                sql_result MakeRequestClose();

                /**
                 * Make data fetch request and use response to set internal state.
                 *
                 * @return Result.
                 */
                sql_result MakeRequestFetch();

                /**
                 * Make next result set request and use response to set internal state.
                 *
                 * @return Result.
                 */
                sql_result MakeRequestMoreResults();

                /**
                 * Make result set metadata request.
                 *
                 * @return Result.
                 */
                sql_result MakeRequestResultsetMeta();

                /**
                 * Process column conversion operation result.
                 *
                 * @param convRes Conversion result.
                 * @param rowIdx Row index.
                 * @param columnIdx Column index.
                 * @return General SQL result.
                 */
                sql_result ProcessConversionResult(conversion_result convRes, int32_t rowIdx,
                    int32_t columnIdx);;

                /**
                 * Process column conversion operation result.
                 *
                 * @param convRes Conversion result.
                 * @param rowIdx Row index.
                 * @param columnIdx Column index.
                 * @return General SQL result.
                 */
                void SetResultsetMeta(const meta::column_meta_vector& value);

                /**
                 * Close query.
                 *
                 * @return Result.
                 */
                sql_result InternalClose();

                /** Connection associated with the statement. */
                connection& connection;

                /** SQL Query. */
                std::string sql;

                /** parameter bindings. */
                const parameter_set& params;

                /** Result set metadata is available */
                bool resultMetaAvailable;

                /** Result set metadata. */
                meta::column_meta_vector resultMeta;

                /** Cursor. */
                std::auto_ptr<Cursor> cursor;

                /** Number of rows affected. */
                std::vector<int64_t> rows_affected;

                /** Rows affected index. */
                size_t rowsAffectedIdx;

                /** Cached next result page. */
                std::auto_ptr<ResultPage> cachedNextPage;

                /** Timeout. */
                int32_t& timeout;
            };
        }
    }
}

#endif //_IGNITE_ODBC_QUERY_DATA_QUERY
