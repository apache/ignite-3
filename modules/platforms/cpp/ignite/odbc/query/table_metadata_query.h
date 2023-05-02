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

#ifndef _IGNITE_ODBC_QUERY_TABLE_METADATA_QUERY
#define _IGNITE_ODBC_QUERY_TABLE_METADATA_QUERY

#include "ignite/odbc/meta/table_meta.h"
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
            class TableMetadataQuery : public Query
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param diag Diagnostics collector.
                 * @param connection Associated connection.
                 * @param catalog Catalog search pattern.
                 * @param schema Schema search pattern.
                 * @param table Table search pattern.
                 * @param tableType Table type search pattern.
                 */
                TableMetadataQuery(diagnosable_adapter& diag, connection& connection,
                    const std::string& catalog, const std::string& schema,
                    const std::string& table, const std::string& tableType);

                /**
                 * Destructor.
                 */
                virtual ~TableMetadataQuery();

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
                 * @return True on success.
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
                 * @return Operation result.
                 */
                virtual sql_result NextResultSet();

            private:
                IGNITE_NO_COPY_ASSIGNMENT(TableMetadataQuery);

                /**
                 * Make get columns metadata requets and use response to set internal state.
                 *
                 * @return True on success.
                 */
                sql_result MakeRequestGetTablesMeta();

                /** Connection associated with the statement. */
                connection& connection;

                /** Catalog search pattern. */
                std::string catalog;

                /** Schema search pattern. */
                std::string schema;

                /** Table search pattern. */
                std::string table;

                /** Table type search pattern. */
                std::string tableType;

                /** Query executed. */
                bool executed;

                /** Fetched flag. */
                bool fetched;

                /** Fetched metadata. */
                meta::TableMetaVector meta;

                /** Metadata cursor. */
                meta::TableMetaVector::iterator cursor;

                /** Columns metadata. */
                meta::column_meta_vector columnsMeta;
            };
        }
    }
}

#endif //_IGNITE_ODBC_QUERY_TABLE_METADATA_QUERY