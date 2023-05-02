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

#ifndef _IGNITE_ODBC_QUERY_QUERY
#define _IGNITE_ODBC_QUERY_QUERY

#include <stdint.h>

#include <map>

#include "ignite/odbc/common_types.h"
#include "ignite/odbc/diagnostic/diagnosable_adapter.h"
#include "ignite/odbc/meta/column_meta.h"
#include "ignite/odbc/row.h"

namespace ignite
{
    namespace odbc
    {
        namespace query
        {
            /** Query type. */
            struct QueryType
            {
                enum Type
                {
                    /** Column metadata query type. */
                    COLUMN_METADATA,

                    /** Data query type. */
                    DATA,

                    /** Batch query type. */
                    BATCH,

                    /** Streaming query type. */
                    STREAMING,

                    /** Foreign keys query type. */
                    FOREIGN_KEYS,

                    /** Primary keys query type. */
                    PRIMARY_KEYS,

                    /** Special columns query type. */
                    SPECIAL_COLUMNS,

                    /** Table metadata query type. */
                    TABLE_METADATA,

                    /** Type info query type. */
                    TYPE_INFO,

                    /** Internal query, that should be parsed by a driver itself. */
                    INTERNAL
                };
            };

            /**
             * Query.
             */
            class Query
            {
            public:
                /**
                 * Destructor.
                 */
                virtual ~Query()
                {
                    // No-op.
                }

                /**
                 * Execute query.
                 *
                 * @return True on success.
                 */
                virtual sql_result Execute() = 0;

                /**
                 * Fetch next result row to application buffers.
                 *
                 * @param columnBindings Application buffers to put data to.
                 * @return Operation result.
                 */
                virtual sql_result FetchNextRow(column_binding_map& columnBindings) = 0;

                /**
                 * Get data of the specified column in the result set.
                 *
                 * @param columnIdx Column index.
                 * @param buffer Buffer to put column data to.
                 * @return Operation result.
                 */
                virtual sql_result GetColumn(uint16_t columnIdx, application_data_buffer& buffer) = 0;

                /**
                 * Close query.
                 *
                 * @return True on success.
                 */
                virtual sql_result Close() = 0;

                /**
                 * Get column metadata.
                 *
                 * @return Column metadata.
                 */
                virtual const meta::column_meta_vector* GetMeta()
                {
                    static const meta::column_meta_vector empty;

                    return &empty;
                }

                /**
                 * Check if data is available.
                 *
                 * @return True if data is available.
                 */
                virtual bool DataAvailable() const = 0;

                /**
                 * Get number of rows affected by the statement.
                 *
                 * @return Number of rows affected by the statement.
                 */
                virtual int64_t AffectedRows() const = 0;

                /**
                 * Move to the next result set.
                 *
                 * @return Operation result.
                 */
                virtual sql_result NextResultSet() = 0;

                /**
                 * Get query type.
                 *
                 * @return Query type.
                 */
                QueryType::Type get_type() const
                {
                    return type;
                }

            protected:
                /**
                 * Constructor.
                 */
                Query(diagnosable_adapter& diag, QueryType::Type type) :
                    diag(diag),
                    type(type)
                {
                    // No-op.
                }

                /** Diagnostics collector. */
                diagnosable_adapter& diag;

                /** Query type. */
                QueryType::Type type;
            };
        }
    }
}

#endif //_IGNITE_ODBC_QUERY_QUERY
