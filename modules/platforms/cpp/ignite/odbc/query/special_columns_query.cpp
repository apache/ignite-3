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

#include <ignite/impl/binary/binary_common.h>

#include "../type_traits.h"
#include "special_columns_query.h"

namespace ignite
{
    namespace odbc
    {
        namespace query
        {
            SpecialColumnsQuery::SpecialColumnsQuery(diagnosable_adapter& diag,
                int16_t type, const std::string& catalog, const std::string& schema,
                const std::string& table, int16_t scope, int16_t nullable) :
                Query(diag, QueryType::SPECIAL_COLUMNS),
                type(type),
                catalog(catalog),
                schema(schema),
                table(table),
                scope(scope),
                nullable(nullable),
                executed(false),
                columnsMeta()
            {
                using namespace ignite::impl::binary;
                using namespace ignite::type_traits;

                using meta::column_meta;

                columnsMeta.reserve(8);

                const std::string sch("");
                const std::string tbl("");

                columnsMeta.push_back(column_meta(sch, tbl, "SCOPE",          IGNITE_TYPE_SHORT));
                columnsMeta.push_back(column_meta(sch, tbl, "COLUMN_NAME",    IGNITE_TYPE_STRING));
                columnsMeta.push_back(column_meta(sch, tbl, "DATA_TYPE",      IGNITE_TYPE_SHORT));
                columnsMeta.push_back(column_meta(sch, tbl, "TYPE_NAME",      IGNITE_TYPE_STRING));
                columnsMeta.push_back(column_meta(sch, tbl, "COLUMN_SIZE",    IGNITE_TYPE_INT));
                columnsMeta.push_back(column_meta(sch, tbl, "BUFFER_LENGTH",  IGNITE_TYPE_INT));
                columnsMeta.push_back(column_meta(sch, tbl, "DECIMAL_DIGITS", IGNITE_TYPE_SHORT));
                columnsMeta.push_back(column_meta(sch, tbl, "PSEUDO_COLUMN",  IGNITE_TYPE_SHORT));
            }

            SpecialColumnsQuery::~SpecialColumnsQuery()
            {
                // No-op.
            }

            sql_result SpecialColumnsQuery::Execute()
            {
                executed = true;

                return sql_result::AI_SUCCESS;
            }

            const meta::column_meta_vector* SpecialColumnsQuery::GetMeta()
            {
                return &columnsMeta;
            }

            sql_result SpecialColumnsQuery::FetchNextRow(column_binding_map&)
            {
                if (!executed)
                {
                    diag.add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query was not executed.");

                    return sql_result::AI_ERROR;
                }

                return sql_result::AI_NO_DATA;
            }

            sql_result SpecialColumnsQuery::GetColumn(uint16_t, application_data_buffer&)
            {
                if (!executed)
                {
                    diag.add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query was not executed.");

                    return sql_result::AI_ERROR;
                }

                return sql_result::AI_NO_DATA;
            }

            sql_result SpecialColumnsQuery::Close()
            {
                executed = false;

                return sql_result::AI_SUCCESS;
            }

            bool SpecialColumnsQuery::DataAvailable() const
            {
                return false;
            }

            int64_t SpecialColumnsQuery::AffectedRows() const
            {
                return 0;
            }

            sql_result SpecialColumnsQuery::NextResultSet()
            {
                return sql_result::AI_NO_DATA;
            }
        }
    }
}
