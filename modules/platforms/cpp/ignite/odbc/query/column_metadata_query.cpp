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

#include "../connection.h"
#include "../log.h"
#include "../message.h"
#include "../odbc_error.h"
#include "../type_traits.h"
#include "column_metadata_query.h"

namespace
{
    struct ResultColumn
    {
        enum Type
        {
            /** Catalog name. NULL if not applicable to the data source. */
            TABLE_CAT = 1,

            /** Schema name. NULL if not applicable to the data source. */
            TABLE_SCHEM,

            /** Table name. */
            TABLE_NAME,

            /** Column name. */
            COLUMN_NAME,

            /** SQL data type. */
            DATA_TYPE,

            /** Data source-dependent data type name. */
            TYPE_NAME,

            /** Column size. */
            COLUMN_SIZE,

            /** The length in bytes of data transferred on fetch. */
            BUFFER_LENGTH,

            /** The total number of significant digits to the right of the decimal point. */
            DECIMAL_DIGITS,

            /** Precision. */
            NUM_PREC_RADIX,

            /** Nullability of the data in column. */
            NULLABLE,

            /** A description of the column. */
            REMARKS
        };
    };
}

namespace ignite
{
    namespace odbc
    {
        namespace query
        {
            ColumnMetadataQuery::ColumnMetadataQuery(diagnosable_adapter& diag,
                connection& connection, const std::string& schema,
                const std::string& table, const std::string& column) :
                Query(diag, QueryType::COLUMN_METADATA),
                connection(connection),
                schema(schema),
                table(table),
                column(column),
                executed(false),
                fetched(false),
                meta(),
                columnsMeta()
            {
                using namespace ignite::impl::binary;
                using namespace ignite::type_traits;

                using meta::column_meta;

                columnsMeta.reserve(12);

                const std::string sch;
                const std::string tbl;

                columnsMeta.push_back(column_meta(sch, tbl, "TABLE_CAT",      IGNITE_TYPE_STRING));
                columnsMeta.push_back(column_meta(sch, tbl, "TABLE_SCHEM",    IGNITE_TYPE_STRING));
                columnsMeta.push_back(column_meta(sch, tbl, "TABLE_NAME",     IGNITE_TYPE_STRING));
                columnsMeta.push_back(column_meta(sch, tbl, "COLUMN_NAME",    IGNITE_TYPE_STRING));
                columnsMeta.push_back(column_meta(sch, tbl, "DATA_TYPE",      IGNITE_TYPE_SHORT));
                columnsMeta.push_back(column_meta(sch, tbl, "TYPE_NAME",      IGNITE_TYPE_STRING));
                columnsMeta.push_back(column_meta(sch, tbl, "COLUMN_SIZE",    IGNITE_TYPE_INT));
                columnsMeta.push_back(column_meta(sch, tbl, "BUFFER_LENGTH",  IGNITE_TYPE_INT));
                columnsMeta.push_back(column_meta(sch, tbl, "DECIMAL_DIGITS", IGNITE_TYPE_SHORT));
                columnsMeta.push_back(column_meta(sch, tbl, "NUM_PREC_RADIX", IGNITE_TYPE_SHORT));
                columnsMeta.push_back(column_meta(sch, tbl, "NULLABLE",       IGNITE_TYPE_SHORT));
                columnsMeta.push_back(column_meta(sch, tbl, "REMARKS",        IGNITE_TYPE_STRING));
            }

            ColumnMetadataQuery::~ColumnMetadataQuery()
            {
                // No-op.
            }

            sql_result ColumnMetadataQuery::Execute()
            {
                if (executed)
                    Close();

                sql_result result = MakeRequestGetColumnsMeta();

                if (result == sql_result::AI_SUCCESS)
                {
                    executed = true;
                    fetched = false;

                    cursor = meta.begin();
                }

                return result;
            }

            const meta::column_meta_vector* ColumnMetadataQuery::GetMeta()
            {
                return &columnsMeta;
            }

            sql_result ColumnMetadataQuery::FetchNextRow(column_binding_map & columnBindings)
            {
                if (!executed)
                {
                    diag.add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query was not executed.");

                    return sql_result::AI_ERROR;
                }

                if (!fetched)
                    fetched = true;
                else
                    ++cursor;

                if (cursor == meta.end())
                    return sql_result::AI_NO_DATA;

                column_binding_map::iterator it;

                for (it = columnBindings.begin(); it != columnBindings.end(); ++it)
                    GetColumn(it->first, it->second);

                return sql_result::AI_SUCCESS;
            }

            sql_result ColumnMetadataQuery::GetColumn(uint16_t columnIdx, application_data_buffer & buffer)
            {
                if (!executed)
                {
                    diag.add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query was not executed.");

                    return sql_result::AI_ERROR;
                }

                if (cursor == meta.end())
                {
                    diag.add_status_record(sql_state::S24000_INVALID_CURSOR_STATE,
                        "Cursor has reached end of the result set.");

                    return sql_result::AI_ERROR;
                }

                const meta::column_meta& currentColumn = *cursor;
                uint8_t columnType = currentColumn.get_data_type();

                switch (columnIdx)
                {
                    case ResultColumn::TABLE_CAT:
                    {
                        buffer.put_null();
                        break;
                    }

                    case ResultColumn::TABLE_SCHEM:
                    {
                        buffer.put_string(currentColumn.get_schema_name());
                        break;
                    }

                    case ResultColumn::TABLE_NAME:
                    {
                        buffer.put_string(currentColumn.get_table_name());
                        break;
                    }

                    case ResultColumn::COLUMN_NAME:
                    {
                        buffer.put_string(currentColumn.get_column_name());
                        break;
                    }

                    case ResultColumn::DATA_TYPE:
                    {
                        buffer.put_int16(ignite_type_to_sql_type(columnType));
                        break;
                    }

                    case ResultColumn::TYPE_NAME:
                    {
                        buffer.put_string(ignite_type_to_sql_type_name(currentColumn.get_data_type()));
                        break;
                    }

                    case ResultColumn::COLUMN_SIZE:
                    {
                        buffer.put_int16(ignite_type_column_size(columnType));
                        break;
                    }

                    case ResultColumn::BUFFER_LENGTH:
                    {
                        buffer.put_int16(ignite_type_transfer_length(columnType));
                        break;
                    }

                    case ResultColumn::DECIMAL_DIGITS:
                    {
                        int32_t decDigits = ignite_type_decimal_digits(columnType);
                        if (decDigits < 0)
                            buffer.put_null();
                        else
                            buffer.put_int16(static_cast<int16_t>(decDigits));
                        break;
                    }

                    case ResultColumn::NUM_PREC_RADIX:
                    {
                        buffer.put_int16(ignite_type_num_precision_radix(columnType));
                        break;
                    }

                    case ResultColumn::NULLABLE:
                    {
                        buffer.put_int16(ignite_type_nullability(columnType));
                        break;
                    }

                    case ResultColumn::REMARKS:
                    {
                        buffer.put_null();
                        break;
                    }

                    default:
                        break;
                }

                return sql_result::AI_SUCCESS;
            }

            sql_result ColumnMetadataQuery::Close()
            {
                meta.clear();

                executed = false;

                return sql_result::AI_SUCCESS;
            }

            bool ColumnMetadataQuery::DataAvailable() const
            {
                return cursor != meta.end();
            }

            int64_t ColumnMetadataQuery::AffectedRows() const
            {
                return 0;
            }

            sql_result ColumnMetadataQuery::NextResultSet()
            {
                return sql_result::AI_NO_DATA;
            }

            sql_result ColumnMetadataQuery::MakeRequestGetColumnsMeta()
            {
                QueryGetColumnsMetaRequest req(schema, table, column);
                QueryGetColumnsMetaResponse rsp;

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

                meta = rsp.GetMeta();

                for (size_t i = 0; i < meta.size(); ++i)
                {
                    LOG_MSG("\n[" << i << "] SchemaName:     " << meta[i].get_schema_name()
                         << "\n[" << i << "] TableName:      " << meta[i].get_table_name()
                         << "\n[" << i << "] ColumnName:     " << meta[i].get_column_name()
                         << "\n[" << i << "] ColumnType:     " << static_cast<int32_t>(meta[i].get_data_type()));
                }

                return sql_result::AI_SUCCESS;
            }
        }
    }
}

