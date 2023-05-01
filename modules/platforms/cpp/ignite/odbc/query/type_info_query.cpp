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

#include <cassert>

#include <ignite/impl/binary/binary_common.h>

#include "../type_traits.h"
#include "ignite/odbc/system/odbc_constants.h"
#include "type_info_query.h"

namespace
{
    struct ResultColumn
    {
        enum Type
        {
            /** Data source-dependent data-type name. */
            TYPE_NAME = 1,

            /** SQL data type. */
            DATA_TYPE,

            /** The maximum column size that the server supports for this data type. */
            COLUMN_SIZE,

            /** Character or characters used to prefix a literal. */
            LITERAL_PREFIX,

            /** Character or characters used to terminate a literal. */
            LITERAL_SUFFIX,

            /**
             * A list of keywords, separated by commas, corresponding to each
             * parameter that the application may specify in parentheses when using
             * the name that is returned in the TYPE_NAME field.
             */
            CREATE_PARAMS,

            /** Whether the data type accepts a NULL value. */
            NULLABLE,

            /**
             * Whether a character data type is case-sensitive in collations and
             * comparisons.
             */
            CASE_SENSITIVE,

            /** How the data type is used in a WHERE clause. */
            SEARCHABLE,

            /** Whether the data type is unsigned. */
            UNSIGNED_ATTRIBUTE,

            /** Whether the data type has predefined fixed precision and scale. */
            FIXED_PREC_SCALE,

            /** Whether the data type is auto-incrementing. */
            AUTO_UNIQUE_VALUE,

            /**
             * Localized version of the data source–dependent name of the data
             * type.
             */
            LOCAL_TYPE_NAME,

            /** The minimum scale of the data type on the data source. */
            MINIMUM_SCALE,

            /** The maximum scale of the data type on the data source. */
            MAXIMUM_SCALE,

            /**
             * The value of the SQL data type as it appears in the SQL_DESC_TYPE
             * field of the descriptor.
             */
            SQL_DATA_TYPE,

            /**
             * When the value of SQL_DATA_TYPE is SQL_DATETIME or SQL_INTERVAL,
             * this column contains the datetime/interval sub-code.
             */
            SQL_DATETIME_SUB,

            /**
             * If the data type is an approximate numeric type, this column
             * contains the value 2 to indicate that COLUMN_SIZE specifies a number
             * of bits.
             */
            NUM_PREC_RADIX,

            /**
             * If the data type is an interval data type, then this column contains
             * the value of the interval leading precision.
             */
            INTERVAL_PRECISION
        };
    };
}

namespace ignite
{
    namespace odbc
    {
        namespace query
        {
            TypeInfoQuery::TypeInfoQuery(DiagnosableAdapter& diag, int16_t sqlType) :
                Query(diag, QueryType::TYPE_INFO),
                columnsMeta(),
                executed(false),
                fetched(false),
                types(),
                cursor(types.end())
            {
                using namespace ignite::impl::binary;
                using namespace ignite::type_traits;

                using meta::ColumnMeta;

                columnsMeta.reserve(19);

                const std::string sch;
                const std::string tbl;

                columnsMeta.push_back(ColumnMeta(sch, tbl, "TYPE_NAME",          IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "DATA_TYPE",          IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "COLUMN_SIZE",        IGNITE_TYPE_INT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "LITERAL_PREFIX",     IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "LITERAL_SUFFIX",     IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "CREATE_PARAMS",      IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "NULLABLE",           IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "CASE_SENSITIVE",     IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "SEARCHABLE",         IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "UNSIGNED_ATTRIBUTE", IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "FIXED_PREC_SCALE",   IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "AUTO_UNIQUE_VALUE",  IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "LOCAL_TYPE_NAME",    IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "MINIMUM_SCALE",      IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "MAXIMUM_SCALE",      IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "SQL_DATA_TYPE",      IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "SQL_DATETIME_SUB",   IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "NUM_PREC_RADIX",     IGNITE_TYPE_INT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "INTERVAL_PRECISION", IGNITE_TYPE_SHORT));

                assert(is_sql_type_supported(sqlType) || sqlType == SQL_ALL_TYPES);

                if (sqlType == SQL_ALL_TYPES)
                {
                    types.push_back(IGNITE_TYPE_STRING);
                    types.push_back(IGNITE_TYPE_SHORT);
                    types.push_back(IGNITE_TYPE_INT);
                    types.push_back(IGNITE_TYPE_DECIMAL);
                    types.push_back(IGNITE_TYPE_FLOAT);
                    types.push_back(IGNITE_TYPE_DOUBLE);
                    types.push_back(IGNITE_TYPE_BOOL);
                    types.push_back(IGNITE_TYPE_BYTE);
                    types.push_back(IGNITE_TYPE_LONG);
                    types.push_back(IGNITE_TYPE_UUID);
                    types.push_back(IGNITE_TYPE_BINARY);
                }
                else
                    types.push_back(sql_type_to_ignite_type(sqlType));
            }

            TypeInfoQuery::~TypeInfoQuery()
            {
                // No-op.
            }

            sql_result TypeInfoQuery::Execute()
            {
                cursor = types.begin();

                executed = true;
                fetched = false;

                return sql_result::AI_SUCCESS;
            }

            const meta::ColumnMetaVector* TypeInfoQuery::GetMeta()
            {
                return &columnsMeta;
            }

            sql_result TypeInfoQuery::FetchNextRow(column_binding_map & columnBindings)
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

                if (cursor == types.end())
                    return sql_result::AI_NO_DATA;

                column_binding_map::iterator it;

                for (it = columnBindings.begin(); it != columnBindings.end(); ++it)
                    GetColumn(it->first, it->second);

                return sql_result::AI_SUCCESS;
            }

            sql_result TypeInfoQuery::GetColumn(uint16_t columnIdx, application_data_buffer & buffer)
            {
                using namespace ignite::impl::binary;

                if (!executed)
                {
                    diag.add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query was not executed.");

                    return sql_result::AI_ERROR;
                }

                if (cursor == types.end())
                {
                    diag.add_status_record(sql_state::S24000_INVALID_CURSOR_STATE,
                        "Cursor has reached end of the result set.");

                    return sql_result::AI_ERROR;
                }

                int8_t currentType = *cursor;

                switch (columnIdx)
                {
                    case ResultColumn::TYPE_NAME:
                    {
                        buffer.put_string(ignite_type_to_sql_type_name(currentType));

                        break;
                    }

                    case ResultColumn::DATA_TYPE:
                    case ResultColumn::SQL_DATA_TYPE:
                    {
                        buffer.put_int16(ignite_type_to_sql_type(currentType));

                        break;
                    }

                    case ResultColumn::COLUMN_SIZE:
                    {
                        buffer.put_int32(ignite_type_column_size(currentType));

                        break;
                    }

                    case ResultColumn::LITERAL_PREFIX:
                    {
                        if (currentType == IGNITE_TYPE_STRING)
                            buffer.put_string("'");
                        else if (currentType == IGNITE_TYPE_BINARY)
                            buffer.put_string("0x");
                        else
                            buffer.put_null();

                        break;
                    }

                    case ResultColumn::LITERAL_SUFFIX:
                    {
                        if (currentType == IGNITE_TYPE_STRING)
                            buffer.put_string("'");
                        else
                            buffer.put_null();

                        break;
                    }

                    case ResultColumn::CREATE_PARAMS:
                    {
                        buffer.put_null();

                        break;
                    }

                    case ResultColumn::NULLABLE:
                    {
                        buffer.put_int32(ignite_type_nullability(currentType));

                        break;
                    }

                    case ResultColumn::CASE_SENSITIVE:
                    {
                        if (currentType == IGNITE_TYPE_STRING)
                            buffer.put_int16(SQL_TRUE);
                        else
                            buffer.put_int16(SQL_FALSE);

                        break;
                    }

                    case ResultColumn::SEARCHABLE:
                    {
                        buffer.put_int16(SQL_SEARCHABLE);

                        break;
                    }

                    case ResultColumn::UNSIGNED_ATTRIBUTE:
                    {
                        buffer.put_int16(is_ignite_type_unsigned(currentType));

                        break;
                    }

                    case ResultColumn::FIXED_PREC_SCALE:
                    case ResultColumn::AUTO_UNIQUE_VALUE:
                    {
                        buffer.put_int16(SQL_FALSE);

                        break;
                    }

                    case ResultColumn::LOCAL_TYPE_NAME:
                    {
                        buffer.put_null();

                        break;
                    }

                    case ResultColumn::MINIMUM_SCALE:
                    case ResultColumn::MAXIMUM_SCALE:
                    {
                        buffer.put_int16(ignite_type_decimal_digits(currentType));

                        break;
                    }

                    case ResultColumn::SQL_DATETIME_SUB:
                    {
                        buffer.put_null();

                        break;
                    }

                    case ResultColumn::NUM_PREC_RADIX:
                    {
                        buffer.put_int32(ignite_type_num_precision_radix(currentType));

                        break;
                    }

                    case ResultColumn::INTERVAL_PRECISION:
                    {
                        buffer.put_null();

                        break;
                    }

                    default:
                        break;
                }

                return sql_result::AI_SUCCESS;
            }

            sql_result TypeInfoQuery::Close()
            {
                cursor = types.end();

                executed = false;

                return sql_result::AI_SUCCESS;
            }

            bool TypeInfoQuery::DataAvailable() const
            {
                return cursor != types.end();
            }

            int64_t TypeInfoQuery::AffectedRows() const
            {
                return 0;
            }

            sql_result TypeInfoQuery::NextResultSet()
            {
                return sql_result::AI_NO_DATA;
            }
        }
    }
}

