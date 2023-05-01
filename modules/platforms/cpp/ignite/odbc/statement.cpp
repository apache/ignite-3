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

#include <limits>

#include "connection.h"
#include "ignite/odbc/system/odbc_constants.h"
#include "log.h"
#include "message.h"
#include "odbc_error.h"
#include "query/batch_query.h"
#include "query/column_metadata_query.h"
#include "query/data_query.h"
#include "query/foreign_keys_query.h"
#include "query/internal_query.h"
#include "query/primary_keys_query.h"
#include "query/special_columns_query.h"
#include "query/streaming_query.h"
#include "query/table_metadata_query.h"
#include "query/type_info_query.h"
#include "sql/sql_parser.h"
#include "sql/sql_set_streaming_command.h"
#include "sql/sql_utils.h"
#include "statement.h"
#include "utility.h"

namespace ignite
{
    namespace odbc
    {
        Statement::Statement(Connection& parent) :
            connection(parent),
            columnBindings(),
            currentQuery(),
            rowsFetched(0),
            rowStatuses(0),
            columnBindOffset(0),
            rowArraySize(1),
            m_parameters(),
            timeout(0)
        {
            // No-op.
        }

        Statement::~Statement()
        {
            // No-op.
        }

        void Statement::BindColumn(uint16_t columnIdx, int16_t targetType, void* targetValue, SQLLEN bufferLength, SQLLEN* strLengthOrIndicator)
        {
            IGNITE_ODBC_API_CALL(InternalBindColumn(columnIdx, targetType, targetValue, bufferLength, strLengthOrIndicator));
        }

        sql_result Statement::InternalBindColumn(uint16_t columnIdx, int16_t targetType, void* targetValue, SQLLEN bufferLength, SQLLEN* strLengthOrIndicator)
        {
            using namespace type_traits;
            odbc_native_type driverType = to_driver_type(targetType);

            if (driverType == odbc_native_type::AI_UNSUPPORTED)
            {
                add_status_record(sql_state::SHY003_INVALID_APPLICATION_BUFFER_TYPE, "The argument TargetType was not a valid data type.");

                return sql_result::AI_ERROR;
            }

            if (bufferLength < 0)
            {
                add_status_record(sql_state::SHY090_INVALID_STRING_OR_BUFFER_LENGTH,
                    "The value specified for the argument BufferLength was less than 0.");

                return sql_result::AI_ERROR;
            }

            if (targetValue || strLengthOrIndicator)
            {
                application_data_buffer dataBuffer(driverType, targetValue, bufferLength, strLengthOrIndicator);

                SafeBindColumn(columnIdx, dataBuffer);
            }
            else
                SafeUnbindColumn(columnIdx);

            return sql_result::AI_SUCCESS;
        }

        void Statement::SafeBindColumn(uint16_t columnIdx, const application_data_buffer& buffer)
        {
            columnBindings[columnIdx] = buffer;
        }

        void Statement::SafeUnbindColumn(uint16_t columnIdx)
        {
            columnBindings.erase(columnIdx);
        }

        void Statement::SafeUnbindAllColumns()
        {
            columnBindings.clear();
        }

        void Statement::SetColumnBindOffsetPtr(int * ptr)
        {
            columnBindOffset = ptr;
        }

        int* Statement::GetColumnBindOffsetPtr()
        {
            return columnBindOffset;
        }

        int32_t Statement::get_column_number()
        {
            int32_t res;

            IGNITE_ODBC_API_CALL(InternalGetColumnNumber(res));

            return res;
        }

        sql_result Statement::InternalGetColumnNumber(int32_t &res)
        {
            const meta::ColumnMetaVector* meta = GetMeta();

            if (!meta)
                return sql_result::AI_ERROR;

            res = static_cast<int32_t>(meta->size());

            return sql_result::AI_SUCCESS;
        }

        void Statement::bind_parameter(uint16_t paramIdx, int16_t ioType, int16_t bufferType, int16_t paramSqlType,
            SQLULEN columnSize, int16_t decDigits, void* buffer, SQLLEN bufferLen, SQLLEN* resLen)
        {
            IGNITE_ODBC_API_CALL(InternalBindParameter(paramIdx, ioType, bufferType, paramSqlType, columnSize,
                decDigits, buffer, bufferLen, resLen));
        }

        sql_result Statement::InternalBindParameter(uint16_t paramIdx, int16_t ioType, int16_t bufferType,
            int16_t paramSqlType, SQLULEN columnSize, int16_t decDigits, void* buffer, SQLLEN bufferLen, SQLLEN* resLen)
        {
            using namespace type_traits;
            using application_data_buffer;
            using parameter;

            if (paramIdx == 0)
            {
                std::stringstream builder;
                builder << "The value specified for the argument ParameterNumber was less than 1. [ParameterNumber=" 
                    << paramIdx << ']';

                add_status_record(sql_state::S24000_INVALID_CURSOR_STATE, builder.str());

                return sql_result::AI_ERROR;
            }

            if (ioType != SQL_PARAM_INPUT)
            {
                std::stringstream builder;
                builder << "The value specified for the argument InputOutputType was not SQL_PARAM_INPUT. [ioType=" 
                    << ioType << ']';

                add_status_record(sql_state::SHY105_INVALID_PARAMETER_TYPE, builder.str());

                return sql_result::AI_ERROR;
            }

            if (!is_sql_type_supported(paramSqlType))
            {
                std::stringstream builder;
                builder << "Data type is not supported. [typeId=" << paramSqlType << ']';

                add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED, builder.str());

                return sql_result::AI_ERROR;
            }

            odbc_native_type driverType = to_driver_type(bufferType);

            if (driverType == odbc_native_type::AI_UNSUPPORTED)
            {
                std::stringstream builder;
                builder << "The argument TargetType was not a valid data type. [TargetType=" << bufferType << ']';

                add_status_record(sql_state::SHY003_INVALID_APPLICATION_BUFFER_TYPE, builder.str());

                return sql_result::AI_ERROR;
            }

            if (!buffer && !resLen)
            {
                add_status_record(sql_state::SHY009_INVALID_USE_OF_NULL_POINTER,
                    "ParameterValuePtr and StrLen_or_IndPtr are both null pointers");

                return sql_result::AI_ERROR;
            }

            application_data_buffer dataBuffer(driverType, buffer, bufferLen, resLen);

            parameter param(dataBuffer, paramSqlType, columnSize, decDigits);

            m_parameters.bind_parameter(paramIdx, param);

            return sql_result::AI_SUCCESS;
        }

        void Statement::SetAttribute(int attr, void* value, SQLINTEGER valueLen)
        {
            IGNITE_ODBC_API_CALL(InternalSetAttribute(attr, value, valueLen));
        }

        sql_result Statement::InternalSetAttribute(int attr, void* value, SQLINTEGER)
        {
            switch (attr)
            {
                case SQL_ATTR_ROW_ARRAY_SIZE:
                {
                    SQLULEN val = reinterpret_cast<SQLULEN>(value);

                    LOG_MSG("SQL_ATTR_ROW_ARRAY_SIZE: " << val);

                    if (val < 1)
                    {
                        add_status_record(sql_state::SHY092_OPTION_TYPE_OUT_OF_RANGE,
                            "Array size value can not be less than 1");

                        return sql_result::AI_ERROR;
                    }

                    rowArraySize = val;

                    break;
                }

                case SQL_ATTR_ROW_BIND_TYPE:
                {
                    SQLULEN rowBindType = reinterpret_cast<SQLULEN>(value);

                    if (rowBindType != SQL_BIND_BY_COLUMN)
                    {
                        add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                            "Only binding by column is currently supported");

                        return sql_result::AI_ERROR;
                    }

                    break;
                }

                case SQL_ATTR_ROWS_FETCHED_PTR:
                {
                    SetRowsFetchedPtr(reinterpret_cast<SQLINTEGER*>(value));

                    break;
                }

                case SQL_ATTR_ROW_STATUS_PTR:
                {
                    SetRowStatusesPtr(reinterpret_cast<SQLUSMALLINT*>(value));

                    break;
                }

                case SQL_ATTR_PARAM_BIND_TYPE:
                {
                    SQLULEN paramBindType = reinterpret_cast<SQLULEN>(value);

                    if (paramBindType != SQL_PARAM_BIND_BY_COLUMN)
                    {
                        add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                            "Only binding by column is currently supported");

                        return sql_result::AI_ERROR;
                    }

                    break;
                }

                case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
                {
                    set_param_bind_offset_ptr(reinterpret_cast<int*>(value));

                    break;
                }

                case SQL_ATTR_ROW_BIND_OFFSET_PTR:
                {
                    SetColumnBindOffsetPtr(reinterpret_cast<int*>(value));

                    break;
                }

                case SQL_ATTR_PARAMSET_SIZE:
                {
                    SQLULEN size = reinterpret_cast<SQLULEN>(value);

                    if (size > 1 && IsStreamingActive())
                    {
                        add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                            "Batching is not supported in streaming mode.");

                        return sql_result::AI_ERROR;
                    }

                    m_parameters.set_param_set_size(size);

                    break;
                }

                case SQL_ATTR_PARAMS_PROCESSED_PTR:
                {
                    m_parameters.set_params_processed_ptr(reinterpret_cast<SQLULEN*>(value));

                    break;
                }

                case SQL_ATTR_PARAM_STATUS_PTR:
                {
                    m_parameters.set_params_status_ptr(reinterpret_cast<SQLUSMALLINT*>(value));
                    break;
                }

                case SQL_ATTR_QUERY_TIMEOUT:
                {
                    SQLULEN uTimeout = reinterpret_cast<SQLULEN>(value);

                    if (uTimeout > INT32_MAX)
                    {
                        timeout = INT32_MAX;

                        std::stringstream ss;

                        ss << "Value is too big: " << uTimeout << ", changing to " << timeout << ".";
                        std::string msg = ss.str();

                        add_status_record(sql_state::S01S02_OPTION_VALUE_CHANGED, msg);

                        return sql_result::AI_SUCCESS_WITH_INFO;
                    }

                    timeout = static_cast<int32_t>(uTimeout);

                    break;
                }

                default:
                {
                    add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                        "Specified attribute is not supported.");

                    return sql_result::AI_ERROR;
                }
            }

            return sql_result::AI_SUCCESS;
        }

        void Statement::GetAttribute(int attr, void* buf, SQLINTEGER bufLen, SQLINTEGER* valueLen)
        {
            IGNITE_ODBC_API_CALL(InternalGetAttribute(attr, buf, bufLen, valueLen));
        }

        sql_result Statement::InternalGetAttribute(int attr, void* buf, SQLINTEGER, SQLINTEGER* valueLen)
        {
            if (!buf)
            {
                add_status_record("Data buffer is NULL.");

                return sql_result::AI_ERROR;
            }

            switch (attr)
            {
                case SQL_ATTR_APP_ROW_DESC:
                case SQL_ATTR_APP_PARAM_DESC:
                case SQL_ATTR_IMP_ROW_DESC:
                case SQL_ATTR_IMP_PARAM_DESC:
                {
                    SQLPOINTER *val = reinterpret_cast<SQLPOINTER*>(buf);

                    *val = static_cast<SQLPOINTER>(this);

                    if (valueLen)
                        *valueLen = SQL_IS_POINTER;

                    break;
                }

                case SQL_ATTR_ROW_BIND_TYPE:
                {
                    SQLULEN* val = reinterpret_cast<SQLULEN*>(buf);

                    *val = SQL_BIND_BY_COLUMN;

                    break;
                }

                case SQL_ATTR_ROW_ARRAY_SIZE:
                {
                    SQLINTEGER *val = reinterpret_cast<SQLINTEGER*>(buf);

                    *val = static_cast<SQLINTEGER>(rowArraySize);

                    if (valueLen)
                        *valueLen = SQL_IS_INTEGER;

                    break;
                }

                case SQL_ATTR_ROWS_FETCHED_PTR:
                {
                    SQLULEN** val = reinterpret_cast<SQLULEN**>(buf);

                    *val = reinterpret_cast<SQLULEN*>(GetRowsFetchedPtr());

                    if (valueLen)
                        *valueLen = SQL_IS_POINTER;

                    break;
                }

                case SQL_ATTR_ROW_STATUS_PTR:
                {
                    SQLUSMALLINT** val = reinterpret_cast<SQLUSMALLINT**>(buf);

                    *val = reinterpret_cast<SQLUSMALLINT*>(GetRowStatusesPtr());

                    if (valueLen)
                        *valueLen = SQL_IS_POINTER;

                    break;
                }

                case SQL_ATTR_PARAM_BIND_TYPE:
                {
                    SQLULEN* val = reinterpret_cast<SQLULEN*>(buf);

                    *val = SQL_PARAM_BIND_BY_COLUMN;

                    break;
                }

                case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
                {
                    SQLULEN** val = reinterpret_cast<SQLULEN**>(buf);

                    *val = reinterpret_cast<SQLULEN*>(m_parameters.get_param_bind_offset_ptr());

                    if (valueLen)
                        *valueLen = SQL_IS_POINTER;

                    break;
                }

                case SQL_ATTR_ROW_BIND_OFFSET_PTR:
                {
                    SQLULEN** val = reinterpret_cast<SQLULEN**>(buf);

                    *val = reinterpret_cast<SQLULEN*>(GetColumnBindOffsetPtr());

                    if (valueLen)
                        *valueLen = SQL_IS_POINTER;

                    break;
                }

                case SQL_ATTR_PARAMSET_SIZE:
                {
                    SQLULEN* val = reinterpret_cast<SQLULEN*>(buf);

                    *val = static_cast<SQLULEN>(m_parameters.get_param_set_size());

                    if (valueLen)
                        *valueLen = SQL_IS_UINTEGER;

                    break;
                }

                case SQL_ATTR_PARAMS_PROCESSED_PTR:
                {
                    SQLULEN** val = reinterpret_cast<SQLULEN**>(buf);

                    *val = m_parameters.get_params_processed_ptr();

                    if (valueLen)
                        *valueLen = SQL_IS_POINTER;

                    break;
                }

                case SQL_ATTR_PARAM_STATUS_PTR:
                {
                    SQLUSMALLINT** val = reinterpret_cast<SQLUSMALLINT**>(buf);

                    *val = m_parameters.get_params_status_ptr();

                    if (valueLen)
                        *valueLen = SQL_IS_POINTER;

                    break;
                }

                case SQL_ATTR_QUERY_TIMEOUT:
                {
                    SQLULEN *uTimeout = reinterpret_cast<SQLULEN*>(buf);

                    *uTimeout = static_cast<SQLULEN>(timeout);

                    break;
                }

                default:
                {
                    add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                        "Specified attribute is not supported.");

                    return sql_result::AI_ERROR;
                }
            }

            return sql_result::AI_SUCCESS;
        }

        void Statement::get_parameters_number(uint16_t& paramNum)
        {
            IGNITE_ODBC_API_CALL(InternalGetParametersNumber(paramNum));
        }

        sql_result Statement::InternalGetParametersNumber(uint16_t& paramNum)
        {
            if (!currentQuery.get())
            {
                add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query is not prepared.");

                return sql_result::AI_ERROR;
            }

            if (currentQuery->get_type() != query::QueryType::DATA)
            {
                paramNum = 0;

                return sql_result::AI_SUCCESS;
            }

            if (!m_parameters.is_metadata_set())
            {
                sql_result res = UpdateParamsMeta();

                if (res != sql_result::AI_SUCCESS)
                    return res;
            }

            paramNum = m_parameters.get_expected_param_num();

            return sql_result::AI_SUCCESS;
        }

        void Statement::set_param_bind_offset_ptr(int* ptr)
        {
            IGNITE_ODBC_API_CALL_ALWAYS_SUCCESS;

            m_parameters.set_param_bind_offset_ptr(ptr);
        }

        void Statement::GetColumnData(uint16_t columnIdx, application_data_buffer& buffer)
        {
            IGNITE_ODBC_API_CALL(InternalGetColumnData(columnIdx, buffer));
        }

        sql_result Statement::InternalGetColumnData(uint16_t columnIdx,
            application_data_buffer& buffer)
        {
            if (!currentQuery.get())
            {
                add_status_record(sql_state::S24000_INVALID_CURSOR_STATE,
                    "Cursor is not in the open state.");

                return sql_result::AI_ERROR;
            }

            sql_result res = currentQuery->GetColumn(columnIdx, buffer);

            return res;
        }

        void Statement::PrepareSqlQuery(const std::string& query)
        {
            IGNITE_ODBC_API_CALL(InternalPrepareSqlQuery(query));
        }

        sql_result Statement::ProcessInternalCommand(const std::string& query)
        {
            try
            {
                SqlParser parser(query);

                std::auto_ptr<SqlCommand> cmd = parser.GetNextCommand();

                assert(cmd.get() != 0);

                m_parameters.prepare();

                currentQuery.reset(new query::InternalQuery(*this, query, cmd));

                return sql_result::AI_SUCCESS;
            }
            catch (const odbc_error& err)
            {
                add_status_record(err);

                return sql_result::AI_ERROR;
            }
        }

        bool Statement::IsStreamingActive() const
        {
            return connection.GetStreamingContext().is_enabled();
        }

        sql_result Statement::InternalPrepareSqlQuery(const std::string& query)
        {
            if (sql_utils::IsInternalCommand(query))
                return ProcessInternalCommand(query);

            // Resetting m_parameters types as we are changing the query.
            m_parameters.prepare();

            if (IsStreamingActive())
            {
                if (!currentQuery.get())
                    currentQuery.reset(new query::StreamingQuery(*this, connection, m_parameters));

                query::StreamingQuery* currentQuery0 = static_cast<query::StreamingQuery*>(currentQuery.get());

                currentQuery0->PrepareQuery(query);

                return sql_result::AI_SUCCESS;
            }

            if (currentQuery.get())
                currentQuery->Close();

            currentQuery.reset(new query::DataQuery(*this, connection, query, m_parameters, timeout));

            return sql_result::AI_SUCCESS;
        }

        void Statement::ExecuteSqlQuery(const std::string& query)
        {
            IGNITE_ODBC_API_CALL(InternalExecuteSqlQuery(query));
        }

        sql_result Statement::InternalExecuteSqlQuery(const std::string& query)
        {
            sql_result result = InternalPrepareSqlQuery(query);

            if (result != sql_result::AI_SUCCESS)
                return result;

            return InternalExecuteSqlQuery();
        }

        void Statement::ExecuteSqlQuery()
        {
            IGNITE_ODBC_API_CALL(InternalExecuteSqlQuery());
        }

        sql_result Statement::InternalExecuteSqlQuery()
        {
            if (!currentQuery.get())
            {
                add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query is not prepared.");

                return sql_result::AI_ERROR;
            }

            if (currentQuery->get_type() == query::QueryType::INTERNAL)
            {
                ProcessInternalQuery();

                return sql_result::AI_SUCCESS;
            }

            if (m_parameters.get_param_set_size() > 1 && currentQuery->get_type() == query::QueryType::DATA)
            {
                query::DataQuery& qry = static_cast<query::DataQuery&>(*currentQuery);

                currentQuery.reset(new query::BatchQuery(*this, connection, qry.GetSql(), m_parameters, timeout));
            }
            else if (m_parameters.get_param_set_size() == 1 && currentQuery->get_type() == query::QueryType::BATCH)
            {
                query::BatchQuery& qry = static_cast<query::BatchQuery&>(*currentQuery);

                currentQuery.reset(new query::DataQuery(*this, connection, qry.GetSql(), m_parameters, timeout));
            }

            if (m_parameters.get_param_set_size() > 1 && currentQuery->get_type() == query::QueryType::STREAMING)
            {
                add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                    "Batching is not supported in streaming mode.");

                return sql_result::AI_ERROR;
            }

            if (m_parameters.is_data_at_exec_needed())
            {
                if (currentQuery->get_type() == query::QueryType::BATCH ||
                    currentQuery->get_type() == query::QueryType::STREAMING)
                {
                    add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                        "Data-at-execution is not supported with batching.");

                    return sql_result::AI_ERROR;
                }

                return sql_result::AI_NEED_DATA;
            }

            return currentQuery->Execute();
        }

        sql_result Statement::ProcessInternalQuery()
        {
            assert(currentQuery->get_type() == query::QueryType::INTERNAL);

            query::InternalQuery* qry = static_cast<query::InternalQuery*>(currentQuery.get());
            
            LOG_MSG("Processing internal query: " << qry->GetQuery());

            assert(qry->GetCommand().get_type() == SqlCommandType::SET_STREAMING);

            SqlSetStreamingCommand& cmd = static_cast<SqlSetStreamingCommand&>(qry->GetCommand());

            StopStreaming();

            if (!cmd.is_enabled())
                return sql_result::AI_SUCCESS;

            LOG_MSG("Sending start streaming command");

            query::DataQuery enablingQuery(*this, connection, qry->GetQuery(), m_parameters, timeout);

            sql_result res = enablingQuery.Execute();

            if (res != sql_result::AI_SUCCESS)
                return res;

            LOG_MSG("Preparing streaming context on client");

            connection.GetStreamingContext().Enable(cmd);

            std::auto_ptr<query::Query> newQry(new query::StreamingQuery(*this, connection, m_parameters));

            std::swap(currentQuery, newQry);

            return sql_result::AI_SUCCESS;
        }

        void Statement::ExecuteGetColumnsMetaQuery(const std::string& schema,
            const std::string& table, const std::string& column)
        {
            IGNITE_ODBC_API_CALL(InternalExecuteGetColumnsMetaQuery(schema, table, column));
        }

        sql_result Statement::InternalExecuteGetColumnsMetaQuery(const std::string& schema,
            const std::string& table, const std::string& column)
        {
            if (currentQuery.get())
                currentQuery->Close();

            std::string schema0(schema);

            if (schema0.empty())
                schema0 = connection.GetSchema();

            currentQuery.reset(new query::ColumnMetadataQuery(*this,
                connection, schema, table, column));

            return currentQuery->Execute();
        }

        void Statement::ExecuteGetTablesMetaQuery(const std::string& catalog,
            const std::string& schema, const std::string& table, const std::string& tableType)
        {
            IGNITE_ODBC_API_CALL(InternalExecuteGetTablesMetaQuery(
                catalog, schema, table, tableType));
        }

        sql_result Statement::InternalExecuteGetTablesMetaQuery(const std::string& catalog,
            const std::string& schema, const std::string& table, const std::string& tableType)
        {
            if (currentQuery.get())
                currentQuery->Close();

            currentQuery.reset(new query::TableMetadataQuery(*this,
                connection, catalog, schema, table, tableType));

            return currentQuery->Execute();
        }

        void Statement::ExecuteGetForeignKeysQuery(const std::string& primaryCatalog,
            const std::string& primarySchema, const std::string& primaryTable,
            const std::string& foreignCatalog, const std::string& foreignSchema,
            const std::string& foreignTable)
        {
            IGNITE_ODBC_API_CALL(InternalExecuteGetForeignKeysQuery(primaryCatalog,
                primarySchema, primaryTable, foreignCatalog, foreignSchema, foreignTable));
        }

        sql_result Statement::InternalExecuteGetForeignKeysQuery(const std::string& primaryCatalog,
            const std::string& primarySchema, const std::string& primaryTable,
            const std::string& foreignCatalog, const std::string& foreignSchema,
            const std::string& foreignTable)
        {
            if (currentQuery.get())
                currentQuery->Close();

            currentQuery.reset(new query::ForeignKeysQuery(*this, connection, primaryCatalog,
                primarySchema, primaryTable, foreignCatalog, foreignSchema, foreignTable));

            return currentQuery->Execute();
        }

        void Statement::ExecuteGetPrimaryKeysQuery(const std::string& catalog,
            const std::string& schema, const std::string& table)
        {
            IGNITE_ODBC_API_CALL(InternalExecuteGetPrimaryKeysQuery(catalog, schema, table));
        }

        sql_result Statement::InternalExecuteGetPrimaryKeysQuery(const std::string& catalog,
            const std::string& schema, const std::string& table)
        {
            if (currentQuery.get())
                currentQuery->Close();

            currentQuery.reset(new query::PrimaryKeysQuery(*this,
                connection, catalog, schema, table));

            return currentQuery->Execute();
        }

        void Statement::ExecuteSpecialColumnsQuery(int16_t type,
            const std::string& catalog, const std::string& schema,
            const std::string& table, int16_t scope, int16_t nullable)
        {
            IGNITE_ODBC_API_CALL(InternalExecuteSpecialColumnsQuery(type,
                catalog, schema, table, scope, nullable));
        }

        sql_result Statement::InternalExecuteSpecialColumnsQuery(int16_t type,
            const std::string& catalog, const std::string& schema,
            const std::string& table, int16_t scope, int16_t nullable)
        {
            if (type != SQL_BEST_ROWID && type != SQL_ROWVER)
            {
                add_status_record(sql_state::SHY097_COLUMN_TYPE_OUT_OF_RANGE,
                    "An invalid IdentifierType value was specified.");

                return sql_result::AI_ERROR;
            }

            if (currentQuery.get())
                currentQuery->Close();

            currentQuery.reset(new query::SpecialColumnsQuery(*this, type,
                catalog, schema, table, scope, nullable));

            return currentQuery->Execute();
        }

        void Statement::ExecuteGetTypeInfoQuery(int16_t sqlType)
        {
            IGNITE_ODBC_API_CALL(InternalExecuteGetTypeInfoQuery(sqlType));
        }

        sql_result Statement::InternalExecuteGetTypeInfoQuery(int16_t sqlType)
        {
            if (sqlType != SQL_ALL_TYPES && !is_sql_type_supported(sqlType))
            {
                std::stringstream builder;
                builder << "Data type is not supported. [typeId=" << sqlType << ']';

                add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED, builder.str());

                return sql_result::AI_ERROR;
            }

            if (currentQuery.get())
                currentQuery->Close();

            currentQuery.reset(new query::TypeInfoQuery(*this, sqlType));

            return currentQuery->Execute();
        }

        void Statement::FreeResources(int16_t option)
        {
            IGNITE_ODBC_API_CALL(InternalFreeResources(option));
        }

        sql_result Statement::InternalFreeResources(int16_t option)
        {
            switch (option)
            {
                case SQL_DROP:
                {
                    add_status_record("Deprecated, call SQLFreeHandle instead");

                    return sql_result::AI_ERROR;
                }

                case SQL_CLOSE:
                {
                    return InternalClose();
                }

                case SQL_UNBIND:
                {
                    SafeUnbindAllColumns();

                    break;
                }

                case SQL_RESET_PARAMS:
                {
                    m_parameters.unbind_all();

                    break;
                }

                default:
                {
                    add_status_record(sql_state::SHY092_OPTION_TYPE_OUT_OF_RANGE, "The value specified for the argument Option was invalid");
                    return sql_result::AI_ERROR;
                }
            }
            return sql_result::AI_SUCCESS;
        }

        void Statement::Close()
        {
            IGNITE_ODBC_API_CALL(InternalClose());
        }

        sql_result Statement::InternalClose()
        {
            if (!currentQuery.get())
                return sql_result::AI_SUCCESS;

            sql_result result = currentQuery->Close();

            return result;
        }

        sql_result Statement::StopStreaming()
        {
            if (!IsStreamingActive())
                return sql_result::AI_SUCCESS;

            LOG_MSG("Stopping streaming");

            sql_result result = connection.GetStreamingContext().Disable();

            return result;
        }

        void Statement::FetchScroll(int16_t orientation, int64_t offset)
        {
            IGNITE_ODBC_API_CALL(InternalFetchScroll(orientation, offset));
        }

        sql_result Statement::InternalFetchScroll(int16_t orientation, int64_t offset)
        {
            UNUSED_VALUE offset;

            if (orientation != SQL_FETCH_NEXT)
            {
                add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                    "Only SQL_FETCH_NEXT FetchOrientation type is supported");

                return sql_result::AI_ERROR;
            }

            return InternalFetchRow();
        }

        void Statement::FetchRow()
        {
            IGNITE_ODBC_API_CALL(InternalFetchRow());
        }

        sql_result Statement::InternalFetchRow()
        {
            if (rowsFetched)
                *rowsFetched = 0;

            if (!currentQuery.get())
            {
                add_status_record(sql_state::S24000_INVALID_CURSOR_STATE, "Cursor is not in the open state");

                return sql_result::AI_ERROR;
            }

            if (columnBindOffset)
            {
                for (column_binding_map::iterator it = columnBindings.begin(); it != columnBindings.end(); ++it)
                    it->second.set_byte_offset(*columnBindOffset);
            }

            SQLINTEGER fetched = 0;
            SQLINTEGER errors = 0;

            for (SQLULEN i = 0; i < rowArraySize; ++i)
            {
                for (column_binding_map::iterator it = columnBindings.begin(); it != columnBindings.end(); ++it)
                    it->second.set_element_offset(i);

                sql_result res = currentQuery->FetchNextRow(columnBindings);

                if (res == sql_result::AI_SUCCESS || res == sql_result::AI_SUCCESS_WITH_INFO)
                    ++fetched;
                else if (res != sql_result::AI_NO_DATA)
                    ++errors;

                if (rowStatuses)
                    rowStatuses[i] = sql_resultToRowResult(res);
            }

            if (rowsFetched)
                *rowsFetched = fetched < 0 ? static_cast<SQLINTEGER>(rowArraySize) : fetched;

            if (fetched > 0)
                return errors == 0 ? sql_result::AI_SUCCESS : sql_result::AI_SUCCESS_WITH_INFO;

            return errors == 0 ? sql_result::AI_NO_DATA : sql_result::AI_ERROR;
        }

        const meta::ColumnMetaVector* Statement::GetMeta()
        {
            if (!currentQuery.get())
            {
                add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query is not executed.");

                return 0;
            }

            return currentQuery->GetMeta();
        }

        bool Statement::DataAvailable() const
        {
            return currentQuery.get() && currentQuery->DataAvailable();
        }

        void Statement::MoreResults()
        {
            IGNITE_ODBC_API_CALL(InternalMoreResults());
        }

        sql_result Statement::InternalMoreResults()
        {
            if (!currentQuery.get())
            {
                add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query is not executed.");

                return sql_result::AI_ERROR;
            }

            return currentQuery->NextResultSet();
        }

        void Statement::GetColumnAttribute(uint16_t colIdx, uint16_t attrId,
            char* strbuf, int16_t buflen, int16_t* reslen, SQLLEN* numbuf)
        {
            IGNITE_ODBC_API_CALL(InternalGetColumnAttribute(colIdx, attrId,
                strbuf, buflen, reslen, numbuf));
        }

        sql_result Statement::InternalGetColumnAttribute(uint16_t colIdx, uint16_t attrId, char* strbuf,
            int16_t buflen, int16_t* reslen, SQLLEN* numbuf)
        {
            const meta::ColumnMetaVector *meta = GetMeta();

            LOG_MSG("Collumn ID: " << colIdx << ", Attribute ID: " << attrId);

            if (!meta)
                return sql_result::AI_ERROR;

            if (colIdx > meta->size() || colIdx < 1)
            {
                add_status_record(sql_state::SHY000_GENERAL_ERROR,
                    "Column index is out of range.", 0, colIdx);

                return sql_result::AI_ERROR;
            }

            const meta::ColumnMeta& columnMeta = meta->at(colIdx - 1);

            bool found = false;

            if (numbuf)
                found = columnMeta.GetAttribute(attrId, *numbuf);

            if (!found)
            {
                std::string out;

                found = columnMeta.GetAttribute(attrId, out);

                size_t outSize = out.size();

                if (found && strbuf)
                    outSize = utility::CopyStringToBuffer(out, strbuf, buflen);

                if (found && reslen)
                    *reslen = static_cast<int16_t>(outSize);
            }

            if (!found)
            {
                add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                    "Unknown attribute.");

                return sql_result::AI_ERROR;
            }

            return sql_result::AI_SUCCESS;
        }

        int64_t Statement::AffectedRows()
        {
            int64_t rowCnt = 0;

            IGNITE_ODBC_API_CALL(InternalAffectedRows(rowCnt));

            return rowCnt;
        }

        sql_result Statement::InternalAffectedRows(int64_t& rowCnt)
        {
            if (!currentQuery.get())
            {
                add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query is not executed.");

                return sql_result::AI_ERROR;
            }

            rowCnt = currentQuery->AffectedRows();

            return sql_result::AI_SUCCESS;
        }

        void Statement::SetRowsFetchedPtr(SQLINTEGER* ptr)
        {
            rowsFetched = ptr;
        }

        SQLINTEGER* Statement::GetRowsFetchedPtr()
        {
            return rowsFetched;
        }

        void Statement::SetRowStatusesPtr(SQLUSMALLINT* ptr)
        {
            rowStatuses = ptr;
        }

        SQLUSMALLINT * Statement::GetRowStatusesPtr()
        {
            return rowStatuses;
        }

        void Statement::SelectParam(void** paramPtr)
        {
            IGNITE_ODBC_API_CALL(InternalSelectParam(paramPtr));
        }

        sql_result Statement::InternalSelectParam(void** paramPtr)
        {
            if (!paramPtr)
            {
                add_status_record(sql_state::SHY000_GENERAL_ERROR,
                    "Invalid parameter: ValuePtrPtr is null.");

                return sql_result::AI_ERROR;
            }

            if (!currentQuery.get())
            {
                add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query is not prepared.");

                return sql_result::AI_ERROR;
            }

            parameter *selected = m_parameters.get_selected_parameter();

            if (selected && !selected->is_data_ready())
            {
                add_status_record(sql_state::S22026_DATA_LENGTH_MISMATCH,
                    "Less data was sent for a parameter than was specified with "
                    "the StrLen_or_IndPtr argument in SQLBindParameter.");

                return sql_result::AI_ERROR;
            }

            selected = m_parameters.select_next_parameter();

            if (selected)
            {
                *paramPtr = selected->get_buffer().get_data();

                return sql_result::AI_NEED_DATA;
            }

            sql_result res = currentQuery->Execute();

            if (res != sql_result::AI_SUCCESS)
                res = sql_result::AI_SUCCESS_WITH_INFO;

            return res;
        }

        void Statement::put_data(void* data, SQLLEN len)
        {
            IGNITE_ODBC_API_CALL(InternalPutData(data, len));
        }

        sql_result Statement::InternalPutData(void* data, SQLLEN len)
        {
            if (!data && len != 0 && len != SQL_DEFAULT_PARAM && len != SQL_NULL_DATA)
            {
                add_status_record(sql_state::SHY009_INVALID_USE_OF_NULL_POINTER,
                    "Invalid parameter: DataPtr is null StrLen_or_Ind is not 0, "
                    "SQL_DEFAULT_PARAM, or SQL_NULL_DATA.");

                return sql_result::AI_ERROR;
            }

            if (!m_parameters.is_parameter_selected())
            {
                add_status_record(sql_state::SHY010_SEQUENCE_ERROR,
                    "parameter is not selected with the SQLParamData.");

                return sql_result::AI_ERROR;
            }

            parameter* param = m_parameters.get_selected_parameter();

            if (!param)
            {
                add_status_record(sql_state::SHY000_GENERAL_ERROR,
                    "Selected parameter has been unbound.");

                return sql_result::AI_ERROR;
            }

            param->put_data(data, len);

            return sql_result::AI_SUCCESS;
        }

        void Statement::DescribeParam(int16_t paramNum, int16_t* dataType,
            SQLULEN* paramSize, int16_t* decimalDigits, int16_t* nullable)
        {
            IGNITE_ODBC_API_CALL(InternalDescribeParam(paramNum,
                dataType, paramSize, decimalDigits, nullable));
        }

        sql_result Statement::InternalDescribeParam(int16_t paramNum, int16_t* dataType,
            SQLULEN* paramSize, int16_t* decimalDigits, int16_t* nullable)
        {
            query::Query *qry = currentQuery.get();
            if (!qry)
            {
                add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query is not prepared.");

                return sql_result::AI_ERROR;
            }

            if (qry->get_type() != query::QueryType::DATA)
            {
                add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query is not SQL data query.");

                return sql_result::AI_ERROR;
            }

            int8_t type = m_parameters.get_param_type(paramNum, 0);

            LOG_MSG("Type: " << type);

            if (!type)
            {
                sql_result res = UpdateParamsMeta();

                if (res != sql_result::AI_SUCCESS)
                    return res;

                type = m_parameters.get_param_type(paramNum, impl::binary::IGNITE_HDR_NULL);
            }

            if (dataType)
                *dataType = ignite_type_to_sql_type(type);

            if (paramSize)
                *paramSize = ignite_type_column_size(type);

            if (decimalDigits)
                *decimalDigits = ignite_type_decimal_digits(type);

            if (nullable)
                *nullable = ignite_type_nullability(type);

            return sql_result::AI_SUCCESS;
        }

        sql_result Statement::UpdateParamsMeta()
        {
            query::Query *qry0 = currentQuery.get();

            assert(qry0 != 0);
            assert(qry0->get_type() == query::QueryType::DATA);

            query::DataQuery* qry = static_cast<query::DataQuery*>(qry0);

            const std::string& schema = connection.GetSchema();
            const std::string& sql = qry->GetSql();

            QueryGetParamsMetaRequest req(schema, sql);
            QueryGetParamsMetaResponse rsp;

            try
            {
                connection.SyncMessage(req, rsp);
            }
            catch (const odbc_error& err)
            {
                add_status_record(err);

                return sql_result::AI_ERROR;
            }
            catch (const IgniteError& err)
            {
                add_status_record(err.GetText());

                return sql_result::AI_ERROR;
            }

            if (rsp.get_state() != response_status::SUCCESS)
            {
                LOG_MSG("Error: " << rsp.GetError());

                add_status_record(response_status_to_sql_state(rsp.get_state()), rsp.GetError());

                return sql_result::AI_ERROR;
            }

            m_parameters.update_params_types(rsp.GetTypeIds());

            for (size_t i = 0; i < rsp.GetTypeIds().size(); ++i)
            {
                LOG_MSG("[" << i << "] parameter type: " << rsp.GetTypeIds()[i]);
            }

            return sql_result::AI_SUCCESS;
        }

        uint16_t Statement::sql_resultToRowResult(sql_result value)
        {
            switch (value)
            {
                case sql_result::AI_NO_DATA:
                    return SQL_ROW_NOROW;

                case sql_result::AI_SUCCESS:
                    return SQL_ROW_SUCCESS;

                case sql_result::AI_SUCCESS_WITH_INFO:
                    return SQL_ROW_SUCCESS_WITH_INFO;

                default:
                    return SQL_ROW_ERROR;
            }
        }
    }
}



































