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

#ifndef _IGNITE_ODBC_STATEMENT
#define _IGNITE_ODBC_STATEMENT

#include <stdint.h>

#include <map>
#include <memory>

#include "ignite/odbc/common_types.h"
#include "ignite/odbc/app/application_data_buffer.h"
#include "ignite/odbc/app/parameter_set.h"
#include "ignite/odbc/diagnostic/diagnosable_adapter.h"
#include "ignite/odbc/meta/column_meta.h"
#include "ignite/odbc/query/query.h"
#include "ignite/odbc/sql/sql_set_streaming_command.h"

namespace ignite
{
    namespace odbc
    {
        class Connection;

        /**
         * SQL-statement abstraction. Holds SQL query user buffers data and
         * call result.
         */
        class Statement : public DiagnosableAdapter
        {
            friend class Connection;
        public:
            /**
             * Destructor.
             */
            ~Statement();

            /**
             * Bind result column to data buffer provided by application
             *
             * @param columnIdx Column index.
             * @param targetType Type of target buffer.
             * @param targetValue Pointer to target buffer.
             * @param bufferLength Length of target buffer.
             * @param strLengthOrIndicator Pointer to the length/indicator buffer.
             */
            void BindColumn(uint16_t columnIdx, int16_t targetType, void* targetValue, SQLLEN bufferLength, SQLLEN* strLengthOrIndicator);

            /**
             * Set column binding offset pointer.
             *
             * @param ptr Column binding offset pointer.
             */
            void SetColumnBindOffsetPtr(int* ptr);

            /**
             * Get column binding offset pointer.
             *
             * @return Column binding offset pointer.
             */
            int* GetColumnBindOffsetPtr();

            /**
             * Get number of columns in the result set.
             *
             * @return Columns number.
             */
            int32_t GetColumnNumber();

            /**
             * Bind parameter.
             *
             * @param paramIdx parameter index.
             * @param ioType Type of the parameter (input/output).
             * @param bufferType The data type of the parameter.
             * @param paramSqlType The SQL data type of the parameter.
             * @param columnSize  The size of the column or expression of the corresponding parameter marker.
             * @param decDigits  The decimal digits of the column or expression of the corresponding parameter marker.
             * @param buffer A pointer to a buffer for the parameter's data.
             * @param bufferLen Length of the ParameterValuePtr buffer in bytes.
             * @param resLen A pointer to a buffer for the parameter's length.
             */
            void bind_parameter(uint16_t paramIdx, int16_t ioType, int16_t bufferType, int16_t paramSqlType,
                               SQLULEN columnSize, int16_t decDigits, void* buffer, SQLLEN bufferLen, SQLLEN* resLen);

            /**
             * Set statement attribute.
             *
             * @param attr Attribute type.
             * @param value Value pointer.
             * @param valueLen Value length.
             */
            void SetAttribute(int attr, void* value, SQLINTEGER valueLen);

            /**
             * Get statement attribute.
             *
             * @param attr Attribute type.
             * @param buf Buffer for value.
             * @param bufLen Buffer length.
             * @param valueLen Resulting value length.
             */
            void GetAttribute(int attr, void* buf, SQLINTEGER bufLen, SQLINTEGER *valueLen);

            /**
             * Get number m_parameters required by the prepared statement.
             *
             * @param paramNum Number of m_parameters.
             */
            void get_parameters_number(uint16_t& paramNum);

            /**
             * Set parameter binding offset pointer.
             *
             * @param ptr parameter binding offset pointer.
             */
            void set_param_bind_offset_ptr(int* ptr);

            /**
             * Get value of the column in the result set.
             *
             * @param columnIdx Column index.
             * @param buffer Buffer to put column data to.
             */
            void GetColumnData(uint16_t columnIdx, application_data_buffer& buffer);

            /**
             * Prepare SQL query.
             *
             * @param query SQL query.
             */
            void PrepareSqlQuery(const std::string& query);

            /**
             * Execute SQL query.
             *
             * @param query SQL query.
             */
            void ExecuteSqlQuery(const std::string& query);

            /**
             * Execute SQL query.
             */
            void ExecuteSqlQuery();

            /**
             * Get columns metadata.
             *
             * @param schema Schema search pattern.
             * @param table Table search pattern.
             * @param column Column search pattern.
             */
            void ExecuteGetColumnsMetaQuery(const std::string& schema,
                const std::string& table, const std::string& column);

            /**
             * Get tables metadata.
             *
             * @param catalog Catalog search pattern.
             * @param schema Schema search pattern.
             * @param table Table search pattern.
             * @param tableType Table type search pattern.
             */
            void ExecuteGetTablesMetaQuery(const std::string& catalog,
                const std::string& schema, const std::string& table,
                const std::string& tableType);

            /**
             * Get foreign keys.
             *
             * @param primaryCatalog Primary key catalog name.
             * @param primarySchema Primary key schema name.
             * @param primaryTable Primary key table name.
             * @param foreignCatalog Foreign key catalog name.
             * @param foreignSchema Foreign key schema name.
             * @param foreignTable Foreign key table name.
             */
            void ExecuteGetForeignKeysQuery(const std::string& primaryCatalog,
                const std::string& primarySchema, const std::string& primaryTable,
                const std::string& foreignCatalog, const std::string& foreignSchema,
                const std::string& foreignTable);

            /**
             * Get primary keys.
             *
             * @param catalog Catalog name.
             * @param schema Schema name.
             * @param table Table name.
             */
            void ExecuteGetPrimaryKeysQuery(const std::string& catalog,
                const std::string& schema, const std::string& table);

            /**
             * Get special columns.
             *
             * @param type Special column type.
             * @param catalog Catalog name.
             * @param schema Schema name.
             * @param table Table name.
             * @param scope Minimum required scope of the rowid.
             * @param type Determines whether to return special columns that
             *             can have a NULL value.
             */
            void ExecuteSpecialColumnsQuery(int16_t type,
                const std::string& catalog, const std::string& schema,
                const std::string& table, int16_t scope, int16_t nullable);

            /**
             * Get type info.
             *
             * @param sqlType SQL type for which to return info or SQL_ALL_TYPES.
             */
            void ExecuteGetTypeInfoQuery(int16_t sqlType);

            /**
             * Free resources
             * @param option indicates what needs to be freed
             */
            void FreeResources(int16_t option);

            /**
             * Close statement.
             */
            void Close();

            /**
             * Fetch query result row with offset
             * @param orientation Fetch type
             * @param offset Fetch offset
             */
            void FetchScroll(int16_t orientation, int64_t offset);

            /**
             * Fetch query result row.
             */
            void FetchRow();

            /**
             * Get column metadata.
             *
             * @return Column metadata.
             */
            const meta::ColumnMetaVector* GetMeta();

            /**
             * Check if data is available.
             *
             * @return True if data is available.
             */
            bool DataAvailable() const;

            /**
             * More results.
             *
             * Move to next result set or affected rows number.
             */
            void MoreResults();

            /**
             * Get column attribute.
             *
             * @param colIdx Column index.
             * @param attrId Attribute ID.
             * @param strbuf Buffer for string attribute value.
             * @param buflen String buffer size.
             * @param reslen Buffer to put resulting string length to.
             * @param numbuf Numeric value buffer.
             */
            void GetColumnAttribute(uint16_t colIdx, uint16_t attrId, char* strbuf,
                int16_t buflen, int16_t* reslen, SQLLEN* numbuf);

            /**
             * Get number of rows affected by the statement.
             *
             * @return Number of rows affected by the statement.
             */
            int64_t AffectedRows();

            /**
             * Set rows fetched buffer pointer.
             *
             * @param ptr Rows fetched buffer pointer.
             */
            void SetRowsFetchedPtr(SQLINTEGER* ptr);

            /**
             * Get rows fetched buffer pointer.
             *
             * @return Rows fetched buffer pointer.
             */
            SQLINTEGER* GetRowsFetchedPtr();

            /**
             * Set row statuses array pointer.
             *
             * @param ptr Row statuses array pointer.
             */
            void SetRowStatusesPtr(SQLUSMALLINT* ptr);

            /**
             * Get row statuses array pointer.
             *
             * @return Row statuses array pointer.
             */
            SQLUSMALLINT* GetRowStatusesPtr();

            /**
             * Select next parameter data for which is required.
             *
             * @param paramPtr Pointer to param id stored here.
             */
            void SelectParam(void** paramPtr);

            /**
             * Puts data for previously selected parameter or column.
             *
             * @param data Data.
             * @param len Data length.
             */
            void put_data(void* data, SQLLEN len);

            /**
             * Get type info of the parameter of the prepared statement.
             *
             * @param paramNum - parameter index.
             * @param dataType - Data type.
             * @param paramSize - Size of the parameter.
             * @param decimalDigits - big_decimal digits.
             * @param nullable - Nullability flag.
             */
            void DescribeParam(int16_t paramNum, int16_t* dataType,
                SQLULEN* paramSize, int16_t* decimalDigits, int16_t* nullable);

        private:
            IGNITE_NO_COPY_ASSIGNMENT(Statement);


            /**
             * Bind result column to specified data buffer.
             *
             * @param columnIdx Column index.
             * @param buffer Buffer to put column data to.
             */
            void SafeBindColumn(uint16_t columnIdx, const application_data_buffer& buffer);

            /**
             * Unbind specified column buffer.
             *
             * @param columnIdx Column index.
             */
            void SafeUnbindColumn(uint16_t columnIdx);

            /**
             * Unbind all column buffers.
             */
            void SafeUnbindAllColumns();

            /**
             * Bind result column to data buffer provided by application
             *
             * @param columnIdx Column index.
             * @param targetType Type of target buffer.
             * @param targetValue Pointer to target buffer.
             * @param bufferLength Length of target buffer.
             * @param strLengthOrIndicator Pointer to the length/indicator buffer.
             * @return Operation result.
             */
            sql_result InternalBindColumn(uint16_t columnIdx, int16_t targetType, void* targetValue, SQLLEN bufferLength, SQLLEN* strLengthOrIndicator);
            
            /**
             * Bind parameter.
             *
             * @param paramIdx parameter index.
             * @param ioType Type of the parameter (input/output).
             * @param bufferType The data type of the parameter.
             * @param paramSqlType The SQL data type of the parameter.
             * @param columnSize  The size of the column or expression of the corresponding parameter marker.
             * @param decDigits  The decimal digits of the column or expression of the corresponding parameter marker.
             * @param buffer A pointer to a buffer for the parameter's data.
             * @param bufferLen Length of the ParameterValuePtr buffer in bytes.
             * @param resLen A pointer to a buffer for the parameter's length.
             * @return Operation result.
             */
            sql_result InternalBindParameter(uint16_t paramIdx, int16_t ioType, int16_t bufferType, int16_t paramSqlType,
                                            SQLULEN columnSize, int16_t decDigits, void* buffer, SQLLEN bufferLen, SQLLEN* resLen);

            /**
             * Set statement attribute.
             * Internal call.
             *
             * @param attr Attribute type.
             * @param value Value pointer.
             * @param valueLen Value length.
             * @return Operation result.
             */
            sql_result InternalSetAttribute(int attr, void* value, SQLINTEGER valueLen);

            /**
             * Get statement attribute.
             * Internal call.
             *
             * @param attr Attribute type.
             * @param buf Buffer for value.
             * @param bufLen Buffer length.
             * @param valueLen Resulting value length.
             * @return Operation result.
             */
            sql_result InternalGetAttribute(int attr, void* buf, SQLINTEGER bufLen, SQLINTEGER* valueLen);

            /**
             * Get number m_parameters required by the prepared statement.
             *
             * @param paramNum Number of m_parameters.
             */
            sql_result InternalGetParametersNumber(uint16_t& paramNum);

            /**
             * Get value of the column in the result set.
             *
             * @param columnIdx Column index.
             * @param buffer Buffer to put column data to.
             * @return Operation result.
             */
            sql_result InternalGetColumnData(uint16_t columnIdx, application_data_buffer& buffer);


            /**
             * Free resources
             * @param option indicates what needs to be freed
             * @return Operation result.
             */
            sql_result InternalFreeResources(int16_t option);

            /**
             * Close statement.
             * Internal call.
             *
             * @return Operation result.
             */
            sql_result InternalClose();

            /**
             * Stop streaming.
             *
             * @return Operation result.
             */
            sql_result StopStreaming();

            /**
             * Process internal SQL command.
             *
             * @param query SQL query.
             * @return Operation result.
             */
            sql_result ProcessInternalCommand(const std::string& query);

            /**
             * Check if the streaming is active currently.
             *
             * @return @c true, if the streaming is active.
             */
            bool IsStreamingActive() const;

            /**
             * Prepare SQL query.
             *
             * @param query SQL query.
             * @return Operation result.
             */
            sql_result InternalPrepareSqlQuery(const std::string& query);

            /**
             * Execute SQL query.
             *
             * @param query SQL query.
             * @return Operation result.
             */
            sql_result InternalExecuteSqlQuery(const std::string& query);

            /**
             * Execute SQL query.
             *
             * @return Operation result.
             */
            sql_result InternalExecuteSqlQuery();

            /**
             * Process internal query.
             *
             * @return Operation result.
             */
            sql_result ProcessInternalQuery();

            /**
             * Fetch query result row with offset
             * @param orientation Fetch type
             * @param offset Fetch offset
             * @return Operation result.
             */
            sql_result InternalFetchScroll(int16_t orientation, int64_t offset);

            /**
             * Fetch query result row.
             *
             * @return Operation result.
             */
            sql_result InternalFetchRow();

            /**
             * Get number of columns in the result set.
             *
             * @param res Columns number.
             * @return Operation result.
             */
            sql_result InternalGetColumnNumber(int32_t &res);

            /**
             * Get columns metadata.
             *
             * @param schema Schema search pattern.
             * @param table Table search pattern.
             * @param column Column search pattern.
             * @return Operation result.
             */
            sql_result InternalExecuteGetColumnsMetaQuery(const std::string& schema,
                const std::string& table, const std::string& column);

            /**
             * Get tables metadata.
             *
             * @param catalog Catalog search pattern.
             * @param schema Schema search pattern.
             * @param table Table search pattern.
             * @param tableType Table type search pattern.
             * @return Operation result.
             */
            sql_result InternalExecuteGetTablesMetaQuery(const std::string& catalog,
                const std::string& schema, const std::string& table,
                const std::string& tableType);

            /**
             * Get foreign keys.
             *
             * @param primaryCatalog Primary key catalog name.
             * @param primarySchema Primary key schema name.
             * @param primaryTable Primary key table name.
             * @param foreignCatalog Foreign key catalog name.
             * @param foreignSchema Foreign key schema name.
             * @param foreignTable Foreign key table name.
             * @return Operation result.
             */
            sql_result InternalExecuteGetForeignKeysQuery(const std::string& primaryCatalog,
                const std::string& primarySchema, const std::string& primaryTable,
                const std::string& foreignCatalog, const std::string& foreignSchema,
                const std::string& foreignTable);

            /**
             * Get primary keys.
             *
             * @param catalog Catalog name.
             * @param schema Schema name.
             * @param table Table name.
             * @return Operation result.
             */
            sql_result InternalExecuteGetPrimaryKeysQuery(const std::string& catalog,
                const std::string& schema, const std::string& table);

            /**
             * Get special columns.
             *
             * @param type Special column type.
             * @param catalog Catalog name.
             * @param schema Schema name.
             * @param table Table name.
             * @param scope Minimum required scope of the rowid.
             * @param nullable Determines whether to return special columns
             *                 that can have a NULL value.
             * @return Operation result.
             */
            sql_result InternalExecuteSpecialColumnsQuery(int16_t type,
                const std::string& catalog, const std::string& schema,
                const std::string& table, int16_t scope, int16_t nullable);

            /**
             * Get type info.
             *
             * @param sqlType SQL type for which to return info or SQL_ALL_TYPES.
             * @return Operation result.
             */
            sql_result InternalExecuteGetTypeInfoQuery(int16_t sqlType);

            /**
             * Next results.
             *
             * Move to next result set or affected rows number.
             *
             * @return Operation result.
             */
            sql_result InternalMoreResults();

            /**
             * Get column attribute.
             *
             * @param colIdx Column index.
             * @param attrId Attribute ID.
             * @param strbuf Buffer for string attribute value.
             * @param buflen String buffer size.
             * @param reslen Buffer to put resulting string length to.
             * @param numbuf Numeric value buffer.
             * @return Operation result.
             */
            sql_result InternalGetColumnAttribute(uint16_t colIdx, uint16_t attrId,
                char* strbuf, int16_t buflen, int16_t* reslen, SQLLEN* numbuf);

            /**
             * Get number of rows affected by the statement.
             *
             * @param rowCnt Number of rows affected by the statement.
             * @return Operation result.
             */
            sql_result InternalAffectedRows(int64_t& rowCnt);

            /**
             * Select next parameter data for which is required.
             *
             * @param paramPtr Pointer to param id stored here.
             * @return Operation result.
             */
            sql_result InternalSelectParam(void** paramPtr);

            /**
             * Puts data for previously selected parameter or column.
             *
             * @param data Data.
             * @param len Data length.
             * @return Operation result.
             */
            sql_result InternalPutData(void* data, SQLLEN len);

            /**
             * Get type info of the parameter of the prepared statement.
             *
             * @param paramNum - parameter index.
             * @param dataType - Data type.
             * @param paramSize - Size of the parameter.
             * @param decimalDigits - big_decimal digits.
             * @param nullable - Nullability flag.
             * @return Operation result.
             */
            sql_result InternalDescribeParam(int16_t paramNum, int16_t* dataType,
                SQLULEN* paramSize, int16_t* decimalDigits, int16_t* nullable);

            /**
             * Make request to data source to update m_parameters metadata.
             */
            sql_result UpdateParamsMeta();

            /**
             * Convert SQLRESULT to SQL_ROW_RESULT.
             *
             * @return Operation result.
             */
            uint16_t sql_resultToRowResult(sql_result value);

            /**
             * Constructor.
             * Called by friend classes.
             *
             * @param parent Connection associated with the statement.
             */
            Statement(Connection& parent);

            /** Connection associated with the statement. */
            Connection& connection;

            /** Column bindings. */
            column_binding_map columnBindings;

            /** Underlying query. */
            std::auto_ptr<query::Query> currentQuery;

            /** Buffer to store number of rows fetched by the last fetch. */
            SQLINTEGER* rowsFetched;

            /** Array to store statuses of rows fetched by the last fetch. */
            SQLUSMALLINT* rowStatuses;

            /** Offset added to pointers to change binding of column data. */
            int* columnBindOffset;

            /** Row array size. */
            SQLULEN rowArraySize;

            /** Parameters. */
            parameter_set m_parameters;

            /** Query timeout in seconds. */
            int32_t timeout;
        };
    }
}

#endif //_IGNITE_ODBC_STATEMENT
