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


#include "ignite/odbc/system/odbc_constants.h"
#include "ignite/odbc/log.h"
#include "ignite/odbc/utility.h"

#include "ignite/odbc/config/configuration.h"
#include "ignite/odbc/config/connection_string_parser.h"
#include "ignite/odbc/odbc.h"
#include "ignite/odbc/sql_connection.h"
#include "ignite/odbc/sql_environment.h"
#include "ignite/odbc/sql_statement.h"
#include "ignite/odbc/type_traits.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <algorithm>

namespace ignite
{
    SQLRETURN SQLGetInfo(SQLHDBC        conn,
                         SQLUSMALLINT   infoType,
                         SQLPOINTER     infoValue,
                         SQLSMALLINT    infoValueMax,
                         SQLSMALLINT*   length)
    {
        using connection_info;
        using connection_info;

        LOG_MSG("SQLGetInfo called: "
            << infoType << " (" << connection_info::info_type_to_string(infoType) << "), "
            << std::hex << reinterpret_cast<size_t>(infoValue) << ", " << infoValueMax << ", "
            << std::hex << reinterpret_cast<size_t>(length));

        sql_connection *connection = reinterpret_cast<sql_connection *>(conn);

        if (!connection)
            return SQL_INVALID_HANDLE;

        connection->get_info(infoType, infoValue, infoValueMax, length);

        return connection->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLAllocHandle(SQLSMALLINT type, SQLHANDLE parent, SQLHANDLE* result)
    {
        //LOG_MSG("SQLAllocHandle called");
        switch (type)
        {
            case SQL_HANDLE_ENV:
                return SQLAllocEnv(result);

            case SQL_HANDLE_DBC:
                return SQLAllocConnect(parent, result);

            case SQL_HANDLE_STMT:
                return SQLAllocStmt(parent, result);

            case SQL_HANDLE_DESC:
            {
                using sql_connection;
                sql_connection *connection = reinterpret_cast<sql_connection *>(parent);

                if (!connection)
                    return SQL_INVALID_HANDLE;

                if (result)
                    *result = 0;

                connection->get_diagnostic_records().reset();
                connection->add_status_record(sql_state::SIM001_FUNCTION_NOT_SUPPORTED,
                                            "The HandleType argument was SQL_HANDLE_DESC, and "
                                            "the driver does not support allocating a descriptor handle");

                return SQL_ERROR;
            }
            default:
                break;
        }

        *result = 0;
        return SQL_ERROR;
    }

    SQLRETURN SQLAllocEnv(SQLHENV* env)
    {
        using sql_environment;

        LOG_MSG("SQLAllocEnv called");

        *env = reinterpret_cast<SQLHENV>(new sql_environment());

        return SQL_SUCCESS;
    }

    SQLRETURN SQLAllocConnect(SQLHENV env, SQLHDBC* conn)
    {
        using sql_connection;
        using sql_connection;

        LOG_MSG("SQLAllocConnect called");

        *conn = SQL_NULL_HDBC;

        sql_environment *environment = reinterpret_cast<sql_environment *>(env);

        if (!environment)
            return SQL_INVALID_HANDLE;

        sql_connection *connection = environment->create_connection();

        if (!cluster_connection)
            return environment->get_diagnostic_records().get_return_code();

        *conn = reinterpret_cast<SQLHDBC>(connection);

        return SQL_SUCCESS;
    }

    SQLRETURN SQLAllocStmt(SQLHDBC conn, SQLHSTMT* stmt)
    {
        using sql_statement;
        using sql_statement;

        LOG_MSG("SQLAllocStmt called");

        *stmt = SQL_NULL_HDBC;

        sql_connection *connection = reinterpret_cast<sql_connection *>(conn);

        if (!connection)
            return SQL_INVALID_HANDLE;

        sql_statement *statement = connection->create_statement();

        *stmt = reinterpret_cast<SQLHSTMT>(statement);

        return connection->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLFreeHandle(SQLSMALLINT type, SQLHANDLE handle)
    {
        switch (type)
        {
            case SQL_HANDLE_ENV:
                return SQLFreeEnv(handle);

            case SQL_HANDLE_DBC:
                return SQLFreeConnect(handle);

            case SQL_HANDLE_STMT:
                return SQLFreeStmt(handle, SQL_DROP);

            case SQL_HANDLE_DESC:
            default:
                break;
        }

        return SQL_ERROR;
    }

    SQLRETURN SQLFreeEnv(SQLHENV env)
    {
        using sql_environment;

        LOG_MSG("SQLFreeEnv called: " << env);

        sql_environment *environment = reinterpret_cast<sql_environment *>(env);

        if (!environment)
            return SQL_INVALID_HANDLE;

        delete environment;

        return SQL_SUCCESS;
    }

    SQLRETURN SQLFreeConnect(SQLHDBC conn)
    {
        using sql_connection;

        LOG_MSG("SQLFreeConnect called");

        sql_connection *connection = reinterpret_cast<sql_connection *>(conn);

        if (!connection)
            return SQL_INVALID_HANDLE;

        connection->deregister();

        delete connection;

        return SQL_SUCCESS;
    }

    SQLRETURN SQLFreeStmt(SQLHSTMT stmt, SQLUSMALLINT option)
    {
        using sql_statement;

        LOG_MSG("SQLFreeStmt called [option=" << option << ']');

        sql_statement *statement = reinterpret_cast<sql_statement *>(stmt);

        if (!statement)
            return SQL_INVALID_HANDLE;

        if (option == SQL_DROP)
        {
            delete statement;
            return SQL_SUCCESS;
        }

        statement->FreeResources(option);

        return statement->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLCloseCursor(SQLHSTMT stmt)
    {
        using sql_statement;

        LOG_MSG("SQLCloseCursor called");

        sql_statement *statement = reinterpret_cast<sql_statement *>(stmt);

        statement->close();

        return statement->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLDriverConnect(SQLHDBC      conn,
                               SQLHWND      windowHandle,
                               SQLCHAR*     inConnectionString,
                               SQLSMALLINT  inConnectionStringLen,
                               SQLCHAR*     outConnectionString,
                               SQLSMALLINT  outConnectionStringBufferLen,
                               SQLSMALLINT* outConnectionStringLen,
                               SQLUSMALLINT driverCompletion)
    {
        UNUSED_VALUE(driverCompletion);

        using copy_string_to_buffer;
        using diagnostic_record_storage;
        using sql_string_to_string;
        using copy_string_to_buffer;

        LOG_MSG("SQLDriverConnect called");
        if (inConnectionString)
            LOG_MSG("Connection String: [" << inConnectionString << "]");

        sql_connection *connection = reinterpret_cast<sql_connection *>(conn);

        if (!connection)
            return SQL_INVALID_HANDLE;

        std::string connectStr = sql_string_to_string(inConnectionString, inConnectionStringLen);
        connection->establish(connectStr, windowHandle);

        diagnostic_record_storage& diag = connection->get_diagnostic_records();
        if (!diag.is_successful())
            return diag.get_return_code();

        size_t result_len = copy_string_to_buffer(connectStr,
            reinterpret_cast<char*>(outConnectionString),
            static_cast<size_t>(outConnectionStringBufferLen));

        if (outConnectionStringLen)
            *outConnectionStringLen = static_cast<SQLSMALLINT>(result_len);

        if (outConnectionString)
            LOG_MSG(outConnectionString);

        return diag.get_return_code();
    }

    SQLRETURN SQLConnect(SQLHDBC        conn,
                         SQLCHAR*       server_name,
                         SQLSMALLINT    serverNameLen,
                         SQLCHAR*       userName,
                         SQLSMALLINT    userNameLen,
                         SQLCHAR*       auth,
                         SQLSMALLINT    authLen)
    {
        UNUSED_VALUE(userName);
        UNUSED_VALUE(userNameLen);
        UNUSED_VALUE(auth);
        UNUSED_VALUE(authLen);

        using configuration;
        using configuration;
        using sql_string_to_string;

        LOG_MSG("SQLConnect called\n");

        sql_connection *connection = reinterpret_cast<sql_connection *>(conn);

        if (!connection)
            return SQL_INVALID_HANDLE;

        configuration config;

        std::string dsn = sql_string_to_string(server_name, serverNameLen);

        LOG_MSG("DSN: " << dsn);

        ReadDsnConfiguration(dsn.c_str(), config, &connection->get_diagnostic_records());

        connection->establish(config);

        return connection->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLDisconnect(SQLHDBC conn)
    {
        using sql_connection;

        LOG_MSG("SQLDisconnect called");

        sql_connection *connection = reinterpret_cast<sql_connection *>(conn);

        if (!connection)
            return SQL_INVALID_HANDLE;

        connection->release();

        return connection->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLPrepare(SQLHSTMT stmt, SQLCHAR* query, SQLINTEGER queryLen)
    {
        using sql_statement;
        using sql_string_to_string;

        LOG_MSG("SQLPrepare called");

        sql_statement *statement = reinterpret_cast<sql_statement *>(stmt);

        if (!statement)
            return SQL_INVALID_HANDLE;

        std::string sql = sql_string_to_string(query, queryLen);

        LOG_MSG("SQL: " << sql);

        statement->PrepareSqlQuery(sql);

        return statement->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLExecute(SQLHSTMT stmt)
    {
        using sql_statement;

        LOG_MSG("SQLExecute called");

        sql_statement *statement = reinterpret_cast<sql_statement *>(stmt);

        if (!statement)
            return SQL_INVALID_HANDLE;

        statement->ExecuteSqlQuery();

        return statement->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLExecDirect(SQLHSTMT stmt, SQLCHAR* query, SQLINTEGER queryLen)
    {
        using sql_statement;
        using sql_string_to_string;

        LOG_MSG("SQLExecDirect called");

        sql_statement *statement = reinterpret_cast<sql_statement *>(stmt);

        if (!statement)
            return SQL_INVALID_HANDLE;

        std::string sql = sql_string_to_string(query, queryLen);

        LOG_MSG("SQL: " << sql);

        statement->ExecuteSqlQuery(sql);

        return statement->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLBindCol(SQLHSTMT       stmt,
                         SQLUSMALLINT   colNum,
                         SQLSMALLINT    targetType,
                         SQLPOINTER     targetValue,
                         SQLLEN         bufferLength,
                         SQLLEN*        strLengthOrIndicator)
    {
        using namespace type_traits;

        using application_data_buffer;
        using application_data_buffer;

        LOG_MSG("SQLBindCol called: index=" << colNum << ", type=" << targetType << 
                ", targetValue=" << reinterpret_cast<size_t>(targetValue) << 
                ", bufferLength=" << bufferLength << 
                ", lengthInd=" << reinterpret_cast<size_t>(strLengthOrIndicator));

        sql_statement *statement = reinterpret_cast<sql_statement *>(stmt);

        if (!statement)
            return SQL_INVALID_HANDLE;

        statement->BindColumn(colNum, targetType, targetValue, bufferLength, strLengthOrIndicator);

        return statement->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLFetch(SQLHSTMT stmt)
    {
        using sql_statement;

        LOG_MSG("SQLFetch called");

        sql_statement *statement = reinterpret_cast<sql_statement *>(stmt);

        if (!statement)
            return SQL_INVALID_HANDLE;

        statement->FetchRow();

        return statement->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLFetchScroll(SQLHSTMT stmt, SQLSMALLINT orientation, SQLLEN offset)
    {
        using sql_statement;

        LOG_MSG("SQLFetchScroll called");
        LOG_MSG("Orientation: " << orientation << " Offset: " << offset);

        sql_statement *statement = reinterpret_cast<sql_statement *>(stmt);

        if (!statement)
            return SQL_INVALID_HANDLE;

        statement->FetchScroll(orientation, offset);

        return statement->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLExtendedFetch(SQLHSTMT         stmt,
                               SQLUSMALLINT     orientation,
                               SQLLEN           offset,
                               SQLULEN*         row_count,
                               SQLUSMALLINT*    rowStatusArray)
    {
        LOG_MSG("SQLExtendedFetch called");

        SQLRETURN res = SQLFetchScroll(stmt, orientation, offset);

        if (res == SQL_SUCCESS)
        {
            if (row_count)
                *row_count = 1;

            if (rowStatusArray)
                rowStatusArray[0] = SQL_ROW_SUCCESS;
        }
        else if (res == SQL_NO_DATA && row_count)
            *row_count = 0;

        return res;
    }

    SQLRETURN SQLNumResultCols(SQLHSTMT stmt, SQLSMALLINT *column_num)
    {
        using column_meta_vector;
        using column_meta_vector;

        LOG_MSG("SQLNumResultCols called");

        sql_statement *statement = reinterpret_cast<sql_statement *>(stmt);

        if (!statement)
            return SQL_INVALID_HANDLE;

        int32_t res = statement->get_column_number();

        if (column_num)
        {
            *column_num = static_cast<SQLSMALLINT>(res);
            LOG_MSG("column_num: " << *column_num);
        }

        return statement->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLTables(SQLHSTMT    stmt,
                        SQLCHAR*    catalog_name,
                        SQLSMALLINT catalogNameLen,
                        SQLCHAR*    schema_name,
                        SQLSMALLINT schemaNameLen,
                        SQLCHAR*    table_name,
                        SQLSMALLINT tableNameLen,
                        SQLCHAR*    tableType,
                        SQLSMALLINT tableTypeLen)
    {
        using sql_statement;
        using sql_string_to_string;

        LOG_MSG("SQLTables called");

        sql_statement *statement = reinterpret_cast<sql_statement *>(stmt);

        if (!statement)
            return SQL_INVALID_HANDLE;

        std::string catalog = sql_string_to_string(catalog_name, catalogNameLen);
        std::string schema = sql_string_to_string(schema_name, schemaNameLen);
        std::string table = sql_string_to_string(table_name, tableNameLen);
        std::string tableTypeStr = sql_string_to_string(tableType, tableTypeLen);

        LOG_MSG("catalog: " << catalog);
        LOG_MSG("schema: " << schema);
        LOG_MSG("table: " << table);
        LOG_MSG("tableType: " << tableTypeStr);

        statement->ExecuteGetTablesMetaQuery(catalog, schema, table, tableTypeStr);

        return statement->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLColumns(SQLHSTMT       stmt,
                         SQLCHAR*       catalog_name,
                         SQLSMALLINT    catalogNameLen,
                         SQLCHAR*       schema_name,
                         SQLSMALLINT    schemaNameLen,
                         SQLCHAR*       table_name,
                         SQLSMALLINT    tableNameLen,
                         SQLCHAR*       column_name,
                         SQLSMALLINT    columnNameLen)
    {
        using sql_statement;
        using sql_string_to_string;

        LOG_MSG("SQLColumns called");

        sql_statement *statement = reinterpret_cast<sql_statement *>(stmt);

        if (!statement)
            return SQL_INVALID_HANDLE;

        std::string catalog = sql_string_to_string(catalog_name, catalogNameLen);
        std::string schema = sql_string_to_string(schema_name, schemaNameLen);
        std::string table = sql_string_to_string(table_name, tableNameLen);
        std::string column = sql_string_to_string(column_name, columnNameLen);

        LOG_MSG("catalog: " << catalog);
        LOG_MSG("schema: " << schema);
        LOG_MSG("table: " << table);
        LOG_MSG("column: " << column);

        statement->ExecuteGetColumnsMetaQuery(schema, table, column);

        return statement->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLMoreResults(SQLHSTMT stmt)
    {
        using sql_statement;

        LOG_MSG("SQLMoreResults called");

        sql_statement *statement = reinterpret_cast<sql_statement *>(stmt);

        if (!statement)
            return SQL_INVALID_HANDLE;

        statement->MoreResults();

        return statement->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLBindParameter(SQLHSTMT     stmt,
                               SQLUSMALLINT paramIdx,
                               SQLSMALLINT  ioType,
                               SQLSMALLINT  bufferType,
                               SQLSMALLINT  paramSqlType,
                               SQLULEN      columnSize,
                               SQLSMALLINT  decDigits,
                               SQLPOINTER   buffer,
                               SQLLEN       bufferLen,
                               SQLLEN*      resLen)
    {
        using sql_statement;

        LOG_MSG("SQLBindParameter called: " << paramIdx << ", " << bufferType << ", " << paramSqlType);

        sql_statement *statement = reinterpret_cast<sql_statement *>(stmt);

        if (!statement)
            return SQL_INVALID_HANDLE;

        statement->bind_parameter(paramIdx, ioType, bufferType, paramSqlType, columnSize, decDigits, buffer, bufferLen, resLen);

        return statement->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLNativeSql(SQLHDBC      conn,
                           SQLCHAR*     inQuery,
                           SQLINTEGER   inQueryLen,
                           SQLCHAR*     outQueryBuffer,
                           SQLINTEGER   outQueryBufferLen,
                           SQLINTEGER*  outQueryLen)
    {
        UNUSED_VALUE(conn);

        using namespace utility;

        LOG_MSG("SQLNativeSql called");

        std::string in = sql_string_to_string(inQuery, inQueryLen);

        copy_string_to_buffer(in, reinterpret_cast<char*>(outQueryBuffer),
            static_cast<size_t>(outQueryBufferLen));

        if (outQueryLen)
            *outQueryLen = std::min(outQueryBufferLen, static_cast<SQLINTEGER>(in.size()));

        return SQL_SUCCESS;
    }

    SQLRETURN SQLColAttribute(SQLHSTMT        stmt,
                              SQLUSMALLINT    column_num,
                              SQLUSMALLINT    field_id,
                              SQLPOINTER      strAttr,
                              SQLSMALLINT     bufferLen,
                              SQLSMALLINT*    strAttrLen,
                              SQLLEN*         numericAttr)
    {
        using column_meta;
        using column_meta_vector;
        using column_meta;

        LOG_MSG("SQLColAttribute called: " << field_id << " (" << column_meta::attr_id_to_string(field_id) << ")");

        sql_statement *statement = reinterpret_cast<sql_statement *>(stmt);

        if (!statement)
            return SQL_INVALID_HANDLE;

        // This is a special case
        if (field_id == SQL_DESC_COUNT)
        {
            SQLSMALLINT val = 0;

            SQLRETURN res = SQLNumResultCols(stmt, &val);

            if (numericAttr && res == SQL_SUCCESS)
                *numericAttr = val;

            return res;
        }

        statement->GetColumnAttribute(column_num, field_id, reinterpret_cast<char*>(strAttr),
            bufferLen, strAttrLen, numericAttr);

        return statement->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLDescribeCol(SQLHSTMT       stmt,
                             SQLUSMALLINT   column_num,
                             SQLCHAR*       columnNameBuf,
                             SQLSMALLINT    columnNameBufLen,
                             SQLSMALLINT*   columnNameLen,
                             SQLSMALLINT*   data_type,
                             SQLULEN*       columnSize,
                             SQLSMALLINT*   decimalDigits,
                             SQLSMALLINT*   nullable)
    {
        using sql_statement;
        using SQLLEN;

        LOG_MSG("SQLDescribeCol called");

        sql_statement *statement = reinterpret_cast<sql_statement *>(stmt);

        if (!statement)
            return SQL_INVALID_HANDLE;

        statement->GetColumnAttribute(column_num, SQL_DESC_NAME,
            reinterpret_cast<char*>(columnNameBuf), columnNameBufLen, columnNameLen, 0);

        SQLLEN dataTypeRes;
        SQLLEN columnSizeRes;
        SQLLEN decimalDigitsRes;
        SQLLEN nullableRes;

        statement->GetColumnAttribute(column_num, SQL_DESC_TYPE, 0, 0, 0, &dataTypeRes);
        statement->GetColumnAttribute(column_num, SQL_DESC_PRECISION, 0, 0, 0, &columnSizeRes);
        statement->GetColumnAttribute(column_num, SQL_DESC_SCALE, 0, 0, 0, &decimalDigitsRes);
        statement->GetColumnAttribute(column_num, SQL_DESC_NULLABLE, 0, 0, 0, &nullableRes);

        LOG_MSG("column_num: " << column_num);
        LOG_MSG("dataTypeRes: " << dataTypeRes);
        LOG_MSG("columnSizeRes: " << columnSizeRes);
        LOG_MSG("decimalDigitsRes: " << decimalDigitsRes);
        LOG_MSG("nullableRes: " << nullableRes);
        LOG_MSG("columnNameBuf: " << (columnNameBuf ? reinterpret_cast<const char*>(columnNameBuf) : "<null>"));
        LOG_MSG("columnNameLen: " << (columnNameLen ? *columnNameLen : -1));

        if (data_type)
            *data_type = static_cast<SQLSMALLINT>(dataTypeRes);

        if (columnSize)
            *columnSize = static_cast<SQLULEN>(columnSizeRes);

        if (decimalDigits)
            *decimalDigits = static_cast<SQLSMALLINT>(decimalDigitsRes);

        if (nullable)
            *nullable = static_cast<SQLSMALLINT>(nullableRes);

        return statement->get_diagnostic_records().get_return_code();
    }


    SQLRETURN SQLRowCount(SQLHSTMT stmt, SQLLEN* rowCnt)
    {
        using sql_statement;

        LOG_MSG("SQLRowCount called");

        sql_statement *statement = reinterpret_cast<sql_statement *>(stmt);

        if (!statement)
            return SQL_INVALID_HANDLE;

        int64_t res = statement->affected_rows();

        LOG_MSG("Row count: " << res);

        if (rowCnt)
            *rowCnt = static_cast<SQLLEN>(res);

        return statement->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLForeignKeys(SQLHSTMT       stmt,
                             SQLCHAR*       primaryCatalogName,
                             SQLSMALLINT    primaryCatalogNameLen,
                             SQLCHAR*       primarySchemaName,
                             SQLSMALLINT    primarySchemaNameLen,
                             SQLCHAR*       primaryTableName,
                             SQLSMALLINT    primaryTableNameLen,
                             SQLCHAR*       foreignCatalogName,
                             SQLSMALLINT    foreignCatalogNameLen,
                             SQLCHAR*       foreignSchemaName,
                             SQLSMALLINT    foreignSchemaNameLen,
                             SQLCHAR*       foreignTableName,
                             SQLSMALLINT    foreignTableNameLen)
    {
        using sql_statement;
        using sql_string_to_string;

        LOG_MSG("SQLForeignKeys called");

        sql_statement *statement = reinterpret_cast<sql_statement *>(stmt);

        if (!statement)
            return SQL_INVALID_HANDLE;

        std::string primaryCatalog = sql_string_to_string(primaryCatalogName, primaryCatalogNameLen);
        std::string primarySchema = sql_string_to_string(primarySchemaName, primarySchemaNameLen);
        std::string primaryTable = sql_string_to_string(primaryTableName, primaryTableNameLen);
        std::string foreignCatalog = sql_string_to_string(foreignCatalogName, foreignCatalogNameLen);
        std::string foreignSchema = sql_string_to_string(foreignSchemaName, foreignSchemaNameLen);
        std::string foreignTable = sql_string_to_string(foreignTableName, foreignTableNameLen);

        LOG_MSG("primaryCatalog: " << primaryCatalog);
        LOG_MSG("primarySchema: " << primarySchema);
        LOG_MSG("primaryTable: " << primaryTable);
        LOG_MSG("foreignCatalog: " << foreignCatalog);
        LOG_MSG("foreignSchema: " << foreignSchema);
        LOG_MSG("foreignTable: " << foreignTable);

        statement->ExecuteGetForeignKeysQuery(primaryCatalog, primarySchema,
            primaryTable, foreignCatalog, foreignSchema, foreignTable);

        return statement->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLGetStmtAttr(SQLHSTMT       stmt,
                             SQLINTEGER     attr,
                             SQLPOINTER     valueBuf,
                             SQLINTEGER     valueBufLen,
                             SQLINTEGER*    valueResLen)
    {
        using sql_statement;

        LOG_MSG("SQLGetStmtAttr called");

#ifdef _DEBUG
        using statement_attr_id_to_string;

        LOG_MSG("Attr: " << statement_attr_id_to_string(attr) << " (" << attr << ")");
#endif //_DEBUG

        sql_statement *statement = reinterpret_cast<sql_statement *>(stmt);

        if (!statement)
            return SQL_INVALID_HANDLE;

        statement->get_attribute(attr, valueBuf, valueBufLen, valueResLen);

        return statement->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLSetStmtAttr(SQLHSTMT    stmt,
                             SQLINTEGER  attr,
                             SQLPOINTER  value,
                             SQLINTEGER  valueLen)
    {
        using sql_statement;

        LOG_MSG("SQLSetStmtAttr called: " << attr);

#ifdef _DEBUG
        using statement_attr_id_to_string;

        LOG_MSG("Attr: " << statement_attr_id_to_string(attr) << " (" << attr << ")");
#endif //_DEBUG

        sql_statement *statement = reinterpret_cast<sql_statement *>(stmt);

        if (!statement)
            return SQL_INVALID_HANDLE;

        statement->set_attribute(attr, value, valueLen);

        return statement->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLPrimaryKeys(SQLHSTMT       stmt,
                             SQLCHAR*       catalog_name,
                             SQLSMALLINT    catalogNameLen,
                             SQLCHAR*       schema_name,
                             SQLSMALLINT    schemaNameLen,
                             SQLCHAR*       table_name,
                             SQLSMALLINT    tableNameLen)
    {
        using sql_statement;
        using sql_string_to_string;

        LOG_MSG("SQLPrimaryKeys called");

        sql_statement *statement = reinterpret_cast<sql_statement *>(stmt);

        if (!statement)
            return SQL_INVALID_HANDLE;

        std::string catalog = sql_string_to_string(catalog_name, catalogNameLen);
        std::string schema = sql_string_to_string(schema_name, schemaNameLen);
        std::string table = sql_string_to_string(table_name, tableNameLen);

        LOG_MSG("catalog: " << catalog);
        LOG_MSG("schema: " << schema);
        LOG_MSG("table: " << table);

        statement->ExecuteGetPrimaryKeysQuery(catalog, schema, table);

        return statement->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLNumParams(SQLHSTMT stmt, SQLSMALLINT* paramCnt)
    {
        using sql_statement;

        LOG_MSG("SQLNumParams called");

        sql_statement *statement = reinterpret_cast<sql_statement *>(stmt);

        if (!statement)
            return SQL_INVALID_HANDLE;

        if (paramCnt)
        {
            uint16_t paramNum = 0;
            statement->get_parameters_number(paramNum);

            *paramCnt = static_cast<SQLSMALLINT>(paramNum);
        }

        return statement->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLGetDiagField(SQLSMALLINT   handleType,
                              SQLHANDLE     handle,
                              SQLSMALLINT   recNum,
                              SQLSMALLINT   diagId,
                              SQLPOINTER    buffer,
                              SQLSMALLINT   bufferLen,
                              SQLSMALLINT*  resLen)
    {
        using namespace odbc;
        using namespace diagnostic;
        using namespace type_traits;

        using application_data_buffer;

        LOG_MSG("SQLGetDiagField called: " << recNum);

        SQLLEN outResLen;
        application_data_buffer outBuffer(odbc_native_type::AI_DEFAULT, buffer, bufferLen, &outResLen);

        sql_result result;

        diagnostic_field field = diagnostic_field_to_internal(diagId);

        switch (handleType)
        {
            case SQL_HANDLE_ENV:
            case SQL_HANDLE_DBC:
            case SQL_HANDLE_STMT:
            {
                diagnosable *diag = reinterpret_cast<diagnosable*>(handle);

                result = diag->get_diagnostic_records().get_field(recNum, field, outBuffer);

                break;
            }

            default:
            {
                result = sql_result::AI_NO_DATA;
                break;
            }
        }

        if (resLen && result == sql_result::AI_SUCCESS)
            *resLen = static_cast<SQLSMALLINT>(outResLen);

        return sql_result_to_return_code(result);
    }

    SQLRETURN SQLGetDiagRec(SQLSMALLINT     handleType,
                            SQLHANDLE       handle,
                            SQLSMALLINT     recNum,
                            SQLCHAR*        sql_state,
                            SQLINTEGER*     nativeError,
                            SQLCHAR*        msgBuffer,
                            SQLSMALLINT     msgBufferLen,
                            SQLSMALLINT*    msgLen)
    {
        using namespace utility;
        using namespace odbc;
        using namespace diagnostic;
        using namespace type_traits;

        using application_data_buffer;

        LOG_MSG("SQLGetDiagRec called");

        const diagnostic_record_storage* records = 0;

        switch (handleType)
        {
            case SQL_HANDLE_ENV:
            case SQL_HANDLE_DBC:
            case SQL_HANDLE_STMT:
            {
                diagnosable *diag = reinterpret_cast<diagnosable*>(handle);

                if (!diag)
                    return SQL_INVALID_HANDLE;

                records = &diag->get_diagnostic_records();

                break;
            }

            default:
                return SQL_INVALID_HANDLE;
        }

        if (recNum < 1 || msgBufferLen < 0)
            return SQL_ERROR;

        if (!records || recNum > records->get_status_records_number())
            return SQL_NO_DATA;

        const diagnostic_record& record = records->get_status_record(recNum);

        if (sql_state)
            copy_string_to_buffer(record.get_sql_state(), reinterpret_cast<char*>(sql_state), 6);

        if (nativeError)
            *nativeError = 0;

        const std::string& errMsg = record.get_message_text();

        if (!msgBuffer || msgBufferLen < static_cast<SQLSMALLINT>(errMsg.size() + 1))
        {
            if (!msgLen)
                return SQL_ERROR;

            copy_string_to_buffer(errMsg, reinterpret_cast<char*>(msgBuffer), static_cast<size_t>(msgBufferLen));

            *msgLen = static_cast<SQLSMALLINT>(errMsg.size());

            return SQL_SUCCESS_WITH_INFO;
        }

        copy_string_to_buffer(errMsg, reinterpret_cast<char*>(msgBuffer), static_cast<size_t>(msgBufferLen));

        if (msgLen)
            *msgLen = static_cast<SQLSMALLINT>(errMsg.size());

        return SQL_SUCCESS;
    }

    SQLRETURN SQLGetTypeInfo(SQLHSTMT stmt, SQLSMALLINT type)
    {
        using sql_statement;

        LOG_MSG("SQLGetTypeInfo called: [type=" << type << ']');

        sql_statement *statement = reinterpret_cast<sql_statement *>(stmt);

        if (!statement)
            return SQL_INVALID_HANDLE;

        statement->ExecuteGetTypeInfoQuery(static_cast<int16_t>(type));

        return statement->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLEndTran(SQLSMALLINT handleType, SQLHANDLE handle, SQLSMALLINT completionType)
    {
        using namespace odbc;

        LOG_MSG("SQLEndTran called");

        SQLRETURN result;

        switch (handleType)
        {
            case SQL_HANDLE_ENV:
            {
                sql_environment *env = reinterpret_cast<sql_environment *>(handle);

                if (!env)
                    return SQL_INVALID_HANDLE;

                if (completionType == SQL_COMMIT)
                    env->transaction_commit();
                else
                    env->transaction_rollback();

                result = env->get_diagnostic_records().get_return_code();

                break;
            }

            case SQL_HANDLE_DBC:
            {
                connection *conn = reinterpret_cast<connection*>(handle);

                if (!conn)
                    return SQL_INVALID_HANDLE;

                if (completionType == SQL_COMMIT)
                    conn->transaction_commit();
                else
                    conn->transaction_rollback();

                result = conn->get_diagnostic_records().get_return_code();

                break;
            }

            default:
            {
                result = SQL_INVALID_HANDLE;

                break;
            }
        }

        return result;
    }

    SQLRETURN SQLGetData(SQLHSTMT       stmt,
                         SQLUSMALLINT   colNum,
                         SQLSMALLINT    targetType,
                         SQLPOINTER     targetValue,
                         SQLLEN         bufferLength,
                         SQLLEN*        strLengthOrIndicator)
    {
        using namespace type_traits;

        using application_data_buffer;
        using application_data_buffer;

        LOG_MSG("SQLGetData called");

        sql_statement *statement = reinterpret_cast<sql_statement *>(stmt);

        if (!statement)
            return SQL_INVALID_HANDLE;

        odbc_native_type driverType = to_driver_type(targetType);

        application_data_buffer dataBuffer(driverType, targetValue, bufferLength, strLengthOrIndicator);

        statement->GetColumnData(colNum, dataBuffer);

        return statement->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLSetEnvAttr(SQLHENV     env,
                            SQLINTEGER  attr,
                            SQLPOINTER  value,
                            SQLINTEGER  valueLen)
    {
        using sql_environment;

        LOG_MSG("SQLSetEnvAttr called");
        LOG_MSG("Attribute: " << attr << ", Value: " << (size_t)value);

        sql_environment *environment = reinterpret_cast<sql_environment *>(env);

        if (!environment)
            return SQL_INVALID_HANDLE;

        environment->set_attribute(attr, value, valueLen);

        return environment->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLGetEnvAttr(SQLHENV     env,
                            SQLINTEGER  attr,
                            SQLPOINTER  valueBuf,
                            SQLINTEGER  valueBufLen,
                            SQLINTEGER* valueResLen)
    {
        using namespace odbc;
        using namespace type_traits;

        using application_data_buffer;

        LOG_MSG("SQLGetEnvAttr called");

        sql_environment *environment = reinterpret_cast<sql_environment *>(env);

        if (!environment)
            return SQL_INVALID_HANDLE;

        SQLLEN outResLen;
        application_data_buffer outBuffer(odbc_native_type::AI_SIGNED_LONG, valueBuf,
            static_cast<int32_t>(valueBufLen), &outResLen);

        environment->get_attribute(attr, outBuffer);

        if (valueResLen)
            *valueResLen = static_cast<SQLSMALLINT>(outResLen);

        return environment->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLSpecialColumns(SQLHSTMT     stmt,
                                SQLUSMALLINT id_type,
                                SQLCHAR*     catalog_name,
                                SQLSMALLINT  catalogNameLen,
                                SQLCHAR*     schema_name,
                                SQLSMALLINT  schemaNameLen,
                                SQLCHAR*     table_name,
                                SQLSMALLINT  tableNameLen,
                                SQLUSMALLINT scope,
                                SQLUSMALLINT nullable)
    {
        using namespace odbc;

        using sql_string_to_string;

        LOG_MSG("SQLSpecialColumns called");

        sql_statement *statement = reinterpret_cast<sql_statement *>(stmt);

        if (!statement)
            return SQL_INVALID_HANDLE;

        std::string catalog = sql_string_to_string(catalog_name, catalogNameLen);
        std::string schema = sql_string_to_string(schema_name, schemaNameLen);
        std::string table = sql_string_to_string(table_name, tableNameLen);

        LOG_MSG("catalog: " << catalog);
        LOG_MSG("schema: " << schema);
        LOG_MSG("table: " << table);

        statement->ExecuteSpecialColumnsQuery(id_type, catalog, schema, table, scope, nullable);

        return statement->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLParamData(SQLHSTMT stmt, SQLPOINTER* value)
    {
        using namespace ignite::odbc;

        LOG_MSG("SQLParamData called");

        sql_statement *statement = reinterpret_cast<sql_statement *>(stmt);

        if (!statement)
            return SQL_INVALID_HANDLE;

        statement->SelectParam(value);

        return statement->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLPutData(SQLHSTMT stmt, SQLPOINTER data, SQLLEN strLengthOrIndicator)
    {
        using namespace ignite::odbc;

        LOG_MSG("SQLPutData called");

        sql_statement *statement = reinterpret_cast<sql_statement *>(stmt);

        if (!statement)
            return SQL_INVALID_HANDLE;

        statement->put_data(data, strLengthOrIndicator);

        return statement->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLDescribeParam(SQLHSTMT     stmt,
                               SQLUSMALLINT paramNum,
                               SQLSMALLINT* data_type,
                               SQLULEN*     paramSize,
                               SQLSMALLINT* decimalDigits,
                               SQLSMALLINT* nullable)
    {
        using namespace ignite::odbc;

        LOG_MSG("SQLDescribeParam called");

        sql_statement *statement = reinterpret_cast<sql_statement *>(stmt);

        if (!statement)
            return SQL_INVALID_HANDLE;

        statement->DescribeParam(paramNum, data_type, paramSize, decimalDigits, nullable);

        return statement->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQLError(SQLHENV      env,
                       SQLHDBC      conn,
                       SQLHSTMT     stmt,
                       SQLCHAR*     state,
                       SQLINTEGER*  error,
                       SQLCHAR*     msgBuf,
                       SQLSMALLINT  msgBufLen,
                       SQLSMALLINT* msgResLen)
    {
        using namespace ignite::utility;
        using namespace ignite::odbc;
        using namespace ignite::diagnostic;
        using namespace ignite::type_traits;

        using ignite::application_data_buffer;

        LOG_MSG("SQLError called");

        SQLHANDLE handle = 0;

        if (env != 0)
            handle = static_cast<SQLHANDLE>(env);
        else if (conn != 0)
            handle = static_cast<SQLHANDLE>(conn);
        else if (stmt != 0)
            handle = static_cast<SQLHANDLE>(stmt);
        else
            return SQL_INVALID_HANDLE;

        diagnosable *diag = reinterpret_cast<diagnosable*>(handle);

        diagnostic_record_storage& records = diag->get_diagnostic_records();

        int32_t recNum = records.get_last_non_retrieved();

        if (recNum < 1 || recNum > records.get_status_records_number())
            return SQL_NO_DATA;

        diagnostic_record& record = records.get_status_record(recNum);

        record.mark_retrieved();

        if (state)
            copy_string_to_buffer(record.get_sql_state(), reinterpret_cast<char*>(state), 6);

        if (error)
            *error = 0;

        SQLLEN outResLen;
        application_data_buffer outBuffer(odbc_native_type::AI_CHAR, msgBuf, msgBufLen, &outResLen);

        outBuffer.put_string(record.get_message_text());

        if (msgResLen)
            *msgResLen = static_cast<SQLSMALLINT>(outResLen);

        return SQL_SUCCESS;
    }

    SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC    conn,
                                        SQLINTEGER attr,
                                        SQLPOINTER valueBuf,
                                        SQLINTEGER valueBufLen,
                                        SQLINTEGER* valueResLen)
    {
        using namespace odbc;
        using namespace type_traits;

        using application_data_buffer;

        LOG_MSG("SQLGetConnectAttr called");

        sql_connection *connection = reinterpret_cast<sql_connection *>(conn);

        if (!connection)
            return SQL_INVALID_HANDLE;

        connection->get_attribute(attr, valueBuf, valueBufLen, valueResLen);

        return connection->get_diagnostic_records().get_return_code();
    }

    SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC    conn,
                                        SQLINTEGER attr,
                                        SQLPOINTER value,
                                        SQLINTEGER valueLen)
    {
        using sql_connection;

        LOG_MSG("SQLSetConnectAttr called(" << attr << ", " << value << ")");

        sql_connection *connection = reinterpret_cast<sql_connection *>(conn);

        if (!connection)
            return SQL_INVALID_HANDLE;

        connection->set_attribute(attr, value, valueLen);

        return connection->get_diagnostic_records().get_return_code();
    }

} // namespace ignite;
