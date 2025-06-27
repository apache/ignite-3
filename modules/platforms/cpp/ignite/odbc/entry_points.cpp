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

#include "log.h"
#include "odbc.h"

#include "ignite/common/detail/config.h"

#ifdef __JETBRAINS_IDE__
# pragma clang diagnostic push
# pragma ide diagnostic ignored "readability-non-const-parameter"
#endif

SQLRETURN SQL_API SQLGetInfo(
    SQLHDBC conn, SQLUSMALLINT infoType, SQLPOINTER infoValue, SQLSMALLINT infoValueMax, SQLSMALLINT *length) {
    return ignite::SQLGetInfo(conn, infoType, infoValue, infoValueMax, length);
}

SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT type, SQLHANDLE parent, SQLHANDLE *result) {
    return ignite::SQLAllocHandle(type, parent, result);
}

SQLRETURN SQL_API SQLAllocEnv(SQLHENV *env) {
    return ignite::SQLAllocEnv(env);
}

SQLRETURN SQL_API SQLAllocConnect(SQLHENV env, SQLHDBC *conn) {
    return ignite::SQLAllocConnect(env, conn);
}

SQLRETURN SQL_API SQLAllocStmt(SQLHDBC conn, SQLHSTMT *stmt) {
    return ignite::SQLAllocStmt(conn, stmt);
}

SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT type, SQLHANDLE handle) {
    return ignite::SQLFreeHandle(type, handle);
}

SQLRETURN SQL_API SQLFreeEnv(SQLHENV env) {
    return ignite::SQLFreeEnv(env);
}

SQLRETURN SQL_API SQLFreeConnect(SQLHDBC conn) {
    return ignite::SQLFreeConnect(conn);
}

SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT stmt, SQLUSMALLINT option) {
    return ignite::SQLFreeStmt(stmt, option);
}

SQLRETURN SQL_API SQLCloseCursor(SQLHSTMT stmt) {
    return ignite::SQLCloseCursor(stmt);
}

SQLRETURN SQL_API SQLDriverConnect(SQLHDBC conn, SQLHWND windowHandle, SQLCHAR *inConnectionString,
    SQLSMALLINT inConnectionStringLen, SQLCHAR *outConnectionString, SQLSMALLINT outConnectionStringBufferLen,
    SQLSMALLINT *outConnectionStringLen, SQLUSMALLINT driverCompletion) {
    return ignite::SQLDriverConnect(conn, windowHandle, inConnectionString, inConnectionStringLen, outConnectionString,
        outConnectionStringBufferLen, outConnectionStringLen, driverCompletion);
}

SQLRETURN SQL_API SQLConnect(SQLHDBC conn, SQLCHAR *server_name, SQLSMALLINT server_name_len, SQLCHAR *user_name,
    SQLSMALLINT user_name_len, SQLCHAR *auth, SQLSMALLINT auth_len) {
    return ignite::SQLConnect(conn, server_name, server_name_len, user_name, user_name_len, auth, auth_len);
}

SQLRETURN SQL_API SQLDisconnect(SQLHDBC conn) {
    return ignite::SQLDisconnect(conn);
}

SQLRETURN SQL_API SQLPrepare(SQLHSTMT stmt, SQLCHAR *query, SQLINTEGER query_len) {
    return ignite::SQLPrepare(stmt, query, query_len);
}

SQLRETURN SQL_API SQLExecute(SQLHSTMT stmt) {
    return ignite::SQLExecute(stmt);
}

SQLRETURN SQL_API SQLExecDirect(SQLHSTMT stmt, SQLCHAR *query, SQLINTEGER query_len) {
    return ignite::SQLExecDirect(stmt, query, query_len);
}

SQLRETURN SQL_API SQLBindCol(SQLHSTMT stmt, SQLUSMALLINT col_num, SQLSMALLINT target_type, SQLPOINTER target_value,
    SQLLEN buffer_length, SQLLEN *str_length_or_indicator) {
    return ignite::SQLBindCol(stmt, col_num, target_type, target_value, buffer_length, str_length_or_indicator);
}

SQLRETURN SQL_API SQLFetch(SQLHSTMT stmt) {
    return ignite::SQLFetch(stmt);
}

SQLRETURN SQL_API SQLFetchScroll(SQLHSTMT stmt, SQLSMALLINT orientation, SQLLEN offset) {
    return ignite::SQLFetchScroll(stmt, orientation, offset);
}

SQLRETURN SQL_API SQLExtendedFetch(
    SQLHSTMT stmt, SQLUSMALLINT orientation, SQLLEN offset, SQLULEN *row_count, SQLUSMALLINT *row_status_array) {
    return ignite::SQLExtendedFetch(stmt, orientation, offset, row_count, row_status_array);
}

SQLRETURN SQL_API SQLNumResultCols(SQLHSTMT stmt, SQLSMALLINT *column_num) {
    return ignite::SQLNumResultCols(stmt, column_num);
}

SQLRETURN SQL_API SQLTables(SQLHSTMT stmt, SQLCHAR *catalog_name, SQLSMALLINT catalog_name_len, SQLCHAR *schema_name,
    SQLSMALLINT schema_name_len, SQLCHAR *table_name, SQLSMALLINT table_name_len, SQLCHAR *table_type,
    SQLSMALLINT table_type_len) {
    return ignite::SQLTables(stmt, catalog_name, catalog_name_len, schema_name, schema_name_len, table_name,
        table_name_len, table_type, table_type_len);
}

SQLRETURN SQL_API SQLColumns(SQLHSTMT stmt, SQLCHAR *catalog_name, SQLSMALLINT catalog_name_len, SQLCHAR *schema_name,
    SQLSMALLINT schema_name_len, SQLCHAR *table_name, SQLSMALLINT table_name_len, SQLCHAR *column_name,
    SQLSMALLINT column_name_len) {
    return ignite::SQLColumns(stmt, catalog_name, catalog_name_len, schema_name, schema_name_len, table_name,
        table_name_len, column_name, column_name_len);
}

SQLRETURN SQL_API SQLMoreResults(SQLHSTMT stmt) {
    return ignite::SQLMoreResults(stmt);
}

SQLRETURN SQL_API SQLBindParameter(SQLHSTMT stmt, SQLUSMALLINT param_idx, SQLSMALLINT io_type, SQLSMALLINT buffer_type,
    SQLSMALLINT param_sql_type, SQLULEN column_size, SQLSMALLINT dec_digits, SQLPOINTER buffer, SQLLEN buffer_len,
    SQLLEN *res_len) {
    return ignite::SQLBindParameter(
        stmt, param_idx, io_type, buffer_type, param_sql_type, column_size, dec_digits, buffer, buffer_len, res_len);
}

SQLRETURN SQL_API SQLNativeSql(SQLHDBC conn, SQLCHAR *in_query, SQLINTEGER in_query_len, SQLCHAR *out_query_buffer,
    SQLINTEGER out_query_buffer_len, SQLINTEGER *out_query_len) {
    return ignite::SQLNativeSql(conn, in_query, in_query_len, out_query_buffer, out_query_buffer_len, out_query_len);
}

#if defined _WIN64 || !defined _WIN32
SQLRETURN SQL_API SQLColAttribute(SQLHSTMT stmt, SQLUSMALLINT column_num, SQLUSMALLINT field_id, SQLPOINTER str_attr,
    SQLSMALLINT buffer_len, SQLSMALLINT *str_attr_len, SQLLEN *numeric_attr)
#else
SQLRETURN SQL_API SQLColAttribute(SQLHSTMT stmt, SQLUSMALLINT column_num, SQLUSMALLINT field_id, SQLPOINTER str_attr,
    SQLSMALLINT buffer_len, SQLSMALLINT *str_attr_len, SQLPOINTER numeric_attr)
#endif
{
    return ignite::SQLColAttribute(
        stmt, column_num, field_id, str_attr, buffer_len, str_attr_len, (SQLLEN *) numeric_attr);
}

SQLRETURN SQL_API SQLDescribeCol(SQLHSTMT stmt, SQLUSMALLINT column_num, SQLCHAR *column_name_buf,
    SQLSMALLINT column_name_buf_len, SQLSMALLINT *column_name_len, SQLSMALLINT *data_type, SQLULEN *column_size,
    SQLSMALLINT *decimal_digits, SQLSMALLINT *nullable) {
    return ignite::SQLDescribeCol(stmt, column_num, column_name_buf, column_name_buf_len, column_name_len, data_type,
        column_size, decimal_digits, nullable);
}

SQLRETURN SQL_API SQLRowCount(SQLHSTMT stmt, SQLLEN *row_count) {
    return ignite::SQLRowCount(stmt, row_count);
}

SQLRETURN SQL_API SQLForeignKeys(SQLHSTMT stmt, SQLCHAR *primary_catalog_name, SQLSMALLINT primary_catalog_name_len,
    SQLCHAR *primary_schema_name, SQLSMALLINT primary_schema_name_len, SQLCHAR *primary_table_name,
    SQLSMALLINT primary_table_name_len, SQLCHAR *foreign_catalog_name, SQLSMALLINT foreign_catalog_name_len,
    SQLCHAR *foreign_schema_name, SQLSMALLINT foreign_schema_name_len, SQLCHAR *foreign_table_name,
    SQLSMALLINT foreign_table_name_len) {
    return ignite::SQLForeignKeys(stmt, primary_catalog_name, primary_catalog_name_len, primary_schema_name,
        primary_schema_name_len, primary_table_name, primary_table_name_len, foreign_catalog_name,
        foreign_catalog_name_len, foreign_schema_name, foreign_schema_name_len, foreign_table_name,
        foreign_table_name_len);
}

SQLRETURN SQL_API SQLGetStmtAttr(
    SQLHSTMT stmt, SQLINTEGER attr, SQLPOINTER value_buf, SQLINTEGER value_buf_len, SQLINTEGER *value_res_len) {
    return ignite::SQLGetStmtAttr(stmt, attr, value_buf, value_buf_len, value_res_len);
}

SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT stmt, SQLINTEGER attr, SQLPOINTER value, SQLINTEGER value_len) {
    return ignite::SQLSetStmtAttr(stmt, attr, value, value_len);
}

SQLRETURN SQL_API SQLPrimaryKeys(SQLHSTMT stmt, SQLCHAR *catalog_name, SQLSMALLINT catalog_name_len,
    SQLCHAR *schema_name, SQLSMALLINT schema_name_len, SQLCHAR *table_name, SQLSMALLINT table_name_len) {
    return ignite::SQLPrimaryKeys(
        stmt, catalog_name, catalog_name_len, schema_name, schema_name_len, table_name, table_name_len);
}

SQLRETURN SQL_API SQLNumParams(SQLHSTMT stmt, SQLSMALLINT *param_cnt) {
    return ignite::SQLNumParams(stmt, param_cnt);
}

SQLRETURN SQL_API SQLGetDiagField(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT rec_num, SQLSMALLINT diag_id,
    SQLPOINTER buffer, SQLSMALLINT buffer_len, SQLSMALLINT *res_len) {
    return ignite::SQLGetDiagField(handle_type, handle, rec_num, diag_id, buffer, buffer_len, res_len);
}

SQLRETURN SQL_API SQLGetDiagRec(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT rec_num, SQLCHAR *sql_state,
    SQLINTEGER *native_error, SQLCHAR *msg_buffer, SQLSMALLINT msg_buffer_len, SQLSMALLINT *msg_len) {
    return ignite::SQLGetDiagRec(
        handle_type, handle, rec_num, sql_state, native_error, msg_buffer, msg_buffer_len, msg_len);
}

SQLRETURN SQL_API SQLGetTypeInfo(SQLHSTMT stmt, SQLSMALLINT type) {
    return ignite::SQLGetTypeInfo(stmt, type);
}

SQLRETURN SQL_API SQLEndTran(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT completion_type) {
    return ignite::SQLEndTran(handle_type, handle, completion_type);
}

SQLRETURN SQL_API SQLGetData(SQLHSTMT stmt, SQLUSMALLINT col_num, SQLSMALLINT target_type, SQLPOINTER target_value,
    SQLLEN buffer_length, SQLLEN *str_length_or_indicator) {
    return ignite::SQLGetData(stmt, col_num, target_type, target_value, buffer_length, str_length_or_indicator);
}

SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV env, SQLINTEGER attr, SQLPOINTER value, SQLINTEGER value_len) {
    return ignite::SQLSetEnvAttr(env, attr, value, value_len);
}

SQLRETURN SQL_API SQLGetEnvAttr(
    SQLHENV env, SQLINTEGER attr, SQLPOINTER value_buf, SQLINTEGER value_buf_len, SQLINTEGER *value_res_len) {
    return ignite::SQLGetEnvAttr(env, attr, value_buf, value_buf_len, value_res_len);
}

SQLRETURN SQL_API SQLSpecialColumns(SQLHSTMT stmt, SQLUSMALLINT id_type, SQLCHAR *catalog_name,
    SQLSMALLINT catalog_name_len, SQLCHAR *schema_name, SQLSMALLINT schema_name_len, SQLCHAR *table_name,
    SQLSMALLINT table_name_len, SQLUSMALLINT scope, SQLUSMALLINT nullable) {
    return ignite::SQLSpecialColumns(stmt, id_type, catalog_name, catalog_name_len, schema_name, schema_name_len,
        table_name, table_name_len, scope, nullable);
}

SQLRETURN SQL_API SQLParamData(SQLHSTMT stmt, SQLPOINTER *value) {
    return ignite::SQLParamData(stmt, value);
}

SQLRETURN SQL_API SQLPutData(SQLHSTMT stmt, SQLPOINTER data, SQLLEN str_length_or_indicator) {
    return ignite::SQLPutData(stmt, data, str_length_or_indicator);
}

SQLRETURN SQL_API SQLDescribeParam(SQLHSTMT stmt, SQLUSMALLINT param_num, SQLSMALLINT *data_type, SQLULEN *param_size,
    SQLSMALLINT *decimal_digits, SQLSMALLINT *nullable) {
    return ignite::SQLDescribeParam(stmt, param_num, data_type, param_size, decimal_digits, nullable);
}

SQLRETURN SQL_API SQLError(SQLHENV env, SQLHDBC conn, SQLHSTMT stmt, SQLCHAR *state, SQLINTEGER *error,
    SQLCHAR *msg_buf, SQLSMALLINT msg_buf_len, SQLSMALLINT *msg_res_len) {
    return ignite::SQLError(env, conn, stmt, state, error, msg_buf, msg_buf_len, msg_res_len);
}

SQLRETURN SQL_API SQLGetConnectAttr(
    SQLHDBC conn, SQLINTEGER attr, SQLPOINTER value_buf, SQLINTEGER value_buf_len, SQLINTEGER *value_res_len) {
    return ignite::SQLGetConnectAttr(conn, attr, value_buf, value_buf_len, value_res_len);
}

SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC conn, SQLINTEGER attr, SQLPOINTER value, SQLINTEGER value_len) {
    return ignite::SQLSetConnectAttr(conn, attr, value, value_len);
}

//
// ==== Not implemented ====
//

SQLRETURN SQL_API SQLCancel(SQLHSTMT stmt) {
    UNUSED_VALUE(stmt);

    LOG_MSG("SQLCancel called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLColAttributes(SQLHSTMT stmt, SQLUSMALLINT col_num, SQLUSMALLINT field_id, SQLPOINTER strAttrBuf,
    SQLSMALLINT strAttrBufLen, SQLSMALLINT *strAttrResLen, SQLLEN *numAttrBuf) {
    UNUSED_VALUE(stmt);
    UNUSED_VALUE(col_num);
    UNUSED_VALUE(field_id);
    UNUSED_VALUE(strAttrBuf);
    UNUSED_VALUE(strAttrBufLen);
    UNUSED_VALUE(strAttrResLen);
    UNUSED_VALUE(numAttrBuf);

    LOG_MSG("SQLColAttributes called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLGetCursorName(SQLHSTMT stmt, SQLCHAR *nameBuf, SQLSMALLINT nameBufLen, SQLSMALLINT *nameResLen) {
    UNUSED_VALUE(stmt);
    UNUSED_VALUE(nameBuf);
    UNUSED_VALUE(nameBufLen);
    UNUSED_VALUE(nameResLen);

    LOG_MSG("SQLGetCursorName called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetCursorName(SQLHSTMT stmt, SQLCHAR *name, SQLSMALLINT nameLen) {
    UNUSED_VALUE(stmt);
    UNUSED_VALUE(name);
    UNUSED_VALUE(nameLen);

    LOG_MSG("SQLSetCursorName called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLGetConnectOption(SQLHDBC conn, SQLUSMALLINT option, SQLPOINTER value) {
    UNUSED_VALUE(conn);
    UNUSED_VALUE(option);
    UNUSED_VALUE(value);

    LOG_MSG("SQLGetConnectOption called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLGetStmtOption(SQLHSTMT stmt, SQLUSMALLINT option, SQLPOINTER value) {
    UNUSED_VALUE(stmt);
    UNUSED_VALUE(option);
    UNUSED_VALUE(value);

    LOG_MSG("SQLGetStmtOption called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetConnectOption(SQLHDBC conn, SQLUSMALLINT option, SQLULEN value) {
    UNUSED_VALUE(conn);
    UNUSED_VALUE(option);
    UNUSED_VALUE(value);

    LOG_MSG("SQLSetConnectOption called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetStmtOption(SQLHSTMT stmt, SQLUSMALLINT option, SQLULEN value) {
    UNUSED_VALUE(stmt);
    UNUSED_VALUE(option);
    UNUSED_VALUE(value);

    LOG_MSG("SQLSetStmtOption called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLStatistics(SQLHSTMT stmt, SQLCHAR *catalog_name, SQLSMALLINT catalog_name_len,
    SQLCHAR *schema_name, SQLSMALLINT schema_name_len, SQLCHAR *table_name, SQLSMALLINT table_name_len,
    SQLUSMALLINT unique, SQLUSMALLINT reserved) {
    UNUSED_VALUE(stmt);
    UNUSED_VALUE(catalog_name);
    UNUSED_VALUE(catalog_name_len);
    UNUSED_VALUE(schema_name);
    UNUSED_VALUE(schema_name_len);
    UNUSED_VALUE(table_name);
    UNUSED_VALUE(table_name_len);
    UNUSED_VALUE(unique);
    UNUSED_VALUE(reserved);

    LOG_MSG("SQLStatistics called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLBrowseConnect(SQLHDBC conn, SQLCHAR *inConnectionStr, SQLSMALLINT inConnectionStrLen,
    SQLCHAR *outConnectionStrBuf, SQLSMALLINT outConnectionStrBufLen, SQLSMALLINT *outConnectionStrResLen) {
    UNUSED_VALUE(conn);
    UNUSED_VALUE(inConnectionStr);
    UNUSED_VALUE(inConnectionStrLen);
    UNUSED_VALUE(outConnectionStrBuf);
    UNUSED_VALUE(outConnectionStrBufLen);
    UNUSED_VALUE(outConnectionStrResLen);

    LOG_MSG("SQLBrowseConnect called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLProcedureColumns(SQLHSTMT stmt, SQLCHAR *catalog_name, SQLSMALLINT catalog_name_len,
    SQLCHAR *schema_name, SQLSMALLINT schema_name_len, SQLCHAR *procName, SQLSMALLINT procNameLen, SQLCHAR *column_name,
    SQLSMALLINT column_name_len) {
    UNUSED_VALUE(stmt);
    UNUSED_VALUE(catalog_name);
    UNUSED_VALUE(catalog_name_len);
    UNUSED_VALUE(schema_name);
    UNUSED_VALUE(schema_name_len);
    UNUSED_VALUE(procName);
    UNUSED_VALUE(procNameLen);
    UNUSED_VALUE(column_name);
    UNUSED_VALUE(column_name_len);

    LOG_MSG("SQLProcedureColumns called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetPos(SQLHSTMT stmt, SQLSETPOSIROW row_num, SQLUSMALLINT operation, SQLUSMALLINT lockType) {
    UNUSED_VALUE(stmt);
    UNUSED_VALUE(row_num);
    UNUSED_VALUE(operation);
    UNUSED_VALUE(lockType);

    LOG_MSG("SQLSetPos called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetScrollOptions(
    SQLHSTMT stmt, SQLUSMALLINT concurrency, SQLLEN crowKeyset, SQLUSMALLINT crowRowset) {
    UNUSED_VALUE(stmt);
    UNUSED_VALUE(concurrency);
    UNUSED_VALUE(crowKeyset);
    UNUSED_VALUE(crowRowset);

    LOG_MSG("SQLSetScrollOptions called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLBulkOperations(SQLHSTMT stmt, SQLUSMALLINT operation) {
    UNUSED_VALUE(stmt);
    UNUSED_VALUE(operation);

    LOG_MSG("SQLBulkOperations called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLTablePrivileges(SQLHSTMT stmt, SQLCHAR *catalog_name, SQLSMALLINT catalog_name_len,
    SQLCHAR *schema_name, SQLSMALLINT schema_name_len, SQLCHAR *table_name, SQLSMALLINT table_name_len) {
    UNUSED_VALUE(stmt);
    UNUSED_VALUE(catalog_name);
    UNUSED_VALUE(catalog_name_len);
    UNUSED_VALUE(schema_name);
    UNUSED_VALUE(schema_name_len);
    UNUSED_VALUE(table_name);
    UNUSED_VALUE(table_name_len);

    LOG_MSG("SQLTablePrivileges called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLCopyDesc(SQLHDESC src, SQLHDESC dst) {
    UNUSED_VALUE(src);
    UNUSED_VALUE(dst);

    LOG_MSG("SQLCopyDesc called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLGetDescField(SQLHDESC descr, SQLSMALLINT rec_num, SQLSMALLINT field_id, SQLPOINTER buffer,
    SQLINTEGER buffer_len, SQLINTEGER *res_len) {
    UNUSED_VALUE(descr);
    UNUSED_VALUE(rec_num);
    UNUSED_VALUE(field_id);
    UNUSED_VALUE(buffer);
    UNUSED_VALUE(buffer_len);
    UNUSED_VALUE(res_len);

    LOG_MSG("SQLGetDescField called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLGetDescRec(SQLHDESC DescriptorHandle, SQLSMALLINT RecNumber, SQLCHAR *nameBuffer,
    SQLSMALLINT nameBufferLen, SQLSMALLINT *strLen, SQLSMALLINT *type, SQLSMALLINT *subType, SQLLEN *len,
    SQLSMALLINT *precision, SQLSMALLINT *scale, SQLSMALLINT *nullable) {
    UNUSED_VALUE(DescriptorHandle);
    UNUSED_VALUE(RecNumber);
    UNUSED_VALUE(nameBuffer);
    UNUSED_VALUE(nameBufferLen);
    UNUSED_VALUE(strLen);
    UNUSED_VALUE(type);
    UNUSED_VALUE(subType);
    UNUSED_VALUE(len);
    UNUSED_VALUE(precision);
    UNUSED_VALUE(scale);
    UNUSED_VALUE(nullable);

    LOG_MSG("SQLGetDescRec called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetDescField(
    SQLHDESC descr, SQLSMALLINT rec_num, SQLSMALLINT field_id, SQLPOINTER buffer, SQLINTEGER buffer_len) {
    UNUSED_VALUE(descr);
    UNUSED_VALUE(rec_num);
    UNUSED_VALUE(field_id);
    UNUSED_VALUE(buffer);
    UNUSED_VALUE(buffer_len);

    LOG_MSG("SQLSetDescField called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetDescRec(SQLHDESC descr, SQLSMALLINT rec_num, SQLSMALLINT type, SQLSMALLINT subType, SQLLEN len,
    SQLSMALLINT precision, SQLSMALLINT scale, SQLPOINTER buffer, SQLLEN *res_len, SQLLEN *id) {
    UNUSED_VALUE(descr);
    UNUSED_VALUE(rec_num);
    UNUSED_VALUE(type);
    UNUSED_VALUE(subType);
    UNUSED_VALUE(len);
    UNUSED_VALUE(precision);
    UNUSED_VALUE(scale);
    UNUSED_VALUE(buffer);
    UNUSED_VALUE(res_len);
    UNUSED_VALUE(id);

    LOG_MSG("SQLSetDescRec called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLColumnPrivileges(SQLHSTMT stmt, SQLCHAR *catalog_name, SQLSMALLINT catalog_name_len,
    SQLCHAR *schema_name, SQLSMALLINT schema_name_len, SQLCHAR *table_name, SQLSMALLINT table_name_len,
    SQLCHAR *column_name, SQLSMALLINT column_name_len) {
    UNUSED_VALUE(stmt);
    UNUSED_VALUE(catalog_name);
    UNUSED_VALUE(catalog_name_len);
    UNUSED_VALUE(schema_name);
    UNUSED_VALUE(schema_name_len);
    UNUSED_VALUE(table_name);
    UNUSED_VALUE(table_name_len);
    UNUSED_VALUE(column_name);
    UNUSED_VALUE(column_name_len);

    LOG_MSG("SQLColumnPrivileges called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLParamOptions(SQLHSTMT stmt, SQLULEN paramSetSize, SQLULEN *paramsProcessed) {
    UNUSED_VALUE(stmt);
    UNUSED_VALUE(paramSetSize);
    UNUSED_VALUE(paramsProcessed);

    LOG_MSG("SQLParamOptions called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLProcedures(SQLHSTMT stmt, SQLCHAR *catalog_name, SQLSMALLINT catalog_name_len,
    SQLCHAR *schema_name, SQLSMALLINT schema_name_len, SQLCHAR *table_name, SQLSMALLINT table_name_len) {
    UNUSED_VALUE(stmt);
    UNUSED_VALUE(catalog_name);
    UNUSED_VALUE(catalog_name_len);
    UNUSED_VALUE(schema_name);
    UNUSED_VALUE(schema_name_len);
    UNUSED_VALUE(table_name);
    UNUSED_VALUE(table_name_len);

    LOG_MSG("SQLProcedures called");
    return SQL_SUCCESS;
}

#ifdef __JETBRAINS_IDE__
# pragma clang diagnostic pop
#endif
