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

#pragma once

#include "ignite/odbc/system/odbc_constants.h"

/**
 * @file odbc.h
 *
 * Functions here are placed to the ignite namespace so there are no
 * collisions with standard ODBC functions when we call driver API
 * functions from other API functions. I.e, when we call SQLAllocEnv
 * from SQLAllocHandle linker can link Driver Manager call here,
 * instead of internal driver call. On other hand if we call
 * ignite::SQLAllocEnv from ignite::SQLAllocHandle we can be sure
 * there are no collisions.
 */

namespace ignite {
SQLRETURN SQLGetInfo(
    SQLHDBC conn, SQLUSMALLINT infoType, SQLPOINTER infoValue, SQLSMALLINT infoValueMax, SQLSMALLINT *length);

SQLRETURN SQLAllocHandle(SQLSMALLINT type, SQLHANDLE parent, SQLHANDLE *result);

SQLRETURN SQLAllocEnv(SQLHENV *env);

SQLRETURN SQLAllocConnect(SQLHENV env, SQLHDBC *conn);

SQLRETURN SQLAllocStmt(SQLHDBC conn, SQLHSTMT *stmt);

SQLRETURN SQLFreeHandle(SQLSMALLINT type, SQLHANDLE handle);

SQLRETURN SQLFreeEnv(SQLHENV env);

SQLRETURN SQLFreeConnect(SQLHDBC conn);

SQLRETURN SQLFreeStmt(SQLHSTMT stmt, SQLUSMALLINT option);

SQLRETURN SQLCloseCursor(SQLHSTMT stmt);

SQLRETURN SQLDriverConnect(SQLHDBC conn, SQLHWND windowHandle, SQLCHAR *inConnectionString,
    SQLSMALLINT inConnectionStringLen, SQLCHAR *outConnectionString, SQLSMALLINT outConnectionStringBufferLen,
    SQLSMALLINT *outConnectionStringLen, SQLUSMALLINT driverCompletion);

SQLRETURN SQLConnect(SQLHDBC conn, SQLCHAR *server_name, SQLSMALLINT server_name_len, SQLCHAR *user_name,
    SQLSMALLINT user_name_len, SQLCHAR *auth, SQLSMALLINT auth_len);

SQLRETURN SQLDisconnect(SQLHDBC conn);

SQLRETURN SQLPrepare(SQLHSTMT stmt, SQLCHAR *query, SQLINTEGER query_len);

SQLRETURN SQLExecute(SQLHSTMT stmt);

SQLRETURN SQLExecDirect(SQLHSTMT stmt, SQLCHAR *query, SQLINTEGER query_len);

SQLRETURN SQLBindCol(SQLHSTMT stmt, SQLUSMALLINT col_num, SQLSMALLINT target_type, SQLPOINTER target_value,
    SQLLEN buffer_length, SQLLEN *str_length_or_indicator);

SQLRETURN SQLFetch(SQLHSTMT stmt);

SQLRETURN SQLFetchScroll(SQLHSTMT stmt, SQLSMALLINT orientation, SQLLEN offset);

SQLRETURN SQLExtendedFetch(
    SQLHSTMT stmt, SQLUSMALLINT orientation, SQLLEN offset, SQLULEN *row_count, SQLUSMALLINT *row_status_array);

SQLRETURN SQLNumResultCols(SQLHSTMT stmt, SQLSMALLINT *column_num);

SQLRETURN SQLTables(SQLHSTMT stmt, SQLCHAR *catalog_name, SQLSMALLINT catalog_name_len, SQLCHAR *schema_name,
    SQLSMALLINT schema_name_len, SQLCHAR *table_name, SQLSMALLINT table_name_len, SQLCHAR *table_type,
    SQLSMALLINT table_type_len);

SQLRETURN SQLColumns(SQLHSTMT stmt, SQLCHAR *catalog_name, SQLSMALLINT catalog_name_len, SQLCHAR *schema_name,
    SQLSMALLINT schema_name_len, SQLCHAR *table_name, SQLSMALLINT table_name_len, SQLCHAR *column_name,
    SQLSMALLINT column_name_len);

SQLRETURN SQLMoreResults(SQLHSTMT stmt);

SQLRETURN SQLBindParameter(SQLHSTMT stmt, SQLUSMALLINT param_idx, SQLSMALLINT io_type, SQLSMALLINT buffer_type,
    SQLSMALLINT param_sql_type, SQLULEN column_size, SQLSMALLINT dec_digits, SQLPOINTER buffer, SQLLEN buffer_len,
    SQLLEN *res_len);

SQLRETURN SQLNativeSql(SQLHDBC conn, SQLCHAR *in_query, SQLINTEGER in_query_len, SQLCHAR *out_query_buffer,
    SQLINTEGER out_query_buffer_len, SQLINTEGER *out_query_len);

SQLRETURN SQLColAttribute(SQLHSTMT stmt, SQLUSMALLINT column_num, SQLUSMALLINT field_id, SQLPOINTER str_attr,
    SQLSMALLINT buffer_len, SQLSMALLINT *str_attr_len, SQLLEN *numeric_attr);

SQLRETURN SQLDescribeCol(SQLHSTMT stmt, SQLUSMALLINT column_num, SQLCHAR *column_name_buf,
    SQLSMALLINT column_name_buf_len, SQLSMALLINT *column_name_len, SQLSMALLINT *data_type, SQLULEN *column_size,
    SQLSMALLINT *decimal_digits, SQLSMALLINT *nullable);

SQLRETURN SQLRowCount(SQLHSTMT stmt, SQLLEN *row_count);

SQLRETURN SQLForeignKeys(SQLHSTMT stmt, SQLCHAR *primary_catalog_name, SQLSMALLINT primary_catalog_name_len,
    SQLCHAR *primary_schema_name, SQLSMALLINT primary_schema_name_len, SQLCHAR *primary_table_name,
    SQLSMALLINT primary_table_name_len, SQLCHAR *foreign_catalog_name, SQLSMALLINT foreign_catalog_name_len,
    SQLCHAR *foreign_schema_name, SQLSMALLINT foreign_schema_name_len, SQLCHAR *foreign_table_name,
    SQLSMALLINT foreign_table_name_len);

SQLRETURN SQLGetStmtAttr(
    SQLHSTMT stmt, SQLINTEGER attr, SQLPOINTER value_buf, SQLINTEGER value_buf_len, SQLINTEGER *value_res_len);

SQLRETURN SQLSetStmtAttr(SQLHSTMT stmt, SQLINTEGER attr, SQLPOINTER value, SQLINTEGER value_len);

SQLRETURN SQLPrimaryKeys(SQLHSTMT stmt, SQLCHAR *catalog_name, SQLSMALLINT catalog_name_len, SQLCHAR *schema_name,
    SQLSMALLINT schema_name_len, SQLCHAR *table_name, SQLSMALLINT table_name_len);

SQLRETURN SQLNumParams(SQLHSTMT stmt, SQLSMALLINT *param_cnt);

SQLRETURN SQLGetDiagField(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT rec_num, SQLSMALLINT diag_id,
    SQLPOINTER buffer, SQLSMALLINT buffer_len, SQLSMALLINT *res_len);

SQLRETURN SQLGetDiagRec(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT rec_num, SQLCHAR *sql_state,
    SQLINTEGER *native_error, SQLCHAR *msg_buffer, SQLSMALLINT msg_buffer_len, SQLSMALLINT *msg_len);

SQLRETURN SQLGetTypeInfo(SQLHSTMT stmt, SQLSMALLINT type);

SQLRETURN SQLEndTran(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT completion_type);

SQLRETURN SQLGetData(SQLHSTMT stmt, SQLUSMALLINT col_num, SQLSMALLINT target_type, SQLPOINTER target_value,
    SQLLEN buffer_length, SQLLEN *str_length_or_indicator);

SQLRETURN SQLSetEnvAttr(SQLHENV env, SQLINTEGER attr, SQLPOINTER value, SQLINTEGER value_len);

SQLRETURN SQLGetEnvAttr(
    SQLHENV env, SQLINTEGER attr, SQLPOINTER value_buf, SQLINTEGER value_buf_len, SQLINTEGER *value_res_len);

SQLRETURN SQLSpecialColumns(SQLHSTMT stmt, SQLUSMALLINT id_type, SQLCHAR *catalog_name, SQLSMALLINT catalog_name_len,
    SQLCHAR *schema_name, SQLSMALLINT schema_name_len, SQLCHAR *table_name, SQLSMALLINT table_name_len,
    SQLUSMALLINT scope, SQLUSMALLINT nullable);

SQLRETURN SQLParamData(SQLHSTMT stmt, SQLPOINTER *value);

SQLRETURN SQLPutData(SQLHSTMT stmt, SQLPOINTER data, SQLLEN str_length_or_indicator);

SQLRETURN SQLDescribeParam(SQLHSTMT stmt, SQLUSMALLINT param_num, SQLSMALLINT *data_type, SQLULEN *param_size,
    SQLSMALLINT *decimal_digits, SQLSMALLINT *nullable);

SQLRETURN SQLError(SQLHENV env, SQLHDBC conn, SQLHSTMT stmt, SQLCHAR *state, SQLINTEGER *error, SQLCHAR *msg_buf,
    SQLSMALLINT msg_buf_len, SQLSMALLINT *msg_res_len);

SQLRETURN SQL_API SQLGetConnectAttr(
    SQLHDBC conn, SQLINTEGER attr, SQLPOINTER value_buf, SQLINTEGER value_buf_len, SQLINTEGER *value_res_len);

SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC conn, SQLINTEGER attr, SQLPOINTER value, SQLINTEGER value_len);
} // namespace ignite
