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

#include "ignite/odbc/log.h"
#include "ignite/odbc/system/odbc_constants.h"
#include "ignite/odbc/utility.h"

#include "ignite/odbc/config/configuration.h"
#include "ignite/odbc/odbc.h"
#include "ignite/odbc/sql_connection.h"
#include "ignite/odbc/sql_environment.h"
#include "ignite/odbc/sql_statement.h"
#include "ignite/odbc/type_traits.h"

#include <algorithm>

#ifdef __JETBRAINS_IDE__
# pragma clang diagnostic push
# pragma ide diagnostic ignored "readability-non-const-parameter"
#endif

namespace {

#define DBG_STR_CASE(x)                                                                                                \
case x:                                                                                                               \
return #x

const char *attr_id_to_string(uint16_t id) {
    switch (id) {
        DBG_STR_CASE(SQL_DESC_LABEL);
        DBG_STR_CASE(SQL_DESC_BASE_COLUMN_NAME);
        DBG_STR_CASE(SQL_DESC_NAME);
        DBG_STR_CASE(SQL_DESC_TABLE_NAME);
        DBG_STR_CASE(SQL_DESC_BASE_TABLE_NAME);
        DBG_STR_CASE(SQL_DESC_SCHEMA_NAME);
        DBG_STR_CASE(SQL_DESC_CATALOG_NAME);
        DBG_STR_CASE(SQL_DESC_LITERAL_PREFIX);
        DBG_STR_CASE(SQL_DESC_LITERAL_SUFFIX);
        DBG_STR_CASE(SQL_DESC_TYPE_NAME);
        DBG_STR_CASE(SQL_DESC_LOCAL_TYPE_NAME);
        DBG_STR_CASE(SQL_DESC_FIXED_PREC_SCALE);
        DBG_STR_CASE(SQL_DESC_AUTO_UNIQUE_VALUE);
        DBG_STR_CASE(SQL_DESC_CASE_SENSITIVE);
        DBG_STR_CASE(SQL_DESC_CONCISE_TYPE);
        DBG_STR_CASE(SQL_DESC_TYPE);
        DBG_STR_CASE(SQL_DESC_DISPLAY_SIZE);
        DBG_STR_CASE(SQL_DESC_LENGTH);
        DBG_STR_CASE(SQL_DESC_OCTET_LENGTH);
        DBG_STR_CASE(SQL_DESC_NULLABLE);
        DBG_STR_CASE(SQL_DESC_NUM_PREC_RADIX);
        DBG_STR_CASE(SQL_DESC_PRECISION);
        DBG_STR_CASE(SQL_DESC_SCALE);
        DBG_STR_CASE(SQL_DESC_SEARCHABLE);
        DBG_STR_CASE(SQL_DESC_UNNAMED);
        DBG_STR_CASE(SQL_DESC_UNSIGNED);
        DBG_STR_CASE(SQL_DESC_UPDATABLE);
        DBG_STR_CASE(SQL_COLUMN_LENGTH);
        DBG_STR_CASE(SQL_COLUMN_PRECISION);
        DBG_STR_CASE(SQL_COLUMN_SCALE);
        default:
            break;
    }
    return "<< UNKNOWN ID >>";
}

#undef DBG_STR_CASE

} // anonymous namespace

namespace ignite {

/**
 * To support timer_thread we have to use shared_ptr to have possibility to create weak_ptr for closure inside the
 * plan_heartbeat. Because of this ODBC handle for connection is the pointer to the shared_ptr on heap.
 */
using sql_connection_ptr = sql_environment::sql_connection_ptr;

diagnosable *diagnosable_from_handle(SQLSMALLINT handle_type, void *handle) {
    switch (handle_type) {
        case SQL_HANDLE_ENV: {
            auto *env = reinterpret_cast<sql_environment *>(handle);
            return static_cast<diagnosable *>(env);
        }
        case SQL_HANDLE_DBC: {
            auto *conn = reinterpret_cast<sql_connection_ptr*>(handle);
            return static_cast<diagnosable *>(conn->get());
        }
        case SQL_HANDLE_STMT: {
            auto *statement = reinterpret_cast<sql_statement *>(handle);
            return static_cast<diagnosable *>(statement);
        }
        default:
            break;
    }
    return nullptr;
}

SQLRETURN SQLGetInfo(
    SQLHDBC conn, SQLUSMALLINT infoType, SQLPOINTER infoValue, SQLSMALLINT infoValueMax, SQLSMALLINT *length) {
    LOG_MSG("SQLGetInfo called: " << infoType << " (" << connection_info::info_type_to_string(infoType) << "), "
                                  << std::hex << reinterpret_cast<size_t>(infoValue) << ", " << infoValueMax << ", "
                                  << std::hex << reinterpret_cast<size_t>(length));

    auto *connection = reinterpret_cast<sql_connection_ptr *>(conn);

    if (!connection)
        return SQL_INVALID_HANDLE;

    (*connection)->get_info(infoType, infoValue, infoValueMax, length);

    return (*connection)->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLAllocHandle(SQLSMALLINT type, SQLHANDLE parent, SQLHANDLE *result) {
    switch (type) {
        case SQL_HANDLE_ENV:
            return SQLAllocEnv(result);

        case SQL_HANDLE_DBC:
            return SQLAllocConnect(parent, result);

        case SQL_HANDLE_STMT:
            return SQLAllocStmt(parent, result);

        case SQL_HANDLE_DESC: {
            auto *connection = reinterpret_cast<sql_connection_ptr *>(parent);

            if (!connection)
                return SQL_INVALID_HANDLE;

            if (result)
                *result = nullptr;

            (*connection)->get_diagnostic_records().reset();
            (*connection)->add_status_record(sql_state::SIM001_FUNCTION_NOT_SUPPORTED,
                "The HandleType argument was SQL_HANDLE_DESC, and "
                "the driver does not support allocating a descriptor handle");

            return SQL_ERROR;
        }
        default:
            break;
    }

    if (result)
        *result = nullptr;

    return SQL_ERROR;
}

SQLRETURN SQLAllocEnv(SQLHENV *env) {
    LOG_MSG("SQLAllocEnv called");

    *env = reinterpret_cast<SQLHENV>(new sql_environment());

    return SQL_SUCCESS;
}

SQLRETURN SQLAllocConnect(SQLHENV env, SQLHDBC *conn) {
    LOG_MSG("SQLAllocConnect called");

    *conn = SQL_NULL_HDBC;

    auto *environment = reinterpret_cast<sql_environment *>(env);

    if (!environment)
        return SQL_INVALID_HANDLE;

    sql_connection_ptr *connection = environment->create_connection();
    if (!connection)
        return environment->get_diagnostic_records().get_return_code();

    *conn = reinterpret_cast<SQLHDBC>(connection);

    return SQL_SUCCESS;
}

SQLRETURN SQLAllocStmt(SQLHDBC conn, SQLHSTMT *stmt) {
    LOG_MSG("SQLAllocStmt called");

    *stmt = SQL_NULL_HDBC;

    auto *connection = reinterpret_cast<sql_connection_ptr *>(conn);

    if (!connection)
        return SQL_INVALID_HANDLE;

    auto *statement = (*connection)->create_statement();

    *stmt = reinterpret_cast<SQLHSTMT>(statement);

    return (*connection)->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLFreeHandle(SQLSMALLINT type, SQLHANDLE handle) {
    switch (type) {
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

SQLRETURN SQLFreeEnv(SQLHENV env) {
    LOG_MSG("SQLFreeEnv called: " << env);

    auto *environment = reinterpret_cast<sql_environment *>(env);
    if (!environment)
        return SQL_INVALID_HANDLE;

    delete environment;

    return SQL_SUCCESS;
}

SQLRETURN SQLFreeConnect(SQLHDBC conn) {
    LOG_MSG("SQLFreeConnect called");

    auto *connection = reinterpret_cast<sql_connection_ptr *>(conn);
    if (!connection)
        return SQL_INVALID_HANDLE;

    (*connection)->deregister();

    delete connection;

    return SQL_SUCCESS;
}

SQLRETURN SQLFreeStmt(SQLHSTMT stmt, SQLUSMALLINT option) {
    LOG_MSG("SQLFreeStmt called [option=" << option << ']');

    auto *statement = reinterpret_cast<sql_statement *>(stmt);
    if (!statement)
        return SQL_INVALID_HANDLE;

    if (option == SQL_DROP) {
        delete statement;
        return SQL_SUCCESS;
    }

    statement->free_resources(option);

    return statement->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLCloseCursor(SQLHSTMT stmt) {
    LOG_MSG("SQLCloseCursor called");

    auto *statement = reinterpret_cast<sql_statement *>(stmt);

    statement->close();

    return statement->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLDriverConnect(SQLHDBC conn, SQLHWND windowHandle, SQLCHAR *inConnectionString,
    SQLSMALLINT inConnectionStringLen, SQLCHAR *outConnectionString, SQLSMALLINT outConnectionStringBufferLen,
    SQLSMALLINT *outConnectionStringLen, SQLUSMALLINT driverCompletion) {
    UNUSED_VALUE(driverCompletion);

    LOG_MSG("SQLDriverConnect called");
    if (inConnectionString)
        LOG_MSG("Connection String: [" << inConnectionString << "]");

    auto *connection = reinterpret_cast<sql_connection_ptr *>(conn);
    if (!connection)
        return SQL_INVALID_HANDLE;

    std::string connectStr = sql_string_to_string(inConnectionString, inConnectionStringLen);
    (*connection)->establish(connectStr, windowHandle);

    diagnostic_record_storage &diag = (*connection)->get_diagnostic_records();
    if (!diag.is_successful())
        return diag.get_return_code();

    size_t result_len = copy_string_to_buffer(
        connectStr, reinterpret_cast<char *>(outConnectionString), static_cast<size_t>(outConnectionStringBufferLen));

    if (outConnectionStringLen)
        *outConnectionStringLen = static_cast<SQLSMALLINT>(result_len);

    if (outConnectionString)
        LOG_MSG(outConnectionString);

    return diag.get_return_code();
}

SQLRETURN SQLConnect(SQLHDBC conn, SQLCHAR *server_name, SQLSMALLINT server_name_len, SQLCHAR *user_name,
    SQLSMALLINT user_name_len, SQLCHAR *auth, SQLSMALLINT auth_len) {

    LOG_MSG("SQLConnect called\n");

    auto *connection = reinterpret_cast<sql_connection_ptr *>(conn);
    if (!connection)
        return SQL_INVALID_HANDLE;

    std::string dsn = sql_string_to_string(server_name, server_name_len);

    LOG_MSG("DSN: " << dsn);
    // TODO: IGNITE-19210 Add DSN support

    std::string auth_identity = sql_string_to_string(user_name, user_name_len);
    std::string auth_secret = sql_string_to_string(auth, auth_len);

    (*connection)->establish({auth_identity, auth_secret});

    return (*connection)->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLDisconnect(SQLHDBC conn) {
    LOG_MSG("SQLDisconnect called");

    auto *connection = reinterpret_cast<sql_connection_ptr *>(conn);
    if (!connection)
        return SQL_INVALID_HANDLE;

    (*connection)->release();

    return (*connection)->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLPrepare(SQLHSTMT stmt, SQLCHAR *query, SQLINTEGER query_len) {
    LOG_MSG("SQLPrepare called");

    auto *statement = reinterpret_cast<sql_statement *>(stmt);
    if (!statement)
        return SQL_INVALID_HANDLE;

    std::string sql = sql_string_to_string(query, query_len);

    LOG_MSG("SQL: " << sql);

    statement->prepare_sql_query(sql);

    return statement->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLExecute(SQLHSTMT stmt) {
    LOG_MSG("SQLExecute called");

    auto *statement = reinterpret_cast<sql_statement *>(stmt);
    if (!statement)
        return SQL_INVALID_HANDLE;

    statement->execute_sql_query();

    return statement->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLExecDirect(SQLHSTMT stmt, SQLCHAR *query, SQLINTEGER query_len) {
    LOG_MSG("SQLExecDirect called");

    auto *statement = reinterpret_cast<sql_statement *>(stmt);
    if (!statement)
        return SQL_INVALID_HANDLE;

    std::string sql = sql_string_to_string(query, query_len);

    LOG_MSG("SQL: " << sql);

    statement->execute_sql_query(sql);

    return statement->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLBindCol(SQLHSTMT stmt, SQLUSMALLINT col_num, SQLSMALLINT target_type, SQLPOINTER target_value,
    SQLLEN buffer_length, SQLLEN *str_length_or_indicator) {
    LOG_MSG("SQLBindCol called: index=" << col_num << ", type=" << target_type << ", target_value="
                                        << reinterpret_cast<size_t>(target_value) << ", buffer_length=" << buffer_length
                                        << ", lengthInd=" << reinterpret_cast<size_t>(str_length_or_indicator));

    auto *statement = reinterpret_cast<sql_statement *>(stmt);
    if (!statement)
        return SQL_INVALID_HANDLE;

    statement->bind_column(col_num, target_type, target_value, buffer_length, str_length_or_indicator);

    return statement->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLFetch(SQLHSTMT stmt) {
    LOG_MSG("SQLFetch called");

    auto *statement = reinterpret_cast<sql_statement *>(stmt);
    if (!statement)
        return SQL_INVALID_HANDLE;

    statement->fetch_row();

    return statement->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLFetchScroll(SQLHSTMT stmt, SQLSMALLINT orientation, SQLLEN offset) {
    LOG_MSG("SQLFetchScroll called");
    LOG_MSG("Orientation: " << orientation << " Offset: " << offset);

    auto *statement = reinterpret_cast<sql_statement *>(stmt);
    if (!statement)
        return SQL_INVALID_HANDLE;

    statement->fetch_scroll(orientation, offset);

    return statement->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLExtendedFetch(
    SQLHSTMT stmt, SQLUSMALLINT orientation, SQLLEN offset, SQLULEN *row_count, SQLUSMALLINT *row_status_array) {
    LOG_MSG("SQLExtendedFetch called");

    SQLRETURN res = SQLFetchScroll(stmt, SQLSMALLINT(orientation), offset);

    if (res == SQL_SUCCESS) {
        if (row_count)
            *row_count = 1;

        if (row_status_array)
            row_status_array[0] = SQL_ROW_SUCCESS;
    } else if (res == SQL_NO_DATA && row_count)
        *row_count = 0;

    return res;
}

SQLRETURN SQLNumResultCols(SQLHSTMT stmt, SQLSMALLINT *column_num) {
    LOG_MSG("SQLNumResultCols called");

    auto *statement = reinterpret_cast<sql_statement *>(stmt);
    if (!statement)
        return SQL_INVALID_HANDLE;

    int32_t res = statement->get_column_number();

    if (column_num) {
        *column_num = static_cast<SQLSMALLINT>(res);
        LOG_MSG("column_num: " << *column_num);
    }

    return statement->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLTables(SQLHSTMT stmt, SQLCHAR *catalog_name, SQLSMALLINT catalog_name_len, SQLCHAR *schema_name,
    SQLSMALLINT schema_name_len, SQLCHAR *table_name, SQLSMALLINT table_name_len, SQLCHAR *table_type,
    SQLSMALLINT table_type_len) {
    LOG_MSG("SQLTables called");

    auto *statement = reinterpret_cast<sql_statement *>(stmt);
    if (!statement)
        return SQL_INVALID_HANDLE;

    std::string catalog = sql_string_to_string(catalog_name, catalog_name_len);
    std::string schema = sql_string_to_string(schema_name, schema_name_len);
    std::string table = sql_string_to_string(table_name, table_name_len);
    std::string tableTypeStr = sql_string_to_string(table_type, table_type_len);

    LOG_MSG("catalog: " << catalog);
    LOG_MSG("schema: " << schema);
    LOG_MSG("table: " << table);
    LOG_MSG("table_type: " << tableTypeStr);

    statement->execute_get_tables_meta_query(catalog, schema, table, tableTypeStr);

    return statement->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLColumns(SQLHSTMT stmt, SQLCHAR *catalog_name, SQLSMALLINT catalog_name_len, SQLCHAR *schema_name,
    SQLSMALLINT schema_name_len, SQLCHAR *table_name, SQLSMALLINT table_name_len, SQLCHAR *column_name,
    SQLSMALLINT column_name_len) {
    LOG_MSG("SQLColumns called");

    auto *statement = reinterpret_cast<sql_statement *>(stmt);
    if (!statement)
        return SQL_INVALID_HANDLE;

    std::string catalog = sql_string_to_string(catalog_name, catalog_name_len);
    std::string schema = sql_string_to_string(schema_name, schema_name_len);
    std::string table = sql_string_to_string(table_name, table_name_len);
    std::string column = sql_string_to_string(column_name, column_name_len);

    LOG_MSG("catalog: " << catalog);
    LOG_MSG("schema: " << schema);
    LOG_MSG("table: " << table);
    LOG_MSG("column: " << column);

    statement->execute_get_columns_meta_query(schema, table, column);

    return statement->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLMoreResults(SQLHSTMT stmt) {
    LOG_MSG("SQLMoreResults called");

    auto *statement = reinterpret_cast<sql_statement *>(stmt);
    if (!statement)
        return SQL_INVALID_HANDLE;

    statement->more_results();

    return statement->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLBindParameter(SQLHSTMT stmt, SQLUSMALLINT param_idx, SQLSMALLINT io_type, SQLSMALLINT buffer_type,
    SQLSMALLINT param_sql_type, SQLULEN column_size, SQLSMALLINT dec_digits, SQLPOINTER buffer, SQLLEN buffer_len,
    SQLLEN *res_len) {
    LOG_MSG("SQLBindParameter called: " << param_idx << ", " << buffer_type << ", " << param_sql_type);

    auto *statement = reinterpret_cast<sql_statement *>(stmt);
    if (!statement)
        return SQL_INVALID_HANDLE;

    statement->bind_parameter(
        param_idx, io_type, buffer_type, param_sql_type, column_size, dec_digits, buffer, buffer_len, res_len);

    return statement->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLNativeSql(SQLHDBC conn, SQLCHAR *in_query, SQLINTEGER in_query_len, SQLCHAR *out_query_buffer,
    SQLINTEGER out_query_buffer_len, SQLINTEGER *out_query_len) {
    UNUSED_VALUE(conn);

    LOG_MSG("SQLNativeSql called");

    std::string in = sql_string_to_string(in_query, in_query_len);

    copy_string_to_buffer(in, reinterpret_cast<char *>(out_query_buffer), static_cast<size_t>(out_query_buffer_len));

    if (out_query_len)
        *out_query_len = std::min(out_query_buffer_len, static_cast<SQLINTEGER>(in.size()));

    return SQL_SUCCESS;
}

SQLRETURN SQLColAttribute(SQLHSTMT stmt, SQLUSMALLINT column_num, SQLUSMALLINT field_id, SQLPOINTER str_attr,
    SQLSMALLINT buffer_len, SQLSMALLINT *str_attr_len, SQLLEN *numeric_attr) {
    LOG_MSG("SQLColAttribute called: " << field_id << " (" << attr_id_to_string(field_id) << ")");

    auto *statement = reinterpret_cast<sql_statement *>(stmt);
    if (!statement)
        return SQL_INVALID_HANDLE;

    // This is a special case
    if (field_id == SQL_DESC_COUNT) {
        SQLSMALLINT val = 0;

        SQLRETURN res = SQLNumResultCols(stmt, &val);

        if (numeric_attr && res == SQL_SUCCESS)
            *numeric_attr = val;

        return res;
    }

    statement->get_column_attribute(
        column_num, field_id, reinterpret_cast<char *>(str_attr), buffer_len, str_attr_len, numeric_attr);

    return statement->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLDescribeCol(SQLHSTMT stmt, SQLUSMALLINT column_num, SQLCHAR *column_name_buf,
    SQLSMALLINT column_name_buf_len, SQLSMALLINT *column_name_len, SQLSMALLINT *data_type, SQLULEN *column_size,
    SQLSMALLINT *decimal_digits, SQLSMALLINT *nullable) {
    LOG_MSG("SQLDescribeCol called");

    auto *statement = reinterpret_cast<sql_statement *>(stmt);
    if (!statement)
        return SQL_INVALID_HANDLE;

    statement->get_column_attribute(column_num, SQL_DESC_NAME, reinterpret_cast<char *>(column_name_buf),
        column_name_buf_len, column_name_len, nullptr);

    SQLLEN dataTypeRes;
    SQLLEN columnSizeRes;
    SQLLEN decimalDigitsRes;
    SQLLEN nullableRes;

    statement->get_column_attribute(column_num, SQL_DESC_TYPE, nullptr, 0, nullptr, &dataTypeRes);
    statement->get_column_attribute(column_num, SQL_DESC_PRECISION, nullptr, 0, nullptr, &columnSizeRes);
    statement->get_column_attribute(column_num, SQL_DESC_SCALE, nullptr, 0, nullptr, &decimalDigitsRes);
    statement->get_column_attribute(column_num, SQL_DESC_NULLABLE, nullptr, 0, nullptr, &nullableRes);

    LOG_MSG("column_num: " << column_num);
    LOG_MSG("dataTypeRes: " << dataTypeRes);
    LOG_MSG("columnSizeRes: " << columnSizeRes);
    LOG_MSG("decimalDigitsRes: " << decimalDigitsRes);
    LOG_MSG("nullableRes: " << nullableRes);
    LOG_MSG("column_name_buf: " << (column_name_buf ? reinterpret_cast<const char *>(column_name_buf) : "<null>"));
    LOG_MSG("column_name_len: " << (column_name_len ? *column_name_len : -1));

    if (data_type)
        *data_type = static_cast<SQLSMALLINT>(dataTypeRes);

    if (column_size)
        *column_size = static_cast<SQLULEN>(columnSizeRes);

    if (decimal_digits)
        *decimal_digits = static_cast<SQLSMALLINT>(decimalDigitsRes);

    if (nullable)
        *nullable = static_cast<SQLSMALLINT>(nullableRes);

    return statement->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLRowCount(SQLHSTMT stmt, SQLLEN *row_count) {
    LOG_MSG("SQLRowCount called");

    auto *statement = reinterpret_cast<sql_statement *>(stmt);
    if (!statement)
        return SQL_INVALID_HANDLE;

    int64_t res = statement->affected_rows();

    LOG_MSG("Row count: " << res);

    if (row_count)
        *row_count = static_cast<SQLLEN>(res);

    return statement->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLForeignKeys(SQLHSTMT stmt, SQLCHAR *primary_catalog_name, SQLSMALLINT primary_catalog_name_len,
    SQLCHAR *primary_schema_name, SQLSMALLINT primary_schema_name_len, SQLCHAR *primary_table_name,
    SQLSMALLINT primary_table_name_len, SQLCHAR *foreign_catalog_name, SQLSMALLINT foreign_catalog_name_len,
    SQLCHAR *foreign_schema_name, SQLSMALLINT foreign_schema_name_len, SQLCHAR *foreign_table_name,
    SQLSMALLINT foreign_table_name_len) {
    LOG_MSG("SQLForeignKeys called");

    auto *statement = reinterpret_cast<sql_statement *>(stmt);
    if (!statement)
        return SQL_INVALID_HANDLE;

    std::string primary_catalog = sql_string_to_string(primary_catalog_name, primary_catalog_name_len);
    std::string primary_schema = sql_string_to_string(primary_schema_name, primary_schema_name_len);
    std::string primary_table = sql_string_to_string(primary_table_name, primary_table_name_len);
    std::string foreign_catalog = sql_string_to_string(foreign_catalog_name, foreign_catalog_name_len);
    std::string foreign_schema = sql_string_to_string(foreign_schema_name, foreign_schema_name_len);
    std::string foreign_table = sql_string_to_string(foreign_table_name, foreign_table_name_len);

    LOG_MSG("primary_catalog: " << primary_catalog);
    LOG_MSG("primary_schema: " << primary_schema);
    LOG_MSG("primary_table: " << primary_table);
    LOG_MSG("foreign_catalog: " << foreign_catalog);
    LOG_MSG("foreign_schema: " << foreign_schema);
    LOG_MSG("foreign_table: " << foreign_table);

    statement->execute_get_foreign_keys_query(
        primary_catalog, primary_schema, primary_table, foreign_catalog, foreign_schema, foreign_table);

    return statement->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLGetStmtAttr(
    SQLHSTMT stmt, SQLINTEGER attr, SQLPOINTER value_buf, SQLINTEGER value_buf_len, SQLINTEGER *value_res_len) {
    LOG_MSG("SQLGetStmtAttr called");

#ifdef _DEBUG
    LOG_MSG("Attr: " << statement_attr_id_to_string(attr) << " (" << attr << ")");
#endif //_DEBUG

    auto *statement = reinterpret_cast<sql_statement *>(stmt);
    if (!statement)
        return SQL_INVALID_HANDLE;

    statement->get_attribute(attr, value_buf, value_buf_len, value_res_len);

    return statement->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLSetStmtAttr(SQLHSTMT stmt, SQLINTEGER attr, SQLPOINTER value, SQLINTEGER value_len) {
    LOG_MSG("SQLSetStmtAttr called: " << attr);

#ifdef _DEBUG
    LOG_MSG("Attr: " << statement_attr_id_to_string(attr) << " (" << attr << ")");
#endif //_DEBUG

    auto *statement = reinterpret_cast<sql_statement *>(stmt);
    if (!statement)
        return SQL_INVALID_HANDLE;

    statement->set_attribute(attr, value, value_len);

    return statement->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLPrimaryKeys(SQLHSTMT stmt, SQLCHAR *catalog_name, SQLSMALLINT catalog_name_len, SQLCHAR *schema_name,
    SQLSMALLINT schema_name_len, SQLCHAR *table_name, SQLSMALLINT table_name_len) {
    LOG_MSG("SQLPrimaryKeys called");

    auto *statement = reinterpret_cast<sql_statement *>(stmt);
    if (!statement)
        return SQL_INVALID_HANDLE;

    std::string catalog = sql_string_to_string(catalog_name, catalog_name_len);
    std::string schema = sql_string_to_string(schema_name, schema_name_len);
    std::string table = sql_string_to_string(table_name, table_name_len);

    LOG_MSG("catalog: " << catalog);
    LOG_MSG("schema: " << schema);
    LOG_MSG("table: " << table);

    statement->execute_get_primary_keys_query(catalog, schema, table);

    return statement->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLNumParams(SQLHSTMT stmt, SQLSMALLINT *param_cnt) {
    LOG_MSG("SQLNumParams called");

    auto *statement = reinterpret_cast<sql_statement *>(stmt);
    if (!statement)
        return SQL_INVALID_HANDLE;

    if (param_cnt) {
        uint16_t param_num = 0;
        statement->get_parameters_number(param_num);

        *param_cnt = static_cast<SQLSMALLINT>(param_num);
    }

    return statement->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLGetDiagField(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT rec_num, SQLSMALLINT diag_id,
    SQLPOINTER buffer, SQLSMALLINT buffer_len, SQLSMALLINT *res_len) {
    LOG_MSG("SQLGetDiagField called: " << rec_num);

    auto *diag = diagnosable_from_handle(handle_type, handle);

    SQLLEN out_res_len;
    sql_result result = sql_result::AI_NO_DATA;

    if (diag) {
        application_data_buffer out_buffer(odbc_native_type::AI_DEFAULT, buffer, buffer_len, &out_res_len);
        auto field = diagnostic_field_to_internal(diag_id);

        result = diag->get_diagnostic_records().get_field(rec_num, field, out_buffer);
    }

    if (res_len && result == sql_result::AI_SUCCESS)
        *res_len = static_cast<SQLSMALLINT>(out_res_len);

    return sql_result_to_return_code(result);
}

SQLRETURN SQLGetDiagRec(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT rec_num, SQLCHAR *sql_state,
    SQLINTEGER *native_error, SQLCHAR *msg_buffer, SQLSMALLINT msg_buffer_len, SQLSMALLINT *msg_len) {
    LOG_MSG("SQLGetDiagRec called");

    auto *diag = diagnosable_from_handle(handle_type, handle);
    const diagnostic_record_storage *records = diag ? &diag->get_diagnostic_records() : nullptr;

    if (rec_num < 1 || msg_buffer_len < 0)
        return SQL_ERROR;

    if (!records || rec_num > records->get_status_records_number())
        return SQL_NO_DATA;

    const diagnostic_record &record = records->get_status_record(rec_num);

    if (sql_state)
        copy_string_to_buffer(record.get_sql_state(), reinterpret_cast<char *>(sql_state), 6);

    if (native_error)
        *native_error = 0;

    const std::string &errMsg = record.get_message_text();

    if (!msg_buffer || msg_buffer_len < static_cast<SQLSMALLINT>(errMsg.size() + 1)) {
        if (!msg_len)
            return SQL_ERROR;

        copy_string_to_buffer(errMsg, reinterpret_cast<char *>(msg_buffer), static_cast<size_t>(msg_buffer_len));

        *msg_len = static_cast<SQLSMALLINT>(errMsg.size());

        return SQL_SUCCESS_WITH_INFO;
    }

    copy_string_to_buffer(errMsg, reinterpret_cast<char *>(msg_buffer), static_cast<size_t>(msg_buffer_len));

    if (msg_len)
        *msg_len = static_cast<SQLSMALLINT>(errMsg.size());

    return SQL_SUCCESS;
}

SQLRETURN SQLGetTypeInfo(SQLHSTMT stmt, SQLSMALLINT type) {
    LOG_MSG("SQLGetTypeInfo called: [type=" << type << ']');

    auto *statement = reinterpret_cast<sql_statement *>(stmt);
    if (!statement)
        return SQL_INVALID_HANDLE;

    statement->execute_get_type_info_query(static_cast<int16_t>(type));

    return statement->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLEndTran(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT completion_type) {
    LOG_MSG("SQLEndTran called");

    SQLRETURN result;

    switch (handle_type) {
        case SQL_HANDLE_ENV: {
            auto *env = reinterpret_cast<sql_environment *>(handle);
            if (!env)
                return SQL_INVALID_HANDLE;

            if (completion_type == SQL_COMMIT)
                env->transaction_commit();
            else
                env->transaction_rollback();

            result = env->get_diagnostic_records().get_return_code();

            break;
        }

        case SQL_HANDLE_DBC: {
            auto *conn = reinterpret_cast<sql_connection_ptr *>(handle);
            if (!conn)
                return SQL_INVALID_HANDLE;

            if (completion_type == SQL_COMMIT)
                (*conn)->transaction_commit();
            else
                (*conn)->transaction_rollback();

            result = (*conn)->get_diagnostic_records().get_return_code();

            break;
        }

        default: {
            result = SQL_INVALID_HANDLE;

            break;
        }
    }

    return result;
}

SQLRETURN SQLGetData(SQLHSTMT stmt, SQLUSMALLINT col_num, SQLSMALLINT target_type, SQLPOINTER target_value,
    SQLLEN buffer_length, SQLLEN *str_length_or_indicator) {
    LOG_MSG("SQLGetData called");

    auto *statement = reinterpret_cast<sql_statement *>(stmt);
    if (!statement)
        return SQL_INVALID_HANDLE;

    odbc_native_type driver_type = to_driver_type(target_type);
    application_data_buffer data_buffer(driver_type, target_value, buffer_length, str_length_or_indicator);

    statement->get_column_data(col_num, data_buffer);

    return statement->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLSetEnvAttr(SQLHENV env, SQLINTEGER attr, SQLPOINTER value, SQLINTEGER value_len) {
    LOG_MSG("SQLSetEnvAttr called");
    LOG_MSG("Attribute: " << attr << ", Value: " << (size_t) value);

    auto *environment = reinterpret_cast<sql_environment *>(env);
    if (!environment)
        return SQL_INVALID_HANDLE;

    environment->set_attribute(attr, value, value_len);

    return environment->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLGetEnvAttr(
    SQLHENV env, SQLINTEGER attr, SQLPOINTER value_buf, SQLINTEGER value_buf_len, SQLINTEGER *value_res_len) {
    LOG_MSG("SQLGetEnvAttr called");

    auto *environment = reinterpret_cast<sql_environment *>(env);
    if (!environment)
        return SQL_INVALID_HANDLE;

    SQLLEN out_res_len;
    application_data_buffer out_buffer(
        odbc_native_type::AI_SIGNED_LONG, value_buf, static_cast<int32_t>(value_buf_len), &out_res_len);

    environment->get_attribute(attr, out_buffer);

    if (value_res_len)
        *value_res_len = static_cast<SQLSMALLINT>(out_res_len);

    return environment->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLSpecialColumns(SQLHSTMT stmt, SQLUSMALLINT id_type, SQLCHAR *catalog_name, SQLSMALLINT catalog_name_len,
    SQLCHAR *schema_name, SQLSMALLINT schema_name_len, SQLCHAR *table_name, SQLSMALLINT table_name_len,
    SQLUSMALLINT scope, SQLUSMALLINT nullable) {
    LOG_MSG("SQLSpecialColumns called");

    auto *statement = reinterpret_cast<sql_statement *>(stmt);
    if (!statement)
        return SQL_INVALID_HANDLE;

    std::string catalog = sql_string_to_string(catalog_name, catalog_name_len);
    std::string schema = sql_string_to_string(schema_name, schema_name_len);
    std::string table = sql_string_to_string(table_name, table_name_len);

    LOG_MSG("catalog: " << catalog);
    LOG_MSG("schema: " << schema);
    LOG_MSG("table: " << table);

    statement->execute_special_columns_query(id_type, catalog, schema, table, scope, nullable);

    return statement->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLParamData(SQLHSTMT stmt, SQLPOINTER *value) {
    LOG_MSG("SQLParamData called");

    auto *statement = reinterpret_cast<sql_statement *>(stmt);
    if (!statement)
        return SQL_INVALID_HANDLE;

    statement->select_param(value);

    return statement->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLPutData(SQLHSTMT stmt, SQLPOINTER data, SQLLEN str_length_or_indicator) {
    LOG_MSG("SQLPutData called");

    auto *statement = reinterpret_cast<sql_statement *>(stmt);
    if (!statement)
        return SQL_INVALID_HANDLE;

    statement->put_data(data, str_length_or_indicator);

    return statement->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLDescribeParam(SQLHSTMT stmt, SQLUSMALLINT param_num, SQLSMALLINT *data_type, SQLULEN *param_size,
    SQLSMALLINT *decimal_digits, SQLSMALLINT *nullable) {
    LOG_MSG("SQLDescribeParam called");

    auto *statement = reinterpret_cast<sql_statement *>(stmt);
    if (!statement)
        return SQL_INVALID_HANDLE;

    statement->describe_param(param_num, data_type, param_size, decimal_digits, nullable);

    return statement->get_diagnostic_records().get_return_code();
}

SQLRETURN SQLError(SQLHENV env, SQLHDBC conn, SQLHSTMT stmt, SQLCHAR *state, SQLINTEGER *error, SQLCHAR *msg_buf,
    SQLSMALLINT msg_buf_len, SQLSMALLINT *msg_res_len) {
    LOG_MSG("SQLError called");

    diagnosable *diag = nullptr;

    if (env != nullptr)
        diag = diagnosable_from_handle(SQL_HANDLE_ENV, env);
    else if (conn != nullptr)
        diag = diagnosable_from_handle(SQL_HANDLE_DBC, conn);
    else if (stmt != nullptr)
        diag = diagnosable_from_handle(SQL_HANDLE_STMT, stmt);
    else
        return SQL_INVALID_HANDLE;

    diagnostic_record_storage &records = diag->get_diagnostic_records();

    int32_t rec_num = records.get_last_non_retrieved();

    if (rec_num < 1 || rec_num > records.get_status_records_number())
        return SQL_NO_DATA;

    diagnostic_record &record = records.get_status_record(rec_num);

    record.mark_retrieved();

    if (state)
        copy_string_to_buffer(record.get_sql_state(), reinterpret_cast<char *>(state), 6);

    if (error)
        *error = 0;

    SQLLEN out_res_len;
    application_data_buffer out_buffer(odbc_native_type::AI_CHAR, msg_buf, msg_buf_len, &out_res_len);

    out_buffer.put_string(record.get_message_text());

    if (msg_res_len)
        *msg_res_len = static_cast<SQLSMALLINT>(out_res_len);

    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLGetConnectAttr(
    SQLHDBC conn, SQLINTEGER attr, SQLPOINTER value_buf, SQLINTEGER value_buf_len, SQLINTEGER *value_res_len) {
    LOG_MSG("SQLGetConnectAttr called");

    auto *connection = reinterpret_cast<sql_connection_ptr *>(conn);
    if (!connection)
        return SQL_INVALID_HANDLE;

    (*connection)->get_attribute(attr, value_buf, value_buf_len, value_res_len);

    return (*connection)->get_diagnostic_records().get_return_code();
}

SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC conn, SQLINTEGER attr, SQLPOINTER value, SQLINTEGER value_len) {
    LOG_MSG("SQLSetConnectAttr called(" << attr << ", " << value << ")");

    auto *connection = reinterpret_cast<sql_connection_ptr *>(conn);
    if (!connection)
        return SQL_INVALID_HANDLE;

    (*connection)->set_attribute(attr, value, value_len);

    return (*connection)->get_diagnostic_records().get_return_code();
}

} // namespace ignite

#ifdef __JETBRAINS_IDE__
# pragma clang diagnostic pop
#endif
