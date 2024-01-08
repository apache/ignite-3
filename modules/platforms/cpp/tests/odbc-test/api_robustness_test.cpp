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

#include "ignite/common/config.h"
#include "odbc_suite.h"

#include <gtest/gtest.h>

#include <vector>

// Using NULLs as specified by ODBC
#ifdef __JETBRAINS_IDE__
# pragma ide diagnostic ignored "modernize-use-nullptr"
#endif // __JETBRAINS_IDE__

using namespace ignite;

/**
 * Test suite.
 */
class api_robustness_test : public odbc_suite {
public:
    static void SetUpTestSuite() {
        odbc_connection conn;
        conn.odbc_connect(get_basic_connection_string());

        auto table_avail = conn.wait_for_table(TABLE_NAME_ALL_COLUMNS_SQL, std::chrono::seconds(10));
        if (!table_avail) {
            FAIL() << "Table '" + TABLE_NAME_ALL_COLUMNS_SQL + "' is not available";
        }
    }
};

std::vector<SQLSMALLINT> unsupported_c_types = {SQL_C_INTERVAL_YEAR, SQL_C_INTERVAL_MONTH, SQL_C_INTERVAL_DAY,
    SQL_C_INTERVAL_HOUR, SQL_C_INTERVAL_MINUTE, SQL_C_INTERVAL_SECOND, SQL_C_INTERVAL_YEAR_TO_MONTH,
    SQL_C_INTERVAL_DAY_TO_HOUR, SQL_C_INTERVAL_DAY_TO_MINUTE, SQL_C_INTERVAL_DAY_TO_SECOND,
    SQL_C_INTERVAL_HOUR_TO_MINUTE, SQL_C_INTERVAL_HOUR_TO_SECOND, SQL_C_INTERVAL_MINUTE_TO_SECOND};

std::vector<SQLSMALLINT> unsupported_sql_types = {SQL_WVARCHAR, SQL_WLONGVARCHAR, SQL_REAL, SQL_NUMERIC,
    SQL_INTERVAL_MONTH, SQL_INTERVAL_YEAR, SQL_INTERVAL_YEAR_TO_MONTH, SQL_INTERVAL_DAY, SQL_INTERVAL_HOUR,
    SQL_INTERVAL_MINUTE, SQL_INTERVAL_SECOND, SQL_INTERVAL_DAY_TO_HOUR, SQL_INTERVAL_DAY_TO_MINUTE,
    SQL_INTERVAL_DAY_TO_SECOND, SQL_INTERVAL_HOUR_TO_MINUTE, SQL_INTERVAL_HOUR_TO_SECOND,
    SQL_INTERVAL_MINUTE_TO_SECOND};

TEST_F(api_robustness_test, sql_driver_connect) {
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    prepare_environment();

    auto connect_str = to_sqlchar(get_basic_connection_string());

    SQLCHAR out_str[ODBC_BUFFER_SIZE]{};
    SQLSMALLINT out_str_len{};

    // Normal connect.
    SQLRETURN ret = SQLDriverConnect(m_conn, NULL, connect_str.data(), SQLSMALLINT(connect_str.size()), out_str,
        sizeof(out_str), &out_str_len, SQL_DRIVER_COMPLETE);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    SQLDisconnect(m_conn);

    // Null out string resulting length.
    SQLDriverConnect(m_conn, NULL, connect_str.data(), SQLSMALLINT(connect_str.size()), out_str, sizeof(out_str), 0,
        SQL_DRIVER_COMPLETE);

    SQLDisconnect(m_conn);

    // Null out string buffer length.
    SQLDriverConnect(m_conn, NULL, connect_str.data(), SQLSMALLINT(connect_str.size()), out_str, 0, &out_str_len,
        SQL_DRIVER_COMPLETE);

    SQLDisconnect(m_conn);

    // Null output string.
    SQLDriverConnect(m_conn, NULL, connect_str.data(), SQLSMALLINT(connect_str.size()), 0, sizeof(out_str),
        &out_str_len, SQL_DRIVER_COMPLETE);

    SQLDisconnect(m_conn);

    // Null all.
    SQLDriverConnect(m_conn, NULL, connect_str.data(), SQLSMALLINT(connect_str.size()), 0, 0, 0, SQL_DRIVER_COMPLETE);

    SQLDisconnect(m_conn);
}

TEST_F(api_robustness_test, sql_get_info) {
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    odbc_connect(get_basic_connection_string());

    SQLCHAR buffer[ODBC_BUFFER_SIZE];
    SQLSMALLINT resLen = 0;

    // Everything is ok.
    SQLRETURN ret = SQLGetInfo(m_conn, SQL_DRIVER_NAME, buffer, ODBC_BUFFER_SIZE, &resLen);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    // The Resulting length is null.
    SQLGetInfo(m_conn, SQL_DRIVER_NAME, buffer, ODBC_BUFFER_SIZE, 0);

    // Buffer length is null.
    SQLGetInfo(m_conn, SQL_DRIVER_NAME, buffer, 0, &resLen);

    // Buffer is null.
    SQLGetInfo(m_conn, SQL_DRIVER_NAME, 0, ODBC_BUFFER_SIZE, &resLen);

    // Unknown info.
    SQLGetInfo(m_conn, -1, buffer, ODBC_BUFFER_SIZE, &resLen);

    // All nulls.
    SQLGetInfo(m_conn, SQL_DRIVER_NAME, 0, 0, 0);
}

TEST_F(api_robustness_test, sql_connect_failed_dsn) {
    // Tests that SQLConnect using DSN doesn't fail with link error (especially on linux).
    prepare_environment();

    SQLCHAR dsn_conn_str[] = "DSN=IGNITE_TEST";

    SQLRETURN ret = SQLConnect(m_conn, dsn_conn_str, sizeof(dsn_conn_str), 0, 0, 0, 0);

    EXPECT_FALSE(SQL_SUCCEEDED(ret));
    EXPECT_EQ(get_odbc_error_state(SQL_HANDLE_DBC, m_conn), "IM002");
}

TEST_F(api_robustness_test, sql_prepare) {
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    odbc_connect(get_basic_connection_string());

    SQLCHAR sql[] = "select str from TBL_ALL_COLUMNS_SQL";

    // Everything is ok.
    SQLRETURN ret = SQLPrepare(m_statement, sql, sizeof(sql));

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    SQLCloseCursor(m_statement);

    // Value length is null.
    SQLPrepare(m_statement, sql, 0);

    SQLCloseCursor(m_statement);

    // Value is null.
    SQLPrepare(m_statement, 0, sizeof(sql));

    SQLCloseCursor(m_statement);

    // All nulls.
    SQLPrepare(m_statement, 0, 0);

    SQLCloseCursor(m_statement);
}

TEST_F(api_robustness_test, sql_exec_direct) {
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    odbc_connect(get_basic_connection_string());

    SQLCHAR sql[] = "select str from TBL_ALL_COLUMNS_SQL";

    // Everything is ok.
    SQLRETURN ret = SQLExecDirect(m_statement, sql, sizeof(sql) - 1);
    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    SQLCloseCursor(m_statement);

    // Value length is null.
    SQLExecDirect(m_statement, sql, 0);

    SQLCloseCursor(m_statement);

    // Value is null.
    SQLExecDirect(m_statement, 0, SQL_NTS);

    SQLCloseCursor(m_statement);

    // All nulls.
    SQLExecDirect(m_statement, 0, 0);

    SQLCloseCursor(m_statement);
}

TEST_F(api_robustness_test, sql_tables) {
    // There are no checks because we do not really care about the result of these
    // calls as long as they do not cause segmentation fault.

    odbc_connect(get_basic_connection_string());

    SQLCHAR catalog_name[] = "";
    SQLCHAR schema_name[] = "";
    SQLCHAR table_name[] = "";
    SQLCHAR table_type[] = "";

    // Everything is ok.
    SQLRETURN ret = SQLTables(m_statement, catalog_name, sizeof(catalog_name), schema_name, sizeof(schema_name),
        table_name, sizeof(table_name), table_type, sizeof(table_type));

    UNUSED_VALUE ret;
    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    // Sizes are nulls.
    SQLTables(m_conn, catalog_name, 0, schema_name, 0, table_name, 0, table_type, 0);

    // Values are nulls.
    SQLTables(m_conn, 0, sizeof(catalog_name), 0, sizeof(schema_name), 0, sizeof(table_name), 0, sizeof(table_type));

    // All nulls.
    SQLTables(m_conn, 0, 0, 0, 0, 0, 0, 0, 0);
}

TEST_F(api_robustness_test, sql_columns) {
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    odbc_connect(get_basic_connection_string());

    SQLCHAR catalog_name[] = "";
    SQLCHAR schema_name[] = "";
    SQLCHAR table_name[] = "";
    SQLCHAR columnName[] = "";

    // Everything is ok.
    SQLRETURN ret = SQLColumns(m_statement, catalog_name, sizeof(catalog_name), schema_name, sizeof(schema_name),
        table_name, sizeof(table_name), columnName, sizeof(columnName));

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    // Sizes are nulls.
    SQLColumns(m_conn, catalog_name, 0, schema_name, 0, table_name, 0, columnName, 0);

    // Values are nulls.
    SQLColumns(m_conn, 0, sizeof(catalog_name), 0, sizeof(schema_name), 0, sizeof(table_name), 0, sizeof(columnName));

    // All nulls.
    SQLColumns(m_conn, 0, 0, 0, 0, 0, 0, 0, 0);
}

TEST_F(api_robustness_test, sql_bind_col) {
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    odbc_connect(get_basic_connection_string());

    SQLINTEGER ind1;
    SQLLEN len1 = 0;

    // Everything is ok.
    SQLRETURN ret = SQLBindCol(m_statement, 1, SQL_C_SLONG, &ind1, sizeof(ind1), &len1);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    // Unsupported data types
    for (short c_type : unsupported_c_types) {
        ret = SQLBindCol(m_statement, 1, c_type, &ind1, sizeof(ind1), &len1);
        ASSERT_EQ(ret, SQL_ERROR);
        EXPECT_EQ(get_statement_error_state(), "HY003");
    }

    // Size is negative.
    ret = SQLBindCol(m_statement, 1, SQL_C_SLONG, &ind1, -1, &len1);
    ASSERT_EQ(ret, SQL_ERROR);
    EXPECT_EQ(get_statement_error_state(), "HY090");

    // Size is null.
    SQLBindCol(m_statement, 1, SQL_C_SLONG, &ind1, 0, &len1);

    // Res size is null.
    SQLBindCol(m_statement, 2, SQL_C_SLONG, &ind1, sizeof(ind1), 0);

    // Value is null.
    SQLBindCol(m_statement, 3, SQL_C_SLONG, 0, sizeof(ind1), &len1);

    // All nulls.
    SQLBindCol(m_statement, 4, SQL_C_SLONG, 0, 0, 0);
}

TEST_F(api_robustness_test, sql_bind_parameter) {
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    odbc_connect(get_basic_connection_string());

    SQLINTEGER ind1;
    SQLLEN len1 = 0;

    // Everything is ok.
    SQLRETURN ret = SQLBindParameter(
        m_statement, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 100, 100, &ind1, sizeof(ind1), &len1);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    // Unsupported parameter type: output
    SQLBindParameter(m_statement, 2, SQL_PARAM_OUTPUT, SQL_C_SLONG, SQL_INTEGER, 100, 100, &ind1, sizeof(ind1), &len1);

    EXPECT_EQ(get_statement_error_state(), "HY105");

    // Unsupported parameter type: input/output
    SQLBindParameter(
        m_statement, 2, SQL_PARAM_INPUT_OUTPUT, SQL_C_SLONG, SQL_INTEGER, 100, 100, &ind1, sizeof(ind1), &len1);

    EXPECT_EQ(get_statement_error_state(), "HY105");

    // Unsupported data types
    for (short sql_type : unsupported_sql_types) {
        ret = SQLBindParameter(
            m_statement, 2, SQL_PARAM_INPUT, SQL_C_SLONG, sql_type, 100, 100, &ind1, sizeof(ind1), &len1);

        ASSERT_EQ(ret, SQL_ERROR);
        EXPECT_EQ(get_statement_error_state(), "HYC00");
    }

    // Size is null.
    SQLBindParameter(m_statement, 2, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 100, 100, &ind1, 0, &len1);

    // Res size is null.
    SQLBindParameter(m_statement, 3, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 100, 100, &ind1, sizeof(ind1), 0);

    // Value is null.
    SQLBindParameter(m_statement, 4, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 100, 100, 0, sizeof(ind1), &len1);

    // All nulls.
    SQLBindParameter(m_statement, 5, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 100, 100, 0, 0, 0);
}

TEST_F(api_robustness_test, sql_native_sql) {
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    odbc_connect(get_basic_connection_string());

    SQLCHAR sql[] = "select str from TBL_ALL_COLUMNS_SQL";
    SQLCHAR buffer[ODBC_BUFFER_SIZE];
    SQLINTEGER resLen = 0;

    // Everything is ok.
    SQLRETURN ret = SQLNativeSql(m_conn, sql, sizeof(sql) - 1, buffer, sizeof(buffer), &resLen);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    // Value size is null.
    SQLNativeSql(m_conn, sql, 0, buffer, sizeof(buffer), &resLen);

    // Buffer size is null.
    SQLNativeSql(m_conn, sql, sizeof(sql) - 1, buffer, 0, &resLen);

    // Res size is null.
    SQLNativeSql(m_conn, sql, sizeof(sql) - 1, buffer, sizeof(buffer), 0);

    // Value is null.
    SQLNativeSql(m_conn, sql, 0, buffer, sizeof(buffer), &resLen);

    // Buffer is null.
    SQLNativeSql(m_conn, sql, sizeof(sql) - 1, 0, sizeof(buffer), &resLen);

    // All nulls.
    SQLNativeSql(m_conn, sql, 0, 0, 0, 0);
}

TEST_F(api_robustness_test, sql_col_attribute) {
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    odbc_connect(get_basic_connection_string());

    SQLCHAR sql[] = "select str from TBL_ALL_COLUMNS_SQL";

    SQLRETURN ret = SQLExecDirect(m_statement, sql, sizeof(sql) - 1);
    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    SQLCHAR buffer[ODBC_BUFFER_SIZE];
    SQLSMALLINT resLen = 0;
    SQLLEN numericAttr = 0;

    // Everything is ok. Character attribute.
    ret = SQLColAttribute(m_statement, 1, SQL_COLUMN_TABLE_NAME, buffer, sizeof(buffer), &resLen, &numericAttr);
    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    // Everything is ok. Numeric attribute.
    ret = SQLColAttribute(m_statement, 1, SQL_DESC_COUNT, buffer, sizeof(buffer), &resLen, &numericAttr);
    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    SQLColAttribute(m_statement, 1, SQL_COLUMN_TABLE_NAME, buffer, sizeof(buffer), &resLen, 0);
    SQLColAttribute(m_statement, 1, SQL_COLUMN_TABLE_NAME, buffer, sizeof(buffer), 0, &numericAttr);
    SQLColAttribute(m_statement, 1, SQL_COLUMN_TABLE_NAME, buffer, 0, &resLen, &numericAttr);
    SQLColAttribute(m_statement, 1, SQL_COLUMN_TABLE_NAME, 0, sizeof(buffer), &resLen, &numericAttr);
    SQLColAttribute(m_statement, 1, SQL_COLUMN_TABLE_NAME, 0, 0, 0, 0);

    SQLColAttribute(m_statement, 1, SQL_DESC_COUNT, buffer, sizeof(buffer), &resLen, 0);
    SQLColAttribute(m_statement, 1, SQL_DESC_COUNT, buffer, sizeof(buffer), 0, &numericAttr);
    SQLColAttribute(m_statement, 1, SQL_DESC_COUNT, buffer, 0, &resLen, &numericAttr);
    SQLColAttribute(m_statement, 1, SQL_DESC_COUNT, 0, sizeof(buffer), &resLen, &numericAttr);
    SQLColAttribute(m_statement, 1, SQL_DESC_COUNT, 0, 0, 0, 0);
}

TEST_F(api_robustness_test, sql_describe_col) {
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    odbc_connect(get_basic_connection_string());

    SQLCHAR sql[] = "select str from TBL_ALL_COLUMNS_SQL";

    SQLRETURN ret = SQLExecDirect(m_statement, sql, sizeof(sql) - 1);
    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    SQLCHAR columnName[ODBC_BUFFER_SIZE];
    SQLSMALLINT columnNameLen = 0;
    SQLSMALLINT dataType = 0;
    SQLULEN columnSize = 0;
    SQLSMALLINT decimalDigits = 0;
    SQLSMALLINT nullable = 0;

    // Everything is ok.
    ret = SQLDescribeCol(m_statement, 1, columnName, sizeof(columnName), &columnNameLen, &dataType, &columnSize,
        &decimalDigits, &nullable);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    SQLDescribeCol(
        m_statement, 1, 0, sizeof(columnName), &columnNameLen, &dataType, &columnSize, &decimalDigits, &nullable);
    SQLDescribeCol(m_statement, 1, columnName, 0, &columnNameLen, &dataType, &columnSize, &decimalDigits, &nullable);
    SQLDescribeCol(
        m_statement, 1, columnName, sizeof(columnName), 0, &dataType, &columnSize, &decimalDigits, &nullable);
    SQLDescribeCol(
        m_statement, 1, columnName, sizeof(columnName), &columnNameLen, 0, &columnSize, &decimalDigits, &nullable);
    SQLDescribeCol(
        m_statement, 1, columnName, sizeof(columnName), &columnNameLen, &dataType, 0, &decimalDigits, &nullable);
    SQLDescribeCol(
        m_statement, 1, columnName, sizeof(columnName), &columnNameLen, &dataType, &columnSize, 0, &nullable);
    SQLDescribeCol(
        m_statement, 1, columnName, sizeof(columnName), &columnNameLen, &dataType, &columnSize, &decimalDigits, 0);
    SQLDescribeCol(m_statement, 1, 0, 0, 0, 0, 0, 0, 0);
}

TEST_F(api_robustness_test, sql_row_count) {
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    odbc_connect(get_basic_connection_string());

    SQLCHAR sql[] = "select str from TBL_ALL_COLUMNS_SQL";

    SQLRETURN ret = SQLExecDirect(m_statement, sql, SQL_NTS);
    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    SQLLEN rows = 0;

    // Everything is ok.
    ret = SQLRowCount(m_statement, &rows);
    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    SQLRowCount(m_statement, 0);
}

TEST_F(api_robustness_test, sql_foreign_keys) {
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    odbc_connect(get_basic_connection_string());

    SQLCHAR catalog_name[] = "";
    SQLCHAR schema_name[] = "cache";
    SQLCHAR table_name[] = "TestType";

    // Everything is ok.
    SQLRETURN ret = SQLForeignKeys(m_statement, catalog_name, sizeof(catalog_name), schema_name, sizeof(schema_name),
        table_name, sizeof(table_name), catalog_name, sizeof(catalog_name), schema_name, sizeof(schema_name),
        table_name, sizeof(table_name));

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    SQLCloseCursor(m_statement);

    SQLForeignKeys(m_statement, 0, sizeof(catalog_name), schema_name, sizeof(schema_name), table_name,
        sizeof(table_name), catalog_name, sizeof(catalog_name), schema_name, sizeof(schema_name), table_name,
        sizeof(table_name));

    SQLCloseCursor(m_statement);

    SQLForeignKeys(m_statement, catalog_name, 0, schema_name, sizeof(schema_name), table_name, sizeof(table_name),
        catalog_name, sizeof(catalog_name), schema_name, sizeof(schema_name), table_name, sizeof(table_name));

    SQLCloseCursor(m_statement);

    SQLForeignKeys(m_statement, catalog_name, sizeof(catalog_name), 0, sizeof(schema_name), table_name,
        sizeof(table_name), catalog_name, sizeof(catalog_name), schema_name, sizeof(schema_name), table_name,
        sizeof(table_name));

    SQLCloseCursor(m_statement);

    SQLForeignKeys(m_statement, catalog_name, sizeof(catalog_name), schema_name, 0, table_name, sizeof(table_name),
        catalog_name, sizeof(catalog_name), schema_name, sizeof(schema_name), table_name, sizeof(table_name));

    SQLCloseCursor(m_statement);

    SQLForeignKeys(m_statement, catalog_name, sizeof(catalog_name), schema_name, sizeof(schema_name), 0,
        sizeof(table_name), catalog_name, sizeof(catalog_name), schema_name, sizeof(schema_name), table_name,
        sizeof(table_name));

    SQLCloseCursor(m_statement);

    SQLForeignKeys(m_statement, catalog_name, sizeof(catalog_name), schema_name, sizeof(schema_name), table_name, 0,
        catalog_name, sizeof(catalog_name), schema_name, sizeof(schema_name), table_name, sizeof(table_name));

    SQLCloseCursor(m_statement);

    SQLForeignKeys(m_statement, catalog_name, sizeof(catalog_name), schema_name, sizeof(schema_name), table_name,
        sizeof(table_name), 0, sizeof(catalog_name), schema_name, sizeof(schema_name), table_name, sizeof(table_name));

    SQLCloseCursor(m_statement);

    SQLForeignKeys(m_statement, catalog_name, sizeof(catalog_name), schema_name, sizeof(schema_name), table_name,
        sizeof(table_name), catalog_name, 0, schema_name, sizeof(schema_name), table_name, sizeof(table_name));

    SQLCloseCursor(m_statement);

    SQLForeignKeys(m_statement, catalog_name, sizeof(catalog_name), schema_name, sizeof(schema_name), table_name,
        sizeof(table_name), catalog_name, sizeof(catalog_name), 0, sizeof(schema_name), table_name, sizeof(table_name));

    SQLCloseCursor(m_statement);

    SQLForeignKeys(m_statement, catalog_name, sizeof(catalog_name), schema_name, sizeof(schema_name), table_name,
        sizeof(table_name), catalog_name, sizeof(catalog_name), schema_name, 0, table_name, sizeof(table_name));

    SQLCloseCursor(m_statement);

    SQLForeignKeys(m_statement, catalog_name, sizeof(catalog_name), schema_name, sizeof(schema_name), table_name,
        sizeof(table_name), catalog_name, sizeof(catalog_name), schema_name, sizeof(schema_name), 0,
        sizeof(table_name));

    SQLCloseCursor(m_statement);

    SQLForeignKeys(m_statement, catalog_name, sizeof(catalog_name), schema_name, sizeof(schema_name), table_name,
        sizeof(table_name), catalog_name, sizeof(catalog_name), schema_name, sizeof(schema_name), table_name, 0);

    SQLCloseCursor(m_statement);

    SQLForeignKeys(m_statement, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

    SQLCloseCursor(m_statement);
}

TEST_F(api_robustness_test, sql_get_stmt_attr) {
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    odbc_connect(get_basic_connection_string());

    SQLCHAR buffer[ODBC_BUFFER_SIZE];
    SQLINTEGER resLen = 0;

    // Everything is ok.
    SQLRETURN ret = SQLGetStmtAttr(m_statement, SQL_ATTR_ROW_ARRAY_SIZE, buffer, sizeof(buffer), &resLen);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    SQLGetStmtAttr(m_statement, SQL_ATTR_ROW_ARRAY_SIZE, 0, sizeof(buffer), &resLen);
    SQLGetStmtAttr(m_statement, SQL_ATTR_ROW_ARRAY_SIZE, buffer, 0, &resLen);
    SQLGetStmtAttr(m_statement, SQL_ATTR_ROW_ARRAY_SIZE, buffer, sizeof(buffer), 0);
    SQLGetStmtAttr(m_statement, SQL_ATTR_ROW_ARRAY_SIZE, 0, 0, 0);
}

TEST_F(api_robustness_test, sql_set_stmt_attr) {
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    odbc_connect(get_basic_connection_string());

    SQLULEN val = 1;

    // Everything is ok.
    SQLRETURN ret =
        SQLSetStmtAttr(m_statement, SQL_ATTR_ROW_ARRAY_SIZE, reinterpret_cast<SQLPOINTER>(val), sizeof(val));

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    SQLSetStmtAttr(m_statement, SQL_ATTR_ROW_ARRAY_SIZE, 0, sizeof(val));
    SQLSetStmtAttr(m_statement, SQL_ATTR_ROW_ARRAY_SIZE, reinterpret_cast<SQLPOINTER>(val), 0);
    SQLSetStmtAttr(m_statement, SQL_ATTR_ROW_ARRAY_SIZE, 0, 0);
}

TEST_F(api_robustness_test, sql_primary_keys) {
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    odbc_connect(get_basic_connection_string());

    SQLCHAR catalog_name[] = "";
    SQLCHAR schema_name[] = "cache";
    SQLCHAR table_name[] = "TestType";

    // Everything is ok.
    SQLRETURN ret = SQLPrimaryKeys(m_statement, catalog_name, sizeof(catalog_name), schema_name, sizeof(schema_name),
        table_name, sizeof(table_name));

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    SQLPrimaryKeys(
        m_statement, 0, sizeof(catalog_name), schema_name, sizeof(schema_name), table_name, sizeof(table_name));
    SQLPrimaryKeys(m_statement, catalog_name, 0, schema_name, sizeof(schema_name), table_name, sizeof(table_name));
    SQLPrimaryKeys(
        m_statement, catalog_name, sizeof(catalog_name), 0, sizeof(schema_name), table_name, sizeof(table_name));
    SQLPrimaryKeys(m_statement, catalog_name, sizeof(catalog_name), schema_name, 0, table_name, sizeof(table_name));
    SQLPrimaryKeys(
        m_statement, catalog_name, sizeof(catalog_name), schema_name, sizeof(schema_name), 0, sizeof(table_name));
    SQLPrimaryKeys(m_statement, catalog_name, sizeof(catalog_name), schema_name, sizeof(schema_name), table_name, 0);
    SQLPrimaryKeys(m_statement, 0, 0, 0, 0, 0, 0);
}

TEST_F(api_robustness_test, sql_num_params) {
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    odbc_connect(get_basic_connection_string());

    SQLCHAR sql[] = "select str from TBL_ALL_COLUMNS_SQL";

    // Everything is ok.
    SQLRETURN ret = SQLPrepare(m_statement, sql, sizeof(sql));

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    SQLSMALLINT params;

    // Everything is ok.
    ret = SQLNumParams(m_statement, &params);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    SQLNumParams(m_statement, 0);
}

TEST_F(api_robustness_test, sql_num_params_escaped) {
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    odbc_connect(get_basic_connection_string());

    SQLCHAR sql[] = "SELECT {fn NOW()}";

    // Everything is ok.
    SQLRETURN ret = SQLPrepare(m_statement, sql, sizeof(sql));

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    SQLSMALLINT params;

    // Everything is ok.
    ret = SQLNumParams(m_statement, &params);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    SQLNumParams(m_statement, 0);
}

TEST_F(api_robustness_test, sql_get_diag_field) {
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    odbc_connect(get_basic_connection_string());

    // Should fail.
    SQLRETURN ret = SQLGetTypeInfo(m_statement, SQL_INTERVAL_MONTH);

    ASSERT_EQ(ret, SQL_ERROR);

    SQLCHAR buffer[ODBC_BUFFER_SIZE];
    SQLSMALLINT resLen = 0;

    // Everything is ok
    ret = SQLGetDiagField(SQL_HANDLE_STMT, m_statement, 1, SQL_DIAG_MESSAGE_TEXT, buffer, sizeof(buffer), &resLen);

    ASSERT_EQ(ret, SQL_SUCCESS);

    SQLGetDiagField(SQL_HANDLE_STMT, m_statement, 1, SQL_DIAG_MESSAGE_TEXT, 0, sizeof(buffer), &resLen);
    SQLGetDiagField(SQL_HANDLE_STMT, m_statement, 1, SQL_DIAG_MESSAGE_TEXT, buffer, 0, &resLen);
    SQLGetDiagField(SQL_HANDLE_STMT, m_statement, 1, SQL_DIAG_MESSAGE_TEXT, buffer, sizeof(buffer), 0);
    SQLGetDiagField(SQL_HANDLE_STMT, m_statement, 1, SQL_DIAG_MESSAGE_TEXT, 0, 0, 0);
}

TEST_F(api_robustness_test, sql_get_diag_rec) {
    odbc_connect(get_basic_connection_string());

    SQLCHAR state[ODBC_BUFFER_SIZE];
    SQLINTEGER nativeError = 0;
    SQLCHAR message[ODBC_BUFFER_SIZE];
    SQLSMALLINT messageLen = 0;

    // Generating error.
    SQLRETURN ret = SQLGetTypeInfo(m_statement, SQL_INTERVAL_MONTH);
    ASSERT_EQ(ret, SQL_ERROR);

    // Everything is ok.
    ret = SQLGetDiagRec(SQL_HANDLE_STMT, m_statement, 1, state, &nativeError, message, sizeof(message), &messageLen);
    ASSERT_EQ(ret, SQL_SUCCESS);

    // Should return error.
    ret = SQLGetDiagRec(SQL_HANDLE_STMT, m_statement, 1, state, &nativeError, message, -1, &messageLen);
    ASSERT_EQ(ret, SQL_ERROR);

    // Should return message length.
    ret = SQLGetDiagRec(SQL_HANDLE_STMT, m_statement, 1, state, &nativeError, message, 1, &messageLen);
    ASSERT_EQ(ret, SQL_SUCCESS_WITH_INFO);

    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.
    SQLGetDiagRec(SQL_HANDLE_STMT, m_statement, 1, 0, &nativeError, message, sizeof(message), &messageLen);
    SQLGetDiagRec(SQL_HANDLE_STMT, m_statement, 1, state, 0, message, sizeof(message), &messageLen);
    SQLGetDiagRec(SQL_HANDLE_STMT, m_statement, 1, state, &nativeError, 0, sizeof(message), &messageLen);
    SQLGetDiagRec(SQL_HANDLE_STMT, m_statement, 1, state, &nativeError, message, 0, &messageLen);
    SQLGetDiagRec(SQL_HANDLE_STMT, m_statement, 1, state, &nativeError, message, sizeof(message), 0);
    SQLGetDiagRec(SQL_HANDLE_STMT, m_statement, 1, 0, 0, 0, 0, 0);
}

TEST_F(api_robustness_test, sql_get_env_attr) {
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    odbc_connect(get_basic_connection_string());

    SQLCHAR buffer[ODBC_BUFFER_SIZE];
    SQLINTEGER resLen = 0;

    // Everything is ok.
    SQLRETURN ret = SQLGetEnvAttr(m_env, SQL_ATTR_ODBC_VERSION, buffer, sizeof(buffer), &resLen);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_ENV, m_env);

    SQLGetEnvAttr(m_env, SQL_ATTR_ODBC_VERSION, 0, sizeof(buffer), &resLen);
    SQLGetEnvAttr(m_env, SQL_ATTR_ODBC_VERSION, buffer, 0, &resLen);
    SQLGetEnvAttr(m_env, SQL_ATTR_ODBC_VERSION, buffer, sizeof(buffer), 0);
    SQLGetEnvAttr(m_env, SQL_ATTR_ODBC_VERSION, 0, 0, 0);
}

TEST_F(api_robustness_test, sql_special_columns) {
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    odbc_connect(get_basic_connection_string());

    SQLCHAR catalog_name[] = "";
    SQLCHAR schema_name[] = "cache";
    SQLCHAR table_name[] = "TestType";

    // Everything is ok.
    SQLRETURN ret = SQLSpecialColumns(m_statement, SQL_BEST_ROWID, catalog_name, sizeof(catalog_name), schema_name,
        sizeof(schema_name), table_name, sizeof(table_name), SQL_SCOPE_CURROW, SQL_NO_NULLS);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    SQLCloseCursor(m_statement);

    SQLSpecialColumns(m_statement, SQL_BEST_ROWID, 0, sizeof(catalog_name), schema_name, sizeof(schema_name),
        table_name, sizeof(table_name), SQL_SCOPE_CURROW, SQL_NO_NULLS);

    SQLCloseCursor(m_statement);

    SQLSpecialColumns(m_statement, SQL_BEST_ROWID, catalog_name, 0, schema_name, sizeof(schema_name), table_name,
        sizeof(table_name), SQL_SCOPE_CURROW, SQL_NO_NULLS);

    SQLCloseCursor(m_statement);

    SQLSpecialColumns(m_statement, SQL_BEST_ROWID, catalog_name, sizeof(catalog_name), 0, sizeof(schema_name),
        table_name, sizeof(table_name), SQL_SCOPE_CURROW, SQL_NO_NULLS);

    SQLCloseCursor(m_statement);

    SQLSpecialColumns(m_statement, SQL_BEST_ROWID, catalog_name, sizeof(catalog_name), schema_name, 0, table_name,
        sizeof(table_name), SQL_SCOPE_CURROW, SQL_NO_NULLS);

    SQLCloseCursor(m_statement);

    SQLSpecialColumns(m_statement, SQL_BEST_ROWID, catalog_name, sizeof(catalog_name), schema_name, sizeof(schema_name),
        0, sizeof(table_name), SQL_SCOPE_CURROW, SQL_NO_NULLS);

    SQLCloseCursor(m_statement);

    SQLSpecialColumns(m_statement, SQL_BEST_ROWID, catalog_name, sizeof(catalog_name), schema_name, sizeof(schema_name),
        table_name, 0, SQL_SCOPE_CURROW, SQL_NO_NULLS);

    SQLCloseCursor(m_statement);

    SQLSpecialColumns(m_statement, SQL_BEST_ROWID, 0, 0, 0, 0, 0, 0, SQL_SCOPE_CURROW, SQL_NO_NULLS);

    SQLCloseCursor(m_statement);
}

TEST_F(api_robustness_test, sql_error) {
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    odbc_connect(get_basic_connection_string());

    SQLCHAR state[6] = {0};
    SQLINTEGER nativeCode = 0;
    SQLCHAR message[ODBC_BUFFER_SIZE] = {0};
    SQLSMALLINT messageLen = 0;

    // Everything is ok.
    SQLRETURN ret = SQLError(m_env, 0, 0, state, &nativeCode, message, sizeof(message), &messageLen);

    if (ret != SQL_SUCCESS && ret != SQL_NO_DATA)
        FAIL() << "Unexpected error";

    ret = SQLError(0, m_conn, 0, state, &nativeCode, message, sizeof(message), &messageLen);

    if (ret != SQL_SUCCESS && ret != SQL_NO_DATA)
        FAIL() << "Unexpected error";

    ret = SQLError(0, 0, m_statement, state, &nativeCode, message, sizeof(message), &messageLen);

    if (ret != SQL_SUCCESS && ret != SQL_NO_DATA)
        FAIL() << "Unexpected error";

    SQLError(0, 0, 0, state, &nativeCode, message, sizeof(message), &messageLen);

    SQLError(0, 0, m_statement, 0, &nativeCode, message, sizeof(message), &messageLen);

    SQLError(0, 0, m_statement, state, 0, message, sizeof(message), &messageLen);

    SQLError(0, 0, m_statement, state, &nativeCode, 0, sizeof(message), &messageLen);

    SQLError(0, 0, m_statement, state, &nativeCode, message, 0, &messageLen);

    SQLError(0, 0, m_statement, state, &nativeCode, message, sizeof(message), 0);

    SQLError(0, 0, m_statement, 0, 0, 0, 0, 0);

    SQLError(0, 0, 0, 0, 0, 0, 0, 0);
}

TEST_F(api_robustness_test, sql_diagnostic_records) {
    odbc_connect(get_basic_connection_string());

    SQLHANDLE hnd;

    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_DESC, m_conn, &hnd);
    ASSERT_EQ(ret, SQL_ERROR);
    EXPECT_EQ(get_connection_error_state(), "IM001");

    ret = SQLFreeStmt(m_statement, 4);
    ASSERT_EQ(ret, SQL_ERROR);
    EXPECT_EQ(get_statement_error_state(), "HY092");
}

TEST_F(api_robustness_test, many_fds) {
    enum { FDS_NUM = 2000 };

    std::FILE *fds[FDS_NUM];

    for (auto &fd : fds)
        fd = tmpfile();

    odbc_connect(get_basic_connection_string());

    for (auto fd : fds) {
        if (fd)
            fclose(fd);
    }
}
