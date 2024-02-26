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

#include "odbc_suite.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

using namespace ignite;

/**
 * Test setup fixture.
 */
struct timeout_test : public odbc_suite {
    void SetUp() override {
        odbc_connect(get_basic_connection_string());
        exec_query("DELETE FROM " + TABLE_NAME_ALL_COLUMNS_SQL);
        odbc_clean_up();
    }
};

TEST_F(timeout_test, login_timeout) {
    prepare_environment();

    SQLRETURN ret = SQLSetConnectAttr(m_conn, SQL_ATTR_LOGIN_TIMEOUT, reinterpret_cast<SQLPOINTER>(1), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    auto connect_str = to_sqlchar(get_basic_connection_string());

    SQLCHAR out_str[ODBC_BUFFER_SIZE];
    SQLSMALLINT out_str_len;

    // Connecting to ODBC server.
    ret = SQLDriverConnect(m_conn, nullptr, connect_str.data(), static_cast<SQLSMALLINT>(connect_str.size()), out_str,
        sizeof(out_str), &out_str_len, SQL_DRIVER_COMPLETE);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_DBC, m_conn));
}

TEST_F(timeout_test, login_timeout_fail) {
    prepare_environment();

    SQLRETURN ret = SQLSetConnectAttr(m_conn, SQL_ATTR_LOGIN_TIMEOUT, reinterpret_cast<SQLPOINTER>(5), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    auto connect_str = to_sqlchar("driver={" + DRIVER_NAME + "};address=127.0.0.1:9999");

    SQLCHAR out_str[ODBC_BUFFER_SIZE];
    SQLSMALLINT out_str_len;

    // Connecting to ODBC server.
    ret = SQLDriverConnect(m_conn, nullptr, connect_str.data(), static_cast<SQLSMALLINT>(connect_str.size()), out_str,
        sizeof(out_str), &out_str_len, SQL_DRIVER_COMPLETE);

    if (SQL_SUCCEEDED(ret))
        FAIL() << ("Should timeout");
}

TEST_F(timeout_test, connection_timeout_query) {
    odbc_connect(get_basic_connection_string());

    SQLRETURN ret = SQLSetConnectAttr(m_conn, SQL_ATTR_CONNECTION_TIMEOUT, reinterpret_cast<SQLPOINTER>(10), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    insert_test_strings(10);
}

TEST_F(timeout_test, query_timeout_query) {
    odbc_connect(get_basic_connection_string());

    SQLRETURN ret = SQLSetStmtAttr(m_statement, SQL_ATTR_QUERY_TIMEOUT, reinterpret_cast<SQLPOINTER>(5), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    insert_test_strings(10);
}

TEST_F(timeout_test, query_and_connection_timeout_query) {
    odbc_connect(get_basic_connection_string());

    SQLRETURN ret = SQLSetStmtAttr(m_statement, SQL_ATTR_QUERY_TIMEOUT, reinterpret_cast<SQLPOINTER>(5), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    ret = SQLSetConnectAttr(m_conn, SQL_ATTR_CONNECTION_TIMEOUT, reinterpret_cast<SQLPOINTER>(3), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    insert_test_strings(10);
}

TEST_F(timeout_test, connection_timeout_batch) {
    odbc_connect(get_basic_connection_string());

    SQLRETURN ret = SQLSetConnectAttr(m_conn, SQL_ATTR_CONNECTION_TIMEOUT, reinterpret_cast<SQLPOINTER>(5), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    insert_test_batch(11, 20, 9);
}

TEST_F(timeout_test, connection_timeout_both) {
    odbc_connect(get_basic_connection_string());

    SQLRETURN ret = SQLSetConnectAttr(m_conn, SQL_ATTR_CONNECTION_TIMEOUT, reinterpret_cast<SQLPOINTER>(5), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    insert_test_strings(10);
    insert_test_batch(11, 20, 9);
}

TEST_F(timeout_test, query_timeout_batch) {
    odbc_connect(get_basic_connection_string());

    SQLRETURN ret = SQLSetStmtAttr(m_statement, SQL_ATTR_QUERY_TIMEOUT, reinterpret_cast<SQLPOINTER>(5), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    insert_test_batch(11, 20, 9);
}

TEST_F(timeout_test, query_timeout_both) {
    odbc_connect(get_basic_connection_string());

    SQLRETURN ret = SQLSetStmtAttr(m_statement, SQL_ATTR_QUERY_TIMEOUT, reinterpret_cast<SQLPOINTER>(5), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    insert_test_strings(10);
    insert_test_batch(11, 20, 9);
}

TEST_F(timeout_test, query_and_connection_timeout_batch) {
    odbc_connect(get_basic_connection_string());

    SQLRETURN ret = SQLSetStmtAttr(m_statement, SQL_ATTR_QUERY_TIMEOUT, reinterpret_cast<SQLPOINTER>(5), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    ret = SQLSetConnectAttr(m_conn, SQL_ATTR_CONNECTION_TIMEOUT, reinterpret_cast<SQLPOINTER>(3), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    insert_test_batch(11, 20, 9);
}

TEST_F(timeout_test, query_and_connection_timeout_both) {
    odbc_connect(get_basic_connection_string());

    SQLRETURN ret = SQLSetStmtAttr(m_statement, SQL_ATTR_QUERY_TIMEOUT, reinterpret_cast<SQLPOINTER>(5), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    ret = SQLSetConnectAttr(m_conn, SQL_ATTR_CONNECTION_TIMEOUT, reinterpret_cast<SQLPOINTER>(3), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    insert_test_strings(10);
    insert_test_batch(11, 20, 9);
}
