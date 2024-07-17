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

#include <algorithm>

using namespace ignite;

/**
 * Test setup fixture.
 */
struct many_cursors_test : public odbc_suite {
    void SetUp() override {
        odbc_connect(get_basic_connection_string());
        exec_query("DELETE FROM " + TABLE_NAME_ALL_COLUMNS_SQL);
        odbc_clean_up();
    }
};

TEST_F(many_cursors_test, several_inserts_without_closing) {
    odbc_connect(get_basic_connection_string());

    SQLCHAR request[] = "INSERT INTO TBL_ALL_COLUMNS_SQL(key, int32) VALUES(?, ?)";

    SQLRETURN ret = SQLPrepare(m_statement, request, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    int64_t key = 0;
    ret = SQLBindParameter(m_statement, 1, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &key, 0, nullptr);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    std::int32_t data = 0;
    ret = SQLBindParameter(m_statement, 2, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 0, 0, &data, 0, nullptr);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    for (std::int32_t i = 0; i < 10; ++i) {
        key = i;
        data = i * 10;

        ret = SQLExecute(m_statement);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));
    }
}

TEST_F(many_cursors_test, many_cursors) {
    odbc_connect(get_basic_connection_string());

    for (std::int32_t i = 0; i < 1000; ++i) {
        SQLCHAR req[] = "SELECT 1";

        SQLRETURN ret = SQLExecDirect(m_statement, req, SQL_NTS);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        ret = SQLFreeStmt(m_statement, SQL_CLOSE);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));
    }
}

TEST_F(many_cursors_test, many_cursors_2) {
    odbc_connect(get_basic_connection_string());

    SQLRETURN ret = SQLFreeHandle(SQL_HANDLE_STMT, m_statement);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    for (std::int32_t i = 0; i < 1000; ++i) {
        ret = SQLAllocHandle(SQL_HANDLE_STMT, m_conn, &m_statement);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        SQLCHAR req[] = "SELECT 1";

        ret = SQLExecDirect(m_statement, req, SQL_NTS);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        std::int32_t res = 0;
        SQLLEN resLen = 0;
        ret = SQLBindCol(m_statement, 1, SQL_INTEGER, &res, 0, &resLen);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        ret = SQLFetch(m_statement);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        ASSERT_EQ(res, 1) << "Step " << i;

        ret = SQLFreeHandle(SQL_HANDLE_STMT, m_statement);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        m_statement = nullptr;
    }
}

// TODO: IGNITE-19855 Multiple queries execution is not supported.
#ifdef MUTED
TEST_F(many_cursors_test, many_cursors_two_selects_1) {
    odbc_connect(get_basic_connection_string());

    for (std::int32_t i = 0; i < 1000; ++i) {
        SQLCHAR req[] = "SELECT 1; SELECT 2";

        SQLRETURN ret = SQLExecDirect(m_statement, req, SQL_NTS);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        ret = SQLFreeStmt(m_statement, SQL_CLOSE);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));
    }
}

TEST_F(many_cursors_test, many_cursors_two_selects_2) {
    odbc_connect(get_basic_connection_string());

    for (std::int32_t i = 0; i < 1000; ++i) {
        SQLCHAR req[] = "SELECT 1; SELECT 2;";

        SQLRETURN ret = SQLExecDirect(m_statement, req, SQL_NTS);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        ret = SQLMoreResults(m_statement);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        ret = SQLFreeStmt(m_statement, SQL_CLOSE);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));
    }
}

TEST_F(many_cursors_test, many_cursors_select_insert_1) {
    odbc_connect(get_basic_connection_string());

    for (std::int32_t i = 0; i < 1000; ++i) {
        SQLCHAR req[] = "SELECT 1; INSERT into TBL_ALL_COLUMNS_SQL(key) values(2);";

        SQLRETURN ret = SQLExecDirect(m_statement, req, SQL_NTS);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        ret = SQLFreeStmt(m_statement, SQL_CLOSE);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));
    }
}

TEST_F(many_cursors_test, many_cursors_select_insert_2) {
    odbc_connect(get_basic_connection_string());

    for (std::int32_t i = 0; i < 1000; ++i) {
        SQLCHAR req[] = "SELECT 1; INSERT into TBL_ALL_COLUMNS_SQL(key) values(2);";

        SQLRETURN ret = SQLExecDirect(m_statement, req, SQL_NTS);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        ret = SQLMoreResults(m_statement);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        ret = SQLFreeStmt(m_statement, SQL_CLOSE);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));
    }
}

#endif // MUTED
