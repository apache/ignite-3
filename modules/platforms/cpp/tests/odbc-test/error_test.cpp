/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "odbc_suite.h"

#include <gtest/gtest.h>

using namespace ignite;

/**
 * Test setup fixture.
 */
struct error_test : public odbc_suite {
public:
    static void SetUpTestSuite() {
        odbc_connection conn;
        conn.odbc_connect(get_basic_connection_string());

        auto table_avail = conn.wait_for_table(TABLE_NAME_ALL_COLUMNS_SQL, std::chrono::seconds(10));
        if (!table_avail) {
            FAIL() << "Table '" + TABLE_NAME_ALL_COLUMNS_SQL + "' is not available";
        }

        SQLRETURN ret = conn.exec_query("DELETE FROM " + TABLE_NAME_ALL_COLUMNS_SQL);
        if (!SQL_SUCCEEDED(ret)) {
            FAIL() << conn.get_statement_error_message();
        }
    }

    void SetUp() override {
        odbc_connect(get_basic_connection_string());
        exec_query("DELETE FROM " + TABLE_NAME_ALL_COLUMNS_SQL);
        odbc_clean_up();
    }
};

TEST_F(error_test, test_connect_fail)
{
    // Allocate an environment handle
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &m_env);

    ASSERT_TRUE(m_env != SQL_NULL_HANDLE);

    // We want ODBC 3 support
    SQLSetEnvAttr(m_env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(SQL_OV_ODBC3), 0);

    // Allocate a connection handle
    SQLAllocHandle(SQL_HANDLE_DBC, m_env, &m_conn);

    ASSERT_TRUE(m_conn != SQL_NULL_HANDLE);

    // Connect string
    auto connect_str = to_sqlchar("driver={" + DRIVER_NAME + "};ADDRESS=127.0.0.1:1111");

    SQLCHAR out_str[ODBC_BUFFER_SIZE];
    SQLSMALLINT out_str_len;

    // Connecting to ODBC server.
    SQLRETURN ret = SQLDriverConnect(m_conn, NULL, connect_str.data(), SQLSMALLINT(connect_str.size()),
        out_str, sizeof(out_str), &out_str_len, SQL_DRIVER_COMPLETE);

    ASSERT_EQ(ret, SQL_ERROR);
    EXPECT_EQ(get_odbc_error_state(SQL_HANDLE_DBC, m_conn), "08001");
}

TEST_F(error_test, test_duplicate_key)
{
    odbc_connect(get_basic_connection_string());

    SQLCHAR insert_req[] = "INSERT INTO tbl_all_columns_sql(key, str) VALUES(1, 'some')";

    SQLRETURN ret;

    ret = SQLExecDirect(m_statement, insert_req, SQL_NTS);
    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    ret = SQLExecDirect(m_statement, insert_req, SQL_NTS);

    ASSERT_EQ(ret, SQL_ERROR);
    // TODO: IGNITE-19944 Propagate SQL errors from engine to driver
    EXPECT_EQ(get_odbc_error_state(SQL_HANDLE_STMT, m_statement), "HY000");
}

TEST_F(error_test, test_update_key)
{
    odbc_connect(get_basic_connection_string());

    SQLCHAR insert_req[] = "INSERT INTO tbl_all_columns_sql(key, str) VALUES(1, 'some')";

    SQLRETURN ret;

    ret = SQLExecDirect(m_statement, insert_req, SQL_NTS);
    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    SQLCHAR update_req[] = "UPDATE TestType SET _key=2 WHERE _key=1";

    ret = SQLExecDirect(m_statement, update_req, SQL_NTS);

    ASSERT_EQ(ret, SQL_ERROR);
    // TODO: IGNITE-19944 Propagate SQL errors from engine to driver
    EXPECT_EQ(get_odbc_error_state(SQL_HANDLE_STMT, m_statement), "HY000");
}

TEST_F(error_test, test_table_not_found)
{
    odbc_connect(get_basic_connection_string());

    SQLCHAR req[] = "DROP TABLE Nonexisting";

    SQLRETURN ret;

    ret = SQLExecDirect(m_statement, req, SQL_NTS);

    ASSERT_EQ(ret, SQL_ERROR);
    // TODO: IGNITE-19944 Propagate SQL errors from engine to driver
    EXPECT_EQ(get_odbc_error_state(SQL_HANDLE_STMT, m_statement), "HY000");
}

TEST_F(error_test, test_index_not_found)
{
    odbc_connect(get_basic_connection_string());

    SQLCHAR req[] = "DROP INDEX Nonexisting";

    SQLRETURN ret;

    ret = SQLExecDirect(m_statement, req, SQL_NTS);

    ASSERT_EQ(ret, SQL_ERROR);
    // TODO: IGNITE-19944 Propagate SQL errors from engine to driver
    EXPECT_EQ(get_odbc_error_state(SQL_HANDLE_STMT, m_statement), "HY000");
}

TEST_F(error_test, test_syntax_error)
{
    odbc_connect(get_basic_connection_string());

    SQLCHAR req[] = "INSERT INTO tbl_all_columns_sql(key, non_existing) VALUES(1, 'some')";

    SQLRETURN ret;

    ret = SQLExecDirect(m_statement, req, SQL_NTS);

    ASSERT_EQ(ret, SQL_ERROR);
    // TODO: IGNITE-19944 Propagate SQL errors from engine to driver
    EXPECT_EQ(get_odbc_error_state(SQL_HANDLE_STMT, m_statement), "HY000");
}
