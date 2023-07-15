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

using namespace ignite;

/**
 * Test setup fixture.
 */
struct transaction_test : public odbc_suite {
    void SetUp() override {
        odbc_connect(get_basic_connection_string());
        exec_query("DELETE FROM " + TABLE_NAME_ALL_COLUMNS_SQL);
        odbc_clean_up();
    }

    /**
     * Insert test string value in cache and make all the necessary checks.
     *
     * @param key Key.
     * @param value Value.
     */
    void insert_test_value(std::int64_t key, const std::string& value)
    {
        insert_test_value(m_statement, key, value);
    }

    /**
     * Insert test string value in cache and make all the necessary checks.
     *
     * @param statement Statement.
     * @param key Key.
     * @param value Value.
     */
    static void insert_test_value(SQLHSTMT statement, std::int64_t key, const std::string& value)
    {
        SQLCHAR insert_req[] = "INSERT INTO TBL_ALL_COLUMNS_SQL(key, str) VALUES(?, ?)";

        SQLRETURN ret = SQLPrepare(statement, insert_req, SQL_NTS);

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);

        char str_field[1024] = { 0 };
        SQLLEN str_field_len = 0;

        ret = SQLBindParameter(statement, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT, 0, 0, &key, 0, nullptr);

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);

        ret = SQLBindParameter(statement, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, sizeof(str_field),
            sizeof(str_field), &str_field, sizeof(str_field), &str_field_len);

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);

        strncpy(str_field, value.c_str(), sizeof(str_field) - 1);
        str_field_len = SQL_NTS;

        ret = SQLExecute(statement);

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);

        SQLLEN affected = 0;
        ret = SQLRowCount(statement, &affected);

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);

        EXPECT_EQ(affected, 1);

        ret = SQLMoreResults(statement);

        if (ret != SQL_NO_DATA)
            ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);

        reset_statement(statement);
    }

    /**
     * Update test string value in cache and make all the necessary checks.
     *
     * @param key Key.
     * @param value Value.
     */
    void update_test_value(std::int64_t key, const std::string& value)
    {
        update_test_value(m_statement, key, value);
    }

    /**
     * Update test string value in cache and make all the necessary checks.
     *
     * @param statement Statement.
     * @param key Key.
     * @param value Value.
     */
    static void update_test_value(SQLHSTMT statement, std::int64_t key, const std::string& value)
    {
        SQLCHAR update_req[] = "UPDATE TBL_ALL_COLUMNS_SQL SET str=? WHERE key=?";

        SQLRETURN ret = SQLPrepare(statement, update_req, SQL_NTS);

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);

        char str_field[1024] = { 0 };
        SQLLEN str_field_len = 0;

        ret = SQLBindParameter(statement, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, sizeof(str_field),
            sizeof(str_field), &str_field, sizeof(str_field), &str_field_len);

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);

        ret = SQLBindParameter(statement, 2, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT, 0, 0, &key, 0, nullptr);

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);

        strncpy(str_field, value.c_str(), sizeof(str_field) - 1);
        str_field_len = SQL_NTS;

        ret = SQLExecute(statement);

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);

        SQLLEN affected = 0;
        ret = SQLRowCount(statement, &affected);

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);

        EXPECT_EQ(affected, 1);

        ret = SQLMoreResults(statement);

        if (ret != SQL_NO_DATA)
            ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);

        reset_statement(statement);
    }

    /**
     * Delete test string value.
     *
     * @param key Key.
     */
    void delete_test_value(std::int64_t key)
    {
        delete_test_value(m_statement, key);
    }

    /**
     * Delete test string value.
     *
     * @param statement Statement.
     * @param key Key.
     */
    static void delete_test_value(SQLHSTMT statement, std::int64_t key)
    {
        SQLCHAR delete_req[] = "DELETE FROM TBL_ALL_COLUMNS_SQL WHERE key=?";

        SQLRETURN ret = SQLPrepare(statement, delete_req, SQL_NTS);

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);

        ret = SQLBindParameter(statement, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT, 0, 0, &key, 0, nullptr);

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);

        ret = SQLExecute(statement);

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);

        SQLLEN affected = 0;
        ret = SQLRowCount(statement, &affected);

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);

        EXPECT_EQ(affected, 1);

        ret = SQLMoreResults(statement);

        if (ret != SQL_NO_DATA)
            ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);

        reset_statement(statement);
    }
    /**
     * Selects and checks the value.
     *
     * @param key Key.
     * @param expect Expected value.
     */
    void check_test_value(std::int64_t key, const std::string& expect)
    {
        check_test_value(m_statement, key, expect);
    }

    /**
     * Selects and checks the value.
     *
     * @param statement Statement.
     * @param key Key.
     * @param expect Expected value.
     */
    static void check_test_value(SQLHSTMT statement, std::int64_t key, const std::string& expect)
    {
        // Just selecting everything to make sure everything is OK
        SQLCHAR selectReq[] = "SELECT str FROM TBL_ALL_COLUMNS_SQL WHERE key = ?";

        char str_field[1024] = { 0 };
        SQLLEN str_field_len = 0;

        SQLRETURN ret = SQLBindCol(statement, 1, SQL_C_CHAR, &str_field, sizeof(str_field), &str_field_len);

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);

        ret = SQLBindParameter(statement, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT, 0, 0, &key, 0, nullptr);

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);

        ret = SQLExecDirect(statement, selectReq, sizeof(selectReq));

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);

        ret = SQLFetch(statement);

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);

        EXPECT_EQ(std::string(str_field, str_field_len), expect);

        ret = SQLFetch(statement);

        EXPECT_EQ(ret, SQL_NO_DATA);

        ret = SQLMoreResults(statement);

        if (ret != SQL_NO_DATA)
            ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);

        reset_statement(statement);
    }

    /**
     * Selects and checks that value is absent.
     *
     * @param key Key.
     */
    void check_no_test_value(std::int64_t key)
    {
        check_no_test_value(m_statement, key);
    }

    /**
     * Selects and checks that value is absent.
     * TODO: replace it with check_test_value
     *
     * @param statement Statement.
     * @param key Key.
     */
    static void check_no_test_value(SQLHSTMT statement, std::int64_t key)
    {
        // Just selecting everything to make sure everything is OK
        SQLCHAR selectReq[] = "SELECT str FROM TBL_ALL_COLUMNS_SQL WHERE key = ?";

        char str_field[1024] = { 0 };
        SQLLEN str_field_len = 0;

        SQLRETURN ret = SQLBindCol(statement, 1, SQL_C_CHAR, &str_field, sizeof(str_field), &str_field_len);

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);

        ret = SQLBindParameter(statement, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT, 0, 0, &key, 0, nullptr);

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);

        ret = SQLExecDirect(statement, selectReq, sizeof(selectReq));

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);

        ret = SQLFetch(statement);

        EXPECT_EQ(ret, SQL_NO_DATA);

        if (ret != SQL_NO_DATA)
            ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);

        ret = SQLMoreResults(statement);

        EXPECT_EQ(ret, SQL_NO_DATA);

        if (ret != SQL_NO_DATA)
            ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);

        reset_statement(statement);
    }

    /**
     * Reset statement state.
     */
    void reset_statement()
    {
        reset_statement(m_statement);
    }

    /**
     * Reset statement state.
     *
     * @param statement Statement.
     */
    static void reset_statement(SQLHSTMT statement)
    {
        SQLRETURN ret = SQLFreeStmt(statement, SQL_RESET_PARAMS);

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);

        ret = SQLFreeStmt(statement, SQL_UNBIND);

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, statement);
    }
};

TEST_F(transaction_test, transaction_connection_commit)
{
    odbc_connect(get_basic_connection_string());

    SQLRETURN ret = SQLSetConnectAttr(m_conn, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    insert_test_value(42, "Some");

    check_test_value(42, "Some");

    ret = SQLEndTran(SQL_HANDLE_DBC, m_conn, SQL_COMMIT);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    check_test_value(42, "Some");
}

TEST_F(transaction_test, transaction_connection_rollback_insert)
{
    odbc_connect(get_basic_connection_string());

    SQLRETURN ret = SQLSetConnectAttr(m_conn, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    insert_test_value(42, "Some");

    check_test_value(42, "Some");

    ret = SQLEndTran(SQL_HANDLE_DBC, m_conn, SQL_ROLLBACK);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    check_no_test_value(42);
}

TEST_F(transaction_test, transaction_connection_rollback_update_1)
{
    odbc_connect(get_basic_connection_string());

    insert_test_value(42, "Some");

    check_test_value(42, "Some");

    SQLRETURN ret = SQLSetConnectAttr(m_conn, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    update_test_value(42, "Other");

    check_test_value(42, "Other");

    ret = SQLEndTran(SQL_HANDLE_DBC, m_conn, SQL_ROLLBACK);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    check_test_value(42, "Some");
}

TEST_F(transaction_test, transaction_connection_rollback_update_2)
{
    odbc_connect(get_basic_connection_string());

    SQLRETURN ret = SQLSetConnectAttr(m_conn, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    insert_test_value(42, "Some");

    check_test_value(42, "Some");

    ret = SQLEndTran(SQL_HANDLE_DBC, m_conn, SQL_COMMIT);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    check_test_value(42, "Some");

    update_test_value(42, "Other");

    check_test_value(42, "Other");

    ret = SQLEndTran(SQL_HANDLE_DBC, m_conn, SQL_ROLLBACK);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    check_test_value(42, "Some");
}

TEST_F(transaction_test, transaction_connection_rollback_delete_1)
{
    odbc_connect(get_basic_connection_string());

    insert_test_value(42, "Some");

    check_test_value(42, "Some");

    SQLRETURN ret = SQLSetConnectAttr(m_conn, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    delete_test_value(42);

    check_no_test_value(42);

    ret = SQLEndTran(SQL_HANDLE_DBC, m_conn, SQL_ROLLBACK);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    check_test_value(42, "Some");
}

TEST_F(transaction_test, transaction_connection_rollback_delete_2)
{
    odbc_connect(get_basic_connection_string());

    SQLRETURN ret = SQLSetConnectAttr(m_conn, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    insert_test_value(42, "Some");

    check_test_value(42, "Some");

    ret = SQLEndTran(SQL_HANDLE_DBC, m_conn, SQL_COMMIT);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    check_test_value(42, "Some");

    delete_test_value(42);

    check_no_test_value(42);

    ret = SQLEndTran(SQL_HANDLE_DBC, m_conn, SQL_ROLLBACK);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    check_test_value(42, "Some");
}

TEST_F(transaction_test, transaction_environment_commit)
{
    odbc_connect(get_basic_connection_string());

    SQLRETURN ret = SQLSetConnectAttr(m_conn, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    insert_test_value(42, "Some");

    check_test_value(42, "Some");

    ret = SQLEndTran(SQL_HANDLE_DBC, m_conn, SQL_COMMIT);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_ENV, m_env);

    check_test_value(42, "Some");
}

TEST_F(transaction_test, transaction_environment_rollback_insert)
{
    odbc_connect(get_basic_connection_string());

    SQLRETURN ret = SQLSetConnectAttr(m_conn, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    insert_test_value(42, "Some");

    check_test_value(42, "Some");

    ret = SQLEndTran(SQL_HANDLE_ENV, m_env, SQL_ROLLBACK);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_ENV, m_env);

    check_no_test_value(42);
}

TEST_F(transaction_test, transaction_environment_rollback_update_1)
{
    odbc_connect(get_basic_connection_string());

    insert_test_value(42, "Some");

    check_test_value(42, "Some");

    SQLRETURN ret = SQLSetConnectAttr(m_conn, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    update_test_value(42, "Other");

    check_test_value(42, "Other");

    ret = SQLEndTran(SQL_HANDLE_ENV, m_env, SQL_ROLLBACK);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_ENV, m_env);

    check_test_value(42, "Some");
}

TEST_F(transaction_test, transaction_environment_rollback_update_2)
{
    odbc_connect(get_basic_connection_string());

    SQLRETURN ret = SQLSetConnectAttr(m_conn, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    insert_test_value(42, "Some");

    check_test_value(42, "Some");

    ret = SQLEndTran(SQL_HANDLE_DBC, m_conn, SQL_COMMIT);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_ENV, m_env);

    check_test_value(42, "Some");

    update_test_value(42, "Other");

    check_test_value(42, "Other");

    ret = SQLEndTran(SQL_HANDLE_ENV, m_env, SQL_ROLLBACK);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_ENV, m_env);

    check_test_value(42, "Some");
}

TEST_F(transaction_test, transaction_environment_rollback_delete_1)
{
    odbc_connect(get_basic_connection_string());

    insert_test_value(42, "Some");

    check_test_value(42, "Some");

    SQLRETURN ret = SQLSetConnectAttr(m_conn, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    delete_test_value(42);

    check_no_test_value(42);

    ret = SQLEndTran(SQL_HANDLE_ENV, m_env, SQL_ROLLBACK);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_ENV, m_env);

    check_test_value(42, "Some");
}

TEST_F(transaction_test, transaction_environment_rollback_delete_2)
{
    odbc_connect(get_basic_connection_string());

    SQLRETURN ret = SQLSetConnectAttr(m_conn, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    insert_test_value(42, "Some");

    check_test_value(42, "Some");

    ret = SQLEndTran(SQL_HANDLE_DBC, m_conn, SQL_COMMIT);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_ENV, m_env);

    check_test_value(42, "Some");

    delete_test_value(42);

    check_no_test_value(42);

    ret = SQLEndTran(SQL_HANDLE_ENV, m_env, SQL_ROLLBACK);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_ENV, m_env);

    check_test_value(42, "Some");
}

TEST_F(transaction_test, transaction_version_mismatch_error)
{
    odbc_connect(get_basic_connection_string());

    insert_test_value(1, "test_1");

    SQLRETURN ret = SQLSetConnectAttr(m_conn, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);

    check_test_value(1, "test_1");

    odbc_connection conn2;
    conn2.odbc_connect(get_basic_connection_string());

    ret = SQLSetConnectAttr(conn2.m_conn, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, conn2.m_conn);

    insert_test_value(conn2.m_statement, 2, "test_2");

    ret = SQLEndTran(SQL_HANDLE_DBC, conn2.m_conn, SQL_COMMIT);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, conn2.m_conn);

    check_test_value(conn2.m_statement, 1, "test_1");
    check_test_value(conn2.m_statement, 2, "test_2");

    try
    {
        insert_test_value(2, "test_2");

        FAIL() << "Exception is expected";
    }
    catch (std::exception& err)
    {
        // TODO: Fixme

//        BOOST_CHECK(err.message.find("Cannot serialize transaction due to write conflict") != err.message.npos);
//        EXPECT_EQ(err.sqlstate, "40001");

        reset_statement(m_statement);
    }

    try
    {
        check_test_value(1, "test_1");

        FAIL() << "Exception is expected";
    }
    catch (std::exception& err)
    {
        // TODO: Fixme
//        BOOST_CHECK(err.message.find("Transaction is already completed") != err.message.npos);
//        EXPECT_EQ(err.sqlstate, "25000");

        reset_statement(m_statement);
    }

    ret = SQLEndTran(SQL_HANDLE_DBC, m_conn, SQL_ROLLBACK);
    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, m_conn);
}

