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
#include "odbc_connection.h"

#include <gtest/gtest.h>

#include <vector>
#include <string>
#include <algorithm>

using namespace ignite;

/**
 * Checks single row result set for correct work with SQLGetData.
 *
 * @param stmt Statement.
 */
void check_single_row_result_set_with_get_data(SQLHSTMT stmt)
{
    SQLRETURN ret = SQLFetch(stmt);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << get_odbc_error_message(SQL_HANDLE_STMT, stmt);

    char buf[1024];
    SQLLEN buf_len = sizeof(buf);

    ret = SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &buf_len);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << get_odbc_error_message(SQL_HANDLE_STMT, stmt);

    ret = SQLFetch(stmt);

    ASSERT_EQ(ret, SQL_NO_DATA);

    ret = SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &buf_len);

    ASSERT_EQ(ret, SQL_ERROR);
    EXPECT_EQ(get_odbc_error_state(SQL_HANDLE_STMT, stmt), "24000");
}

/**
 * Check string column.
 *
 * @param stmt Statement.
 * @param colId Column ID to check.
 * @param value Expected value.
 */
void check_string_column(SQLHSTMT stmt, int colId, const std::string& value)
{
    char buf[1024];
    SQLLEN bufLen = sizeof(buf);

    SQLRETURN ret = SQLGetData(stmt, colId, SQL_C_CHAR, buf, sizeof(buf), &bufLen);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << get_odbc_error_message(SQL_HANDLE_STMT, stmt);

    if (bufLen <= 0)
        EXPECT_TRUE(value.empty());
    else
        EXPECT_EQ(std::string(buf, static_cast<size_t>(bufLen)), value);
}

/**
 * Check result set column metadata using SQLDescribeCol.
 *
 * @param stmt Statement.
 * @param idx Index.
 * @param exp_name Expected name.
 * @param exp_data_type Expected data type.
 * @param exp_size Expected column size.
 * @param exp_scale Expected column scale.
 * @param exp_nullability expected nullability.
 */
void check_column_meta_with_sqldescribe_col(SQLHSTMT stmt, SQLUSMALLINT idx, const std::string& exp_name,
    SQLSMALLINT exp_data_type, SQLULEN exp_size, SQLSMALLINT exp_scale, SQLSMALLINT exp_nullability)
{
    std::vector<SQLCHAR> name(ODBC_BUFFER_SIZE);
    SQLSMALLINT name_len = 0;
    SQLSMALLINT data_type = 0;
    SQLULEN size;
    SQLSMALLINT scale;
    SQLSMALLINT nullability;

    SQLRETURN ret = SQLDescribeCol(
        stmt, idx, &name[0], (SQLSMALLINT)name.size(), &name_len, &data_type, &size, &scale, &nullability);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    EXPECT_GE(name_len, 0);
    EXPECT_LE(name_len, static_cast<SQLSMALLINT>(ODBC_BUFFER_SIZE));

    std::string name_str(name.begin(), name.begin() + name_len);

    EXPECT_EQ(name_str, exp_name);
    EXPECT_EQ(data_type, exp_data_type);
    EXPECT_EQ(size, exp_size);
    EXPECT_EQ(scale, exp_scale);
    EXPECT_EQ(nullability, exp_nullability);
}

/**
 * Check result set column metadata using SQLColAttribute.
 *
 * @param stmt Statement.
 * @param idx Index.
 * @param exp_name Expected name.
 * @param exp_data_type Expected data type.
 * @param exp_size Expected column size.
 * @param exp_scale Expected column scale.
 * @param exp_nullability expected nullability.
 */
void check_column_meta_with_sqlcol_attribute(SQLHSTMT stmt, SQLUSMALLINT idx, const std::string& exp_name,
    SQLLEN exp_data_type, SQLULEN exp_size, SQLLEN exp_scale, SQLLEN exp_nullability)
{
    std::vector<SQLCHAR> name(ODBC_BUFFER_SIZE);
    SQLSMALLINT name_len = 0;
    SQLLEN data_type = 0;
    SQLLEN size;
    SQLLEN scale;
    SQLLEN nullability;

    SQLRETURN ret = SQLColAttribute(stmt, idx, SQL_DESC_NAME, &name[0], (SQLSMALLINT)name.size(), &name_len, nullptr);
    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLColAttribute(stmt, idx, SQL_DESC_TYPE, nullptr, 0, nullptr, &data_type);
    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLColAttribute(stmt, idx, SQL_DESC_PRECISION, nullptr, 0, nullptr, &size);
    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLColAttribute(stmt, idx, SQL_DESC_SCALE, nullptr, 0, nullptr, &scale);
    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLColAttribute(stmt, idx, SQL_DESC_NULLABLE, nullptr, 0, nullptr, &nullability);
    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    EXPECT_GE(name_len, 0);
    EXPECT_LE(name_len, static_cast<SQLSMALLINT>(ODBC_BUFFER_SIZE));

    std::string name_str(name.begin(), name.begin() + name_len);

    EXPECT_EQ(name_str, exp_name);
    EXPECT_EQ(data_type, exp_data_type);
    EXPECT_EQ(size, exp_size);
    EXPECT_EQ(scale, exp_scale);
    EXPECT_EQ(nullability, exp_nullability);
}

/**
 * Test suite.
 */
class meta_queries_test : public odbc_suite {
public:
    static void SetUpTestSuite() {
        odbc_connection conn(get_basic_connection_string());
        conn.connect();

        SQLRETURN ret = conn.exec_query("CREATE TABLE META_QUERIES_TEST(ID INT PRIMARY KEY, VAL VARCHAR(60))");
        if (!SQL_SUCCEEDED(ret)) {
            FAIL() << conn.get_statement_error_message();
        }
    }

    static void TearDownTestSuite() {
        odbc_connection conn(get_basic_connection_string());
        conn.connect();

        conn.exec_query("DROP TABLE META_QUERIES_TEST");
    }

    void SetUp() override {
        odbc_connect(get_basic_connection_string());
        exec_query("DELETE FROM " + std::string(TABLE_NAME_ALL_COLUMNS_SQL));
        odbc_clean_up();
    }

    /**
     * @param func Function to call before tests. May be PrepareQuery or ExecQuery.
     *
     * 1. Connect to node using ODBC.
     * 2. Create table with decimal and char columns with specified size and scale.
     * 3. Execute or prepare statement.
     * 4. Check precision and scale of every column using SQLDescribeCol.
     */
    template<typename F>
    void check_sqldescribe_col_precision_and_scale(F func)
    {
        odbc_connect(get_basic_connection_string());

        SQLRETURN ret = exec_query(
            "create table TestScalePrecision("
            "   id int primary key,"
            "   dec1 decimal(3,0),"
            "   dec2 decimal(42,12),"
            "   dec3 decimal,"
            "   char1 char(3),"
            "   char2 char(42),"
            "   char3 char not null,"
            "   vchar varchar"
            ")");

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = SQLFreeStmt(m_statement, SQL_CLOSE);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = exec_query(
            "insert into "
            "TestScalePrecision(id, dec1, dec2, dec3, char1, char2, char3, vchar) "
            "values (1, 12, 160.23, -1234.56789, 'TST', 'Lorem Ipsum', 'Some test value', 'Some test varchar')");

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = SQLFreeStmt(m_statement, SQL_CLOSE);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = (this->*func)("select id, dec1, dec2, dec3, char1, char2, char3, vchar from PUBLIC.TestScalePrecision");
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        SQLSMALLINT columnCount = 0;

        ret = SQLNumResultCols(m_statement, &columnCount);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        EXPECT_EQ(columnCount, 8);

        check_column_meta_with_sqldescribe_col(m_statement, 1, "ID", SQL_INTEGER, 10, 0, SQL_NULLABLE);
        check_column_meta_with_sqldescribe_col(m_statement, 2, "DEC1", SQL_DECIMAL, 3, 0, SQL_NULLABLE);
        check_column_meta_with_sqldescribe_col(m_statement, 3, "DEC2", SQL_DECIMAL, 42, 12, SQL_NULLABLE);
        check_column_meta_with_sqldescribe_col(m_statement, 4, "DEC3", SQL_DECIMAL, 65535, 32767, SQL_NULLABLE);
        check_column_meta_with_sqldescribe_col(m_statement, 5, "CHAR1", SQL_VARCHAR, 3, 0, SQL_NULLABLE);
        check_column_meta_with_sqldescribe_col(m_statement, 6, "CHAR2", SQL_VARCHAR, 42, 0, SQL_NULLABLE);
        check_column_meta_with_sqldescribe_col(m_statement, 7, "CHAR3", SQL_VARCHAR, 2147483647, 0, SQL_NO_NULLS);
        check_column_meta_with_sqldescribe_col(m_statement, 8, "VCHAR", SQL_VARCHAR, 2147483647, 0, SQL_NULLABLE);
    }

    /**
     * @param func Function to call before tests. May be PrepareQuery or ExecQuery.
     *
     * 1. Connect to node using ODBC.
     * 2. Create table with decimal and char columns with specified size and scale.
     * 3. Execute or prepare statement.
     * 4. Check precision and scale of every column using SQLColAttribute.
     */
    template<typename F>
    void check_sqlcol_attribute_precision_and_scale(F func)
    {
        odbc_connect(get_basic_connection_string());

        SQLRETURN ret = exec_query(
            "create table TestScalePrecision("
            "   id int primary key,"
            "   dec1 decimal(3,0),"
            "   dec2 decimal(42,12),"
            "   dec3 decimal,"
            "   char1 char(3),"
            "   char2 char(42),"
            "   char3 char not null,"
            "   vchar varchar"
            ")");

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = SQLFreeStmt(m_statement, SQL_CLOSE);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = exec_query(
            "insert into "
            "TestScalePrecision(id, dec1, dec2, dec3, char1, char2, char3, vchar) "
            "values (1, 12, 160.23, -1234.56789, 'TST', 'Lorem Ipsum', 'Some test value', 'Some test varchar')");

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = SQLFreeStmt(m_statement, SQL_CLOSE);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = (this->*func)("select id, dec1, dec2, dec3, char1, char2, char3, vchar from PUBLIC.TestScalePrecision");
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        SQLSMALLINT columnCount = 0;

        ret = SQLNumResultCols(m_statement, &columnCount);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        EXPECT_EQ(columnCount, 8);

        check_column_meta_with_sqlcol_attribute(m_statement, 1, "ID", SQL_INTEGER, 10, 0, SQL_NULLABLE);
        check_column_meta_with_sqlcol_attribute(m_statement, 2, "DEC1", SQL_DECIMAL, 3, 0, SQL_NULLABLE);
        check_column_meta_with_sqlcol_attribute(m_statement, 3, "DEC2", SQL_DECIMAL, 42, 12, SQL_NULLABLE);
        check_column_meta_with_sqlcol_attribute(m_statement, 4, "DEC3", SQL_DECIMAL, 65535, 32767, SQL_NULLABLE);
        check_column_meta_with_sqlcol_attribute(m_statement, 5, "CHAR1", SQL_VARCHAR, 3, 0, SQL_NULLABLE);
        check_column_meta_with_sqlcol_attribute(m_statement, 6, "CHAR2", SQL_VARCHAR, 42, 0, SQL_NULLABLE);
        check_column_meta_with_sqlcol_attribute(m_statement, 7, "CHAR3", SQL_VARCHAR, 2147483647, 0, SQL_NO_NULLS);
        check_column_meta_with_sqlcol_attribute(m_statement, 8, "VCHAR", SQL_VARCHAR, 2147483647, 0, SQL_NULLABLE);
    }

    void insert_test_string()
    {
        auto insert_req =
            "INSERT INTO " + std::string(TABLE_NAME_ALL_COLUMNS_SQL) + "(key, str) VALUES(42, 'Lorem ipsum')";

        SQLRETURN ret = exec_query(insert_req);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << get_odbc_error_message(SQL_HANDLE_STMT, m_statement);

        // Resetting parameters.
        ret = SQLFreeStmt(m_statement, SQL_RESET_PARAMS);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << get_odbc_error_message(SQL_HANDLE_STMT, m_statement);
    }

};

// TODO IGNITE-19216 Implement type info fetching
#ifdef MUTED
TEST_F(meta_queries_test, test_get_type_info_all_types)
{
    odbc_connect(get_basic_connection_string());

    SQLRETURN ret = SQLGetTypeInfo(m_statement, SQL_ALL_TYPES);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));
}
#endif // MUTED

TEST_F(meta_queries_test, test_date_type_column_attribute_curdate)
{
    odbc_connect(get_basic_connection_string());

    SQLCHAR req[] = "select {fn CURDATE()}";
    SQLRETURN ret = SQLExecDirect(m_statement, req, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    SQLLEN int_val = 0;

    ret = SQLColAttribute(m_statement, 1, SQL_DESC_TYPE, nullptr, 0, nullptr, &int_val);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    EXPECT_EQ(int_val, SQL_TYPE_DATE);
}

TEST_F(meta_queries_test, test_date_type_column_attribute_literal)
{
    odbc_connect(get_basic_connection_string());

    SQLCHAR req[] = "select DATE '2020-10-25'";
    SQLExecDirect(m_statement, req, SQL_NTS);

    SQLLEN int_val = 0;

    SQLRETURN ret = SQLColAttribute(m_statement, 1, SQL_DESC_TYPE, nullptr, 0, nullptr, &int_val);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    EXPECT_EQ(int_val, SQL_TYPE_DATE);
}

TEST_F(meta_queries_test, test_date_type_column_attribute_field)
{
    odbc_connect(get_basic_connection_string());

    SQLCHAR req[] = "select {fn CONVERT(date, DATE)} from TBL_ALL_COLUMNS_SQL";
    SQLRETURN ret = SQLExecDirect(m_statement, req, SQL_NTS);
    if (!SQL_SUCCEEDED(ret))
    {
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));
        return;
    }

    SQLLEN int_val = 0;

    ret = SQLColAttribute(m_statement, 1, SQL_DESC_TYPE, nullptr, 0, nullptr, &int_val);

    if (!SQL_SUCCEEDED(ret))
    {
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));
        return;
    }

    EXPECT_EQ(int_val, SQL_TYPE_DATE);
}

TEST_F(meta_queries_test, test_time_type_column_attribute_literal)
{
    odbc_connect(get_basic_connection_string());

    SQLCHAR req[] = "select TIME '12:42:13'";
    SQLExecDirect(m_statement, req, SQL_NTS);

    SQLLEN int_val = 0;

    SQLRETURN ret = SQLColAttribute(m_statement, 1, SQL_DESC_TYPE, nullptr, 0, nullptr, &int_val);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    EXPECT_EQ(int_val, SQL_TYPE_TIME);
}

TEST_F(meta_queries_test, test_time_type_column_attribute_field)
{
    odbc_connect(get_basic_connection_string());

    SQLCHAR req[] = "select time2 from TBL_ALL_COLUMNS_SQL";
    SQLExecDirect(m_statement, req, SQL_NTS);

    SQLLEN int_val = 0;

    SQLRETURN ret = SQLColAttribute(m_statement, 1, SQL_DESC_TYPE, nullptr, 0, nullptr, &int_val);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    EXPECT_EQ(int_val, SQL_TYPE_TIME);
}

TEST_F(meta_queries_test, test_col_attributes_column_length)
{
    odbc_connect(get_basic_connection_string());

    SQLCHAR req[] = "select str from TBL_ALL_COLUMNS_SQL";
    SQLExecDirect(m_statement, req, SQL_NTS);

    SQLLEN int_val;
    SQLCHAR str_buf[1024];
    SQLSMALLINT str_len;

    SQLRETURN ret = SQLColAttribute(m_statement, 1, SQL_COLUMN_LENGTH, str_buf, sizeof(str_buf), &str_len, &int_val);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    EXPECT_EQ(int_val, 60);
}

TEST_F(meta_queries_test, test_col_attributes_column_presicion)
{
    odbc_connect(get_basic_connection_string());

    SQLCHAR req[] = "select str from TBL_ALL_COLUMNS_SQL";
    SQLExecDirect(m_statement, req, SQL_NTS);

    SQLLEN int_val;
    SQLCHAR str_buf[1024];
    SQLSMALLINT str_len;

    SQLRETURN ret = SQLColAttribute(m_statement, 1, SQL_COLUMN_PRECISION, str_buf, sizeof(str_buf), &str_len, &int_val);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    EXPECT_EQ(int_val, 60);
}

TEST_F(meta_queries_test, test_col_attributes_column_scale)
{
    odbc_connect(get_basic_connection_string());

    SQLCHAR req[] = "select str from TBL_ALL_COLUMNS_SQL";
    SQLExecDirect(m_statement, req, SQL_NTS);

    SQLLEN int_val;
    SQLCHAR str_buf[1024];
    SQLSMALLINT str_len;

    SQLRETURN ret = SQLColAttribute(m_statement, 1, SQL_COLUMN_SCALE, str_buf, sizeof(str_buf), &str_len, &int_val);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));
}

TEST_F(meta_queries_test, test_col_attributes_column_length_prepare)
{
    odbc_connect(get_basic_connection_string());

    insert_test_string();

    SQLCHAR req[] = "select str from TBL_ALL_COLUMNS_SQL";
    SQLPrepare(m_statement, req, SQL_NTS);

    SQLLEN int_val;
    SQLCHAR str_buf[1024];
    SQLSMALLINT str_len;

    SQLRETURN ret = SQLColAttribute(m_statement, 1, SQL_COLUMN_LENGTH, str_buf, sizeof(str_buf), &str_len, &int_val);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    EXPECT_EQ(int_val, 60);

    ret = SQLExecute(m_statement);
    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    ret = SQLColAttribute(m_statement, 1, SQL_COLUMN_LENGTH, str_buf, sizeof(str_buf), &str_len, &int_val);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    EXPECT_EQ(int_val, 60);
}

TEST_F(meta_queries_test, test_col_attributes_column_presicion_prepare)
{
    odbc_connect(get_basic_connection_string());

    insert_test_string();

    SQLCHAR req[] = "select str from TBL_ALL_COLUMNS_SQL";
    SQLPrepare(m_statement, req, SQL_NTS);

    SQLLEN int_val;
    SQLCHAR str_buf[1024];
    SQLSMALLINT str_len;

    SQLRETURN ret = SQLColAttribute(m_statement, 1, SQL_COLUMN_PRECISION, str_buf, sizeof(str_buf), &str_len, &int_val);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    EXPECT_EQ(int_val, 60);

    ret = SQLExecute(m_statement);
    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    ret = SQLColAttribute(m_statement, 1, SQL_COLUMN_PRECISION, str_buf, sizeof(str_buf), &str_len, &int_val);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    EXPECT_EQ(int_val, 60);
}

TEST_F(meta_queries_test, test_col_attributes_column_scale_prepare)
{
    odbc_connect(get_basic_connection_string());

    insert_test_string();

    SQLCHAR req[] = "select str from TBL_ALL_COLUMNS_SQL";
    SQLPrepare(m_statement, req, SQL_NTS);

    SQLLEN int_val;
    SQLCHAR str_buf[1024];
    SQLSMALLINT str_len;

    SQLRETURN ret = SQLColAttribute(m_statement, 1, SQL_COLUMN_SCALE, str_buf, sizeof(str_buf), &str_len, &int_val);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLExecute(m_statement);
    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    ret = SQLColAttribute(m_statement, 1, SQL_COLUMN_SCALE, str_buf, sizeof(str_buf), &str_len, &int_val);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));
}

TEST_F(meta_queries_test, test_get_data_with_get_type_info)
{
    odbc_connect(get_basic_connection_string());

    SQLRETURN ret = SQLGetTypeInfo(m_statement, SQL_VARCHAR);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    check_single_row_result_set_with_get_data(m_statement);
}

TEST_F(meta_queries_test, test_get_data_with_tables)
{
    odbc_connect(get_basic_connection_string());

    SQLCHAR empty[] = "";
    SQLCHAR table[] = "TestType";

    SQLRETURN ret = SQLTables(m_statement, empty, SQL_NTS, empty, SQL_NTS, table, SQL_NTS, empty, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    check_single_row_result_set_with_get_data(m_statement);
}

TEST_F(meta_queries_test, test_get_data_with_columns)
{
    odbc_connect(get_basic_connection_string());

    SQLCHAR empty[] = "";
    SQLCHAR table[] = "TestType";
    SQLCHAR column[] = "str";

    SQLRETURN ret = SQLColumns(m_statement, empty, SQL_NTS, empty, SQL_NTS, table, SQL_NTS, column, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    check_single_row_result_set_with_get_data(m_statement);
}

TEST_F(meta_queries_test, test_get_data_with_select_query)
{
    odbc_connect(get_basic_connection_string());

    SQLCHAR insert_req[] = "insert into TestType(_key, str) VALUES(1, 'Lorem ipsum')";
    SQLRETURN ret = SQLExecDirect(m_statement, insert_req, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    SQLCHAR select_req[] = "select str from TBL_ALL_COLUMNS_SQL";
    ret = SQLExecDirect(m_statement, select_req, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    check_single_row_result_set_with_get_data(m_statement);
}

TEST_F(meta_queries_test, test_insert_too_long_value_fail)
{
    odbc_connect(get_basic_connection_string());

    SQLCHAR insert_req[] =
        "insert into TestType(_key, str) VALUES(42, '0123456789012345678901234567890123456789012345678901234567891')";

    SQLRETURN ret = SQLExecDirect(m_statement, insert_req, SQL_NTS);

    if (SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));
}

TEST_F(meta_queries_test, test_get_info_scroll_options)
{
    odbc_connect(get_basic_connection_string());

    SQLUINTEGER val = 0;
    SQLRETURN ret = SQLGetInfo(m_conn, SQL_SCROLL_OPTIONS, &val, 0, nullptr);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_DBC, m_conn));

    EXPECT_NE(val, 0);
}

TEST_F(meta_queries_test, test_ddl_tables_meta)
{
    odbc_connect(get_basic_connection_string());

    SQLCHAR create_table[] = "create table TestTable(id int primary key, testColumn varchar)";
    SQLRETURN ret = SQLExecDirect(m_statement, create_table, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    SQLCHAR empty[] = "";
    SQLCHAR table[] = "TestTable";

    ret = SQLTables(m_statement, empty, SQL_NTS, empty, SQL_NTS, table, SQL_NTS, empty, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLFetch(m_statement);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    check_string_column(m_statement, 1, "");
    check_string_column(m_statement, 2, "\"PUBLIC\"");
    check_string_column(m_statement, 3, "TESTTABLE");
    check_string_column(m_statement, 4, "TABLE");

    ret = SQLFetch(m_statement);

    ASSERT_EQ(ret, SQL_NO_DATA);
}

TEST_F(meta_queries_test, test_ddl_tables_meta_table_type_list)
{
    odbc_connect(get_basic_connection_string());

    SQLCHAR create_table[] = "create table TestTable(id int primary key, testColumn varchar)";
    SQLRETURN ret = SQLExecDirect(m_statement, create_table, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    SQLCHAR* typeLists[] = {(SQLCHAR*)"'TABLE'", (SQLCHAR*)"TABLE,VIEW"};
    for (auto &type_list : typeLists) {
        SQLCHAR empty[] = "";
        SQLCHAR table[] = "TestTable";

        ret = SQLTables(m_statement, empty, SQL_NTS, empty, SQL_NTS, table, SQL_NTS, type_list, SQL_NTS);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        ret = SQLFetch(m_statement);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        check_string_column(m_statement, 1, "");
        check_string_column(m_statement, 2, "\"PUBLIC\"");
        check_string_column(m_statement, 3, "TESTTABLE");
        check_string_column(m_statement, 4, "TABLE");

        ret = SQLFetch(m_statement);

        ASSERT_EQ(ret, SQL_NO_DATA);
    }
}

TEST_F(meta_queries_test, test_ddl_columns_meta)
{
    odbc_connect(get_basic_connection_string());

    SQLCHAR create_table[] = "create table TestTable(id int primary key, testColumn varchar)";
    SQLRETURN ret = SQLExecDirect(m_statement, create_table, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    SQLCHAR empty[] = "";
    SQLCHAR table[] = "TestTable";

    ret = SQLColumns(m_statement, empty, SQL_NTS, empty, SQL_NTS, table, SQL_NTS, empty, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLFetch(m_statement);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    check_string_column(m_statement, 1, "");
    check_string_column(m_statement, 2, "\"PUBLIC\"");
    check_string_column(m_statement, 3, "TESTTABLE");
    check_string_column(m_statement, 4, "ID");

    ret = SQLFetch(m_statement);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    check_string_column(m_statement, 1, "");
    check_string_column(m_statement, 2, "\"PUBLIC\"");
    check_string_column(m_statement, 3, "TESTTABLE");
    check_string_column(m_statement, 4, "TESTCOLUMN");

    ret = SQLFetch(m_statement);

    ASSERT_EQ(ret, SQL_NO_DATA);
}

TEST_F(meta_queries_test, test_ddl_columns_meta_escaped)
{
    odbc_connect(get_basic_connection_string());

    SQLCHAR create_table[] = "create table ESG_FOCUS(id int primary key, TEST_COLUMN varchar)";
    SQLRETURN ret = SQLExecDirect(m_statement, create_table, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    SQLCHAR empty[] = "";
    SQLCHAR table[] = "ESG\\_FOCUS";

    ret = SQLColumns(m_statement, empty, SQL_NTS, empty, SQL_NTS, table, SQL_NTS, empty, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLFetch(m_statement);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    check_string_column(m_statement, 1, "");
    check_string_column(m_statement, 2, "\"PUBLIC\"");
    check_string_column(m_statement, 3, "ESG_FOCUS");
    check_string_column(m_statement, 4, "ID");

    ret = SQLFetch(m_statement);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    check_string_column(m_statement, 1, "");
    check_string_column(m_statement, 2, "\"PUBLIC\"");
    check_string_column(m_statement, 3, "ESG_FOCUS");
    check_string_column(m_statement, 4, "TEST_COLUMN");

    ret = SQLFetch(m_statement);

    ASSERT_EQ(ret, SQL_NO_DATA);
}

TEST_F(meta_queries_test, test_sqlnum_result_cols_after_sqlprepare)
{
    odbc_connect(get_basic_connection_string());

    SQLRETURN ret = exec_query("create table TestSqlPrepare(id int primary key, test1 varchar, test2 long, test3 varchar)");
    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    ret = SQLFreeStmt(m_statement, SQL_CLOSE);
    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    // TODO: IGNITE-19854 Implement metadata fetching for the non-executed query.
//    ret = prepare_query("select * from PUBLIC.TestSqlPrepare");
//    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);
//
//    SQLSMALLINT columnCount = 0;
//
//    ret = SQLNumResultCols(m_statement, &columnCount);
//    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);
//
//    EXPECT_EQ(columnCount, 4);
//
//    ret = SQLExecute(m_statement);
//    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);
//
//    columnCount = 0;
//
//    ret = SQLNumResultCols(m_statement, &columnCount);
//    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);
//
//    EXPECT_EQ(columnCount, 4);
}

/**
 * Check that SQLDescribeCol return valid scale and precision for columns of different type after Prepare.
 */
TEST_F(meta_queries_test, test_sqldescribe_col_precision_and_scale_after_prepare)
{
    // TODO: IGNITE-19854 Implement metadata fetching for the non-executed query.
//    check_sqldescribe_col_precision_and_scale(&odbc_suite::prepare_query);
}

/**
 * Check that SQLDescribeCol return valid scale and precision for columns of different type after Execute.
 */
TEST_F(meta_queries_test, test_sqldescribe_col_precision_and_scale_after_exec)
{
    check_sqldescribe_col_precision_and_scale(&odbc_suite::exec_query);
}

/**
 * Check that SQLColAttribute return valid scale and precision for columns of different type after Prepare.
 */
TEST_F(meta_queries_test, test_sqlcol_attribute_precision_and_scale_after_prepare)
{
    // TODO: IGNITE-19854 Implement metadata fetching for the non-executed query.
//    check_sqlcol_attribute_precision_and_scale(&odbc_suite::prepare_query);
}

/**
 * Check that SQLColAttribute return valid scale and precision for columns of different type after Execute.
 */
TEST_F(meta_queries_test, test_sqlcol_attribute_precision_and_scale_after_exec)
{
    check_sqlcol_attribute_precision_and_scale(&odbc_suite::exec_query);
}
