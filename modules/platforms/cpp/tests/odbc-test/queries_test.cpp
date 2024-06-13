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
#include <cstring>
#include <string>
#include <vector>

using namespace ignite;

/**
 * Test setup fixture.
 */
struct queries_test : public odbc_suite {
    void SetUp() override {
        odbc_connect(get_basic_connection_string());
        exec_query("DELETE FROM " + TABLE_NAME_ALL_COLUMNS_SQL);
        odbc_clean_up();
    }

    template<typename T>
    void check_two_rows_int(SQLSMALLINT type) {
        odbc_connect(get_basic_connection_string());

        SQLRETURN ret;

        insert_all_types_row(1);
        insert_all_types_row(2);

        const SQLSMALLINT columns_cnt = 13;

        T columns[columns_cnt];

        std::memset(&columns, 0, sizeof(columns));

        // Binding columns.
        for (SQLSMALLINT i = 0; i < columns_cnt; ++i) {
            ret = SQLBindCol(m_statement, i + 1, type, &columns[i], sizeof(columns[i]), nullptr);

            if (!SQL_SUCCEEDED(ret))
                FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));
        }

        SQLCHAR request[] = "select key,str,int8,int16,int32,int64,\"FLOAT\",\"DOUBLE\",\"UUID\","
                            "\"DATE\",\"TIME\",\"DATETIME\",\"DECIMAL\" FROM TBL_ALL_COLUMNS_SQL ORDER BY key";

        ret = SQLExecDirect(m_statement, request, SQL_NTS);
        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        ret = SQLFetch(m_statement);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        EXPECT_EQ(columns[0], 1);
        EXPECT_EQ(columns[1], 2);
        EXPECT_EQ(columns[2], 3);
        EXPECT_EQ(columns[3], 4);
        EXPECT_EQ(columns[4], 5);
        EXPECT_EQ(columns[5], 6);
        EXPECT_EQ(columns[6], 7);
        EXPECT_EQ(columns[7], 8);
        EXPECT_EQ(columns[8], 0);
        EXPECT_EQ(columns[9], 0);
        EXPECT_EQ(columns[10], 0);
        EXPECT_EQ(columns[11], 0);
        EXPECT_EQ(columns[12], T(-1));

        SQLLEN column_lens[columns_cnt];

        // Binding columns.
        for (SQLSMALLINT i = 0; i < columns_cnt; ++i) {
            ret = SQLBindCol(m_statement, i + 1, type, &columns[i], sizeof(columns[i]), &column_lens[i]);

            if (!SQL_SUCCEEDED(ret))
                FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));
        }

        ret = SQLFetch(m_statement);
        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        EXPECT_EQ(columns[0], 2);
        EXPECT_EQ(columns[1], 4);
        EXPECT_EQ(columns[2], 6);
        EXPECT_EQ(columns[3], 8);
        EXPECT_EQ(columns[4], 10);
        EXPECT_EQ(columns[5], 12);
        EXPECT_EQ(columns[6], 14);
        EXPECT_EQ(columns[7], 16);
        EXPECT_EQ(columns[8], 0);
        EXPECT_EQ(columns[9], 0);
        EXPECT_EQ(columns[10], 0);
        EXPECT_EQ(columns[11], 0);
        EXPECT_EQ(columns[12], 2);

        EXPECT_EQ(column_lens[0], static_cast<SQLLEN>(sizeof(T)));
        EXPECT_EQ(column_lens[1], static_cast<SQLLEN>(sizeof(T)));
        EXPECT_EQ(column_lens[2], static_cast<SQLLEN>(sizeof(T)));
        EXPECT_EQ(column_lens[3], static_cast<SQLLEN>(sizeof(T)));
        EXPECT_EQ(column_lens[4], static_cast<SQLLEN>(sizeof(T)));
        EXPECT_EQ(column_lens[5], static_cast<SQLLEN>(sizeof(T)));
        EXPECT_EQ(column_lens[6], static_cast<SQLLEN>(sizeof(T)));
        EXPECT_EQ(column_lens[7], static_cast<SQLLEN>(sizeof(T)));

        ret = SQLFetch(m_statement);
        EXPECT_EQ(ret, SQL_NO_DATA);
    }

    void check_params_num(const std::string &req, SQLSMALLINT expectedParamsNum) {
        std::vector<SQLCHAR> req0(req.begin(), req.end());

        SQLRETURN ret = SQLPrepare(m_statement, &req0[0], static_cast<SQLINTEGER>(req0.size()));

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        SQLSMALLINT params_num = -1;

        ret = SQLNumParams(m_statement, &params_num);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        EXPECT_EQ(params_num, expectedParamsNum);
    }
};

TEST_F(queries_test, two_rows_int8) {
    check_two_rows_int<signed char>(SQL_C_STINYINT);
}

TEST_F(queries_test, two_rows_uint8) {
    check_two_rows_int<unsigned char>(SQL_C_UTINYINT);
}

TEST_F(queries_test, two_rows_int16) {
    check_two_rows_int<signed short>(SQL_C_SSHORT);
}

TEST_F(queries_test, two_rows_uint16) {
    check_two_rows_int<unsigned short>(SQL_C_USHORT);
}

TEST_F(queries_test, two_rows_int32) {
    check_two_rows_int<SQLINTEGER>(SQL_C_SLONG);
}

TEST_F(queries_test, two_rows_uint32) {
    check_two_rows_int<SQLUINTEGER>(SQL_C_ULONG);
}

TEST_F(queries_test, two_rows_int64) {
    check_two_rows_int<std::int64_t>(SQL_C_SBIGINT);
}

TEST_F(queries_test, two_rows_uint64) {
    check_two_rows_int<std::uint64_t>(SQL_C_UBIGINT);
}

TEST_F(queries_test, two_rows_string) {
    odbc_connect(get_basic_connection_string());

    insert_all_types_row(1);
    insert_all_types_row(2);

    const SQLSMALLINT columns_cnt = 13;

    static SQLCHAR columns[columns_cnt][ODBC_BUFFER_SIZE] = {};

    SQLRETURN ret;
    // Binding columns.
    for (SQLSMALLINT i = 0; i < columns_cnt; ++i) {
        ret = SQLBindCol(m_statement, i + 1, SQL_C_CHAR, &columns[i], ODBC_BUFFER_SIZE, nullptr);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));
    }

    SQLCHAR request[] = "select key,str,int8,int16,int32,int64,\"FLOAT\",\"DOUBLE\",\"UUID\","
                        "\"DATE\",\"TIME\",\"DATETIME\",\"DECIMAL\" FROM TBL_ALL_COLUMNS_SQL ORDER BY key";

    ret = SQLExecDirect(m_statement, request, SQL_NTS);
    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLFetch(m_statement);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[0])), "1");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[1])), "2");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[2])), "3");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[3])), "4");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[4])), "5");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[5])), "6");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[6])), "7");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[7])), "8");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[8])), "00000009-000a-000b-0000-000000000000");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[9])), "2001-02-02");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[10])), "01:01:01");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[11])), "2002-03-03 01:01:01");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[12])), "-1.000");

    SQLLEN column_lens[columns_cnt];

    // Binding columns.
    for (SQLSMALLINT i = 0; i < columns_cnt; ++i) {
        ret = SQLBindCol(m_statement, i + 1, SQL_C_CHAR, &columns[i], ODBC_BUFFER_SIZE, &column_lens[i]);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));
    }

    ret = SQLFetch(m_statement);
    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[0])), "2");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[1])), "4");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[2])), "6");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[3])), "8");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[4])), "10");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[5])), "12");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[6])), "14");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[7])), "16");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[8])), "00000012-0014-0016-0000-000000000000");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[9])), "2002-03-03");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[10])), "02:02:02");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[11])), "2003-04-04 02:02:02");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[12])), "2.000");

    EXPECT_EQ(column_lens[0], 1);
    EXPECT_EQ(column_lens[1], 1);
    EXPECT_EQ(column_lens[2], 1);
    EXPECT_EQ(column_lens[3], 1);
    EXPECT_EQ(column_lens[4], 2);
    EXPECT_EQ(column_lens[5], 2);
    EXPECT_EQ(column_lens[6], 2);
    EXPECT_EQ(column_lens[7], 2);
    EXPECT_EQ(column_lens[8], 36);
    EXPECT_EQ(column_lens[9], 11);
    EXPECT_EQ(column_lens[10], 9);
    EXPECT_EQ(column_lens[11], 20);
    EXPECT_EQ(column_lens[12], 5);

    ret = SQLFetch(m_statement);
    EXPECT_TRUE(ret == SQL_NO_DATA);
}

TEST_F(queries_test, one_row_string_len) {
    odbc_connect(get_basic_connection_string());

    SQLRETURN ret;

    insert_all_types_row(1);

    const SQLSMALLINT columns_cnt = 13;

    SQLLEN column_lens[columns_cnt];

    // Binding columns.
    for (SQLSMALLINT i = 0; i < columns_cnt; ++i) {
        ret = SQLBindCol(m_statement, i + 1, SQL_C_CHAR, nullptr, 0, &column_lens[i]);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));
    }

    SQLCHAR request[] = "select key,str,int8,int16,int32,int64,\"FLOAT\",\"DOUBLE\",\"UUID\","
                        "\"DATE\",\"TIME\",\"DATETIME\",\"DECIMAL\" FROM TBL_ALL_COLUMNS_SQL ORDER BY key";

    ret = SQLExecDirect(m_statement, request, SQL_NTS);
    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLFetch(m_statement);
    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    EXPECT_EQ(column_lens[0], 1);
    EXPECT_EQ(column_lens[1], 1);
    EXPECT_EQ(column_lens[2], 1);
    EXPECT_EQ(column_lens[3], 1);
    EXPECT_EQ(column_lens[4], 1);
    EXPECT_EQ(column_lens[5], 1);
    EXPECT_EQ(column_lens[6], 1);
    EXPECT_EQ(column_lens[7], 1);
    EXPECT_EQ(column_lens[8], 36);
    EXPECT_EQ(column_lens[9], 11);
    EXPECT_EQ(column_lens[10], 9);
    EXPECT_EQ(column_lens[11], 20);
    EXPECT_EQ(column_lens[12], 6);

    ret = SQLFetch(m_statement);
    EXPECT_TRUE(ret == SQL_NO_DATA);
}

TEST_F(queries_test, data_at_execution) {
    odbc_connect(get_basic_connection_string());

    insert_all_types_row(1);
    insert_all_types_row(2);

    const SQLSMALLINT columns_cnt = 13;

    static SQLLEN column_lens[columns_cnt] = {};
    static SQLCHAR columns[columns_cnt][ODBC_BUFFER_SIZE] {};

    SQLRETURN ret;
    // Binding columns.
    for (SQLSMALLINT i = 0; i < columns_cnt; ++i) {
        ret = SQLBindCol(m_statement, i + 1, SQL_C_CHAR, &columns[i], ODBC_BUFFER_SIZE, &column_lens[i]);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));
    }

    SQLCHAR request[] = "select key,str,int8,int16,int32,int64,\"FLOAT\",\"DOUBLE\",\"UUID\","
                        "\"DATE\",\"TIME\",\"DATETIME\",\"DECIMAL\" FROM TBL_ALL_COLUMNS_SQL "
                        "where int32 = ? AND str = ? ORDER BY key";

    ret = SQLPrepare(m_statement, request, SQL_NTS);

    SQLLEN ind1 = 1;
    SQLLEN ind2 = 2;

    SQLINTEGER int32_arg = 5;
    SQLCHAR str_arg[] = "2";

    SQLLEN len1 = SQL_DATA_AT_EXEC;
    SQLLEN len2 = SQL_LEN_DATA_AT_EXEC(static_cast<SQLLEN>(sizeof(str_arg)));

    ret = SQLBindParameter(
        m_statement, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 100, 100, &ind1, sizeof(ind1), &len1);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLBindParameter(
        m_statement, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 100, 100, &ind2, sizeof(ind2), &len2);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLExecute(m_statement);

    ASSERT_EQ(ret, SQL_NEED_DATA);

    void *oind;

    ret = SQLParamData(m_statement, &oind);

    ASSERT_EQ(ret, SQL_NEED_DATA);

    if (oind == &ind1)
        ret = SQLPutData(m_statement, &int32_arg, 0);
    else if (oind == &ind2)
        ret = SQLPutData(m_statement, (SQLPOINTER) str_arg, (SQLLEN) sizeof(str_arg));
    else
        FAIL() << ("Unknown indicator value");

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLParamData(m_statement, &oind);

    ASSERT_EQ(ret, SQL_NEED_DATA);

    if (oind == &ind1)
        ret = SQLPutData(m_statement, &int32_arg, 0);
    else if (oind == &ind2)
        ret = SQLPutData(m_statement, (SQLPOINTER) str_arg, (SQLLEN) sizeof(str_arg));
    else
        FAIL() << ("Unknown indicator value");

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLParamData(m_statement, &oind);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLFetch(m_statement);
    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[0])), "1");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[1])), "2");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[2])), "3");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[3])), "4");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[4])), "5");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[5])), "6");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[6])), "7");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[7])), "8");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[8])), "00000009-000a-000b-0000-000000000000");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[9])), "2001-02-02");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[10])), "01:01:01");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[11])), "2002-03-03 01:01:01");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(columns[12])), "-1.000");

    EXPECT_EQ(column_lens[0], 1);
    EXPECT_EQ(column_lens[1], 1);
    EXPECT_EQ(column_lens[2], 1);
    EXPECT_EQ(column_lens[3], 1);
    EXPECT_EQ(column_lens[4], 1);
    EXPECT_EQ(column_lens[5], 1);
    EXPECT_EQ(column_lens[6], 1);
    EXPECT_EQ(column_lens[7], 1);
    EXPECT_EQ(column_lens[8], 36);
    EXPECT_EQ(column_lens[9], 11);
    EXPECT_EQ(column_lens[10], 9);
    EXPECT_EQ(column_lens[11], 20);
    EXPECT_EQ(column_lens[12], 6);

    ret = SQLFetch(m_statement);
    EXPECT_TRUE(ret == SQL_NO_DATA);
}

TEST_F(queries_test, null_fields) {
    odbc_connect(get_basic_connection_string());

    SQLRETURN ret;

    insert_all_types_row(1);

    exec_query(
        "insert into TBL_ALL_COLUMNS_SQL("
        "   key,str,int8,int16,int32,int64,\"FLOAT\",\"DOUBLE\",\"UUID\",\"DATE\",\"TIME\",\"DATETIME\",\"DECIMAL\") "
        "values(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)");

    insert_all_types_row(3);

    const SQLSMALLINT columns_cnt = 12;

    SQLLEN column_lens[columns_cnt];

    char str_arg[ODBC_BUFFER_SIZE];
    std::int8_t i8_arg;
    std::int16_t i16_arg;
    std::int32_t i32_arg;
    std::int64_t i64_arg;
    float float_arg;
    double double_arg;
    SQLGUID uuid_arg;
    SQL_DATE_STRUCT date_arg;
    SQL_TIME_STRUCT time_arg;
    SQL_TIMESTAMP_STRUCT timestamp_arg;
    SQL_NUMERIC_STRUCT decimal_arg;

    // Binding columns.
    ret = SQLBindCol(m_statement, 1, SQL_C_CHAR, &str_arg, ODBC_BUFFER_SIZE, &column_lens[0]);
    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLBindCol(m_statement, 2, SQL_C_STINYINT, &i8_arg, 0, &column_lens[1]);
    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLBindCol(m_statement, 3, SQL_C_SSHORT, &i16_arg, 0, &column_lens[2]);
    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLBindCol(m_statement, 4, SQL_C_SLONG, &i32_arg, 0, &column_lens[3]);
    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLBindCol(m_statement, 5, SQL_C_SBIGINT, &i64_arg, 0, &column_lens[4]);
    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLBindCol(m_statement, 6, SQL_C_FLOAT, &float_arg, 0, &column_lens[5]);
    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLBindCol(m_statement, 7, SQL_C_DOUBLE, &double_arg, 0, &column_lens[6]);
    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLBindCol(m_statement, 8, SQL_C_GUID, &uuid_arg, 0, &column_lens[7]);
    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLBindCol(m_statement, 9, SQL_C_TYPE_DATE, &date_arg, 0, &column_lens[8]);
    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLBindCol(m_statement, 10, SQL_C_TYPE_TIME, &time_arg, 0, &column_lens[9]);
    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLBindCol(m_statement, 11, SQL_C_TYPE_TIMESTAMP, &timestamp_arg, 0, &column_lens[10]);
    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLBindCol(m_statement, 12, SQL_C_NUMERIC, &decimal_arg, 0, &column_lens[11]);
    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    SQLCHAR request[] = "select str,int8,int16,int32,int64,\"FLOAT\",\"DOUBLE\",\"UUID\","
                        "\"DATE\",\"TIME\",\"DATETIME\",\"DECIMAL\" FROM TBL_ALL_COLUMNS_SQL ORDER BY key";

    ret = SQLExecDirect(m_statement, request, SQL_NTS);
    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    // Fetching the first non-null row.
    ret = SQLFetch(m_statement);
    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    // Checking that columns are not null.
    for (int i = 0; i < columns_cnt; ++i)
        EXPECT_NE(column_lens[i], SQL_NULL_DATA) << ", column idx = " << i;

    // Fetching null row.
    ret = SQLFetch(m_statement);
    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    // Checking that columns are null.
    for (int i = 0; i < columns_cnt; ++i)
        EXPECT_EQ(column_lens[i], SQL_NULL_DATA) << ", column idx = " << i;

    // Fetching the last non-null row.
    ret = SQLFetch(m_statement);
    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    // Checking that columns are not null.
    for (int i = 0; i < columns_cnt; ++i)
        EXPECT_NE(column_lens[i], SQL_NULL_DATA) << ", column idx = " << i;

    ret = SQLFetch(m_statement);
    EXPECT_EQ(ret, SQL_NO_DATA);
}

TEST_F(queries_test, insert_select) {
    odbc_connect(get_basic_connection_string());

    const int records_num = 100;

    // Inserting values.
    insert_test_strings(records_num);

    int64_t key = 0;
    char str[1024];
    SQLLEN str_len = 0;

    // Binding columns.
    SQLRETURN ret = SQLBindCol(m_statement, 1, SQL_C_SLONG, &key, 0, nullptr);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    // Binding columns.
    ret = SQLBindCol(m_statement, 2, SQL_C_CHAR, &str, sizeof(str), &str_len);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    // Just selecting everything to make sure everything is OK
    SQLCHAR select_req[] = "SELECT key, str FROM TBL_ALL_COLUMNS_SQL ORDER BY key";

    ret = SQLExecDirect(m_statement, select_req, sizeof(select_req));

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    int selected_records_num = 0;

    ret = SQL_SUCCESS;

    while (ret == SQL_SUCCESS) {
        ret = SQLFetch(m_statement);

        if (ret == SQL_NO_DATA)
            break;

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        std::string expected_str = get_test_string(selected_records_num);
        int64_t expected_key = selected_records_num + 1;

        EXPECT_EQ(key, expected_key);

        EXPECT_EQ(std::string(str, str_len), expected_str);

        ++selected_records_num;
    }

    EXPECT_EQ(records_num, selected_records_num);
}

TEST_F(queries_test, insert_update_select) {
    odbc_connect(get_basic_connection_string());

    const int records_num = 10;

    // Inserting values.
    insert_test_strings(records_num);

    std::int64_t key = 0;
    char str[1024];
    SQLLEN str_len = 0;

    SQLCHAR update_req[] = "UPDATE TBL_ALL_COLUMNS_SQL SET str = 'Updated value' WHERE key = 7";

    SQLRETURN ret = SQLExecDirect(m_statement, update_req, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLFreeStmt(m_statement, SQL_CLOSE);

    // Binding columns.
    ret = SQLBindCol(m_statement, 1, SQL_C_SLONG, &key, 0, nullptr);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    // Binding columns.
    ret = SQLBindCol(m_statement, 2, SQL_C_CHAR, &str, sizeof(str), &str_len);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    // Just selecting everything to make sure everything is OK
    SQLCHAR select_req[] = "SELECT key, str FROM TBL_ALL_COLUMNS_SQL ORDER BY key";

    ret = SQLExecDirect(m_statement, select_req, sizeof(select_req));

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    int selected_records_num = 0;

    ret = SQL_SUCCESS;

    while (ret == SQL_SUCCESS) {
        ret = SQLFetch(m_statement);

        if (ret == SQL_NO_DATA)
            break;

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        int64_t expected_key = selected_records_num + 1;
        std::string expected_str;

        EXPECT_EQ(key, expected_key);

        if (expected_key == 7)
            expected_str = "Updated value";
        else
            expected_str = get_test_string(selected_records_num);

        EXPECT_EQ(std::string(str, str_len), expected_str);

        ++selected_records_num;
    }

    EXPECT_EQ(records_num, selected_records_num);
}

TEST_F(queries_test, insert_delete_select) {
    odbc_connect(get_basic_connection_string());

    const int records_num = 100;

    // Inserting values.
    insert_test_strings(records_num);

    std::int64_t key = 0;
    char str[1024];
    SQLLEN str_len = 0;

    SQLCHAR update_req[] = "DELETE FROM TBL_ALL_COLUMNS_SQL WHERE (key % 2) = 1";

    SQLRETURN ret = SQLExecDirect(m_statement, update_req, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLFreeStmt(m_statement, SQL_CLOSE);

    // Binding columns.
    ret = SQLBindCol(m_statement, 1, SQL_C_SLONG, &key, 0, nullptr);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    // Binding columns.
    ret = SQLBindCol(m_statement, 2, SQL_C_CHAR, &str, sizeof(str), &str_len);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    // Just selecting everything to make sure everything is OK
    SQLCHAR select_req[] = "SELECT key, str FROM TBL_ALL_COLUMNS_SQL ORDER BY key";

    ret = SQLExecDirect(m_statement, select_req, sizeof(select_req));

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    int selected_records_num = 0;

    ret = SQL_SUCCESS;

    while (ret == SQL_SUCCESS) {
        ret = SQLFetch(m_statement);

        if (ret == SQL_NO_DATA)
            break;

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        std::int64_t expected_key = (selected_records_num + 1) * 2;
        std::string expected_str = get_test_string(int(expected_key) - 1);

        EXPECT_EQ(key, expected_key);
        EXPECT_EQ(std::string(str, str_len), expected_str);

        ++selected_records_num;
    }

    EXPECT_EQ(records_num / 2, selected_records_num);
}

TEST_F(queries_test, params_num) {
    odbc_connect(get_basic_connection_string());

    check_params_num("SELECT * FROM TBL_ALL_COLUMNS_SQL", 0);
    check_params_num("SELECT * FROM TBL_ALL_COLUMNS_SQL WHERE key=?", 1);
    check_params_num("SELECT * FROM TBL_ALL_COLUMNS_SQL WHERE key=? AND str=?", 2);
    check_params_num("INSERT INTO TBL_ALL_COLUMNS_SQL(key, str) VALUES(1, 'some')", 0);
    check_params_num("INSERT INTO TBL_ALL_COLUMNS_SQL(key, str) VALUES(?, ?)", 2);
}

TEST_F(queries_test, execute_after_cursor_close) {
    odbc_connect(get_basic_connection_string());

    exec_query("INSERT INTO TBL_ALL_COLUMNS_SQL(key, str) VALUES(1, 'str1')");

    std::int64_t key = 0;
    char str[1024];
    SQLLEN str_len = 0;

    // Binding columns.
    SQLRETURN ret = SQLBindCol(m_statement, 1, SQL_C_SLONG, &key, 0, nullptr);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    // Binding columns.
    ret = SQLBindCol(m_statement, 2, SQL_C_CHAR, &str, sizeof(str), &str_len);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    // Just selecting everything to make sure everything is OK
    SQLCHAR select_req[] = "SELECT key, str FROM TBL_ALL_COLUMNS_SQL";

    ret = SQLPrepare(m_statement, select_req, sizeof(select_req));

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLExecute(m_statement);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLFreeStmt(m_statement, SQL_CLOSE);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLExecute(m_statement);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLFetch(m_statement);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    EXPECT_EQ(key, 1);

    EXPECT_EQ(std::string(str, str_len), "str1");

    ret = SQLFetch(m_statement);

    EXPECT_EQ(ret, SQL_NO_DATA);
}

TEST_F(queries_test, close_non_full_fetch) {
    odbc_connect(get_basic_connection_string());

    exec_query("INSERT INTO TBL_ALL_COLUMNS_SQL(key, str) VALUES(1, 'str1')");
    exec_query("INSERT INTO TBL_ALL_COLUMNS_SQL(key, str) VALUES(2, 'str2')");

    std::int64_t key = 0;
    char str[1024];
    SQLLEN str_len = 0;

    // Binding columns.
    SQLRETURN ret = SQLBindCol(m_statement, 1, SQL_C_SLONG, &key, 0, nullptr);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    // Binding columns.
    ret = SQLBindCol(m_statement, 2, SQL_C_CHAR, &str, sizeof(str), &str_len);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    // Just selecting everything to make sure everything is OK
    SQLCHAR select_req[] = "SELECT key, str FROM TBL_ALL_COLUMNS_SQL ORDER BY key";

    ret = SQLExecDirect(m_statement, select_req, sizeof(select_req));

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLFetch(m_statement);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    EXPECT_EQ(key, 1);
    EXPECT_EQ(std::string(str, str_len), "str1");

    ret = SQLFreeStmt(m_statement, SQL_CLOSE);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));
}

TEST_F(queries_test, bind_null_parameter) {
    odbc_connect(get_basic_connection_string());

    SQLLEN paramInd = SQL_NULL_DATA;

    // Binding NULL parameter.
    SQLRETURN ret =
        SQLBindParameter(m_statement, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 100, 100, nullptr, 0, &paramInd);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    // Just selecting everything to make sure everything is OK
    SQLCHAR insertReq[] = "INSERT INTO TBL_ALL_COLUMNS_SQL(key, str) VALUES(1, ?)";

    ret = SQLExecDirect(m_statement, insertReq, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    // Un-binding parameter.
    ret = SQLFreeStmt(m_statement, SQL_RESET_PARAMS);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    // Selecting inserted column to make sure that everything is OK
    SQLCHAR select_req[] = "SELECT str FROM TBL_ALL_COLUMNS_SQL";

    char str[1024];
    SQLLEN str_len = 0;

    // Binding column.
    ret = SQLBindCol(m_statement, 1, SQL_C_CHAR, &str, sizeof(str), &str_len);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLExecDirect(m_statement, select_req, sizeof(select_req));

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLFetch(m_statement);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    EXPECT_EQ(str_len, SQL_NULL_DATA);
}

TEST_F(queries_test, affected_rows) {
    odbc_connect(get_basic_connection_string() + ";PAGE_SIZE=1024");

    const int records_num = 100;

    // Inserting values.
    insert_test_strings(records_num);

    SQLCHAR update_req[] = "UPDATE TBL_ALL_COLUMNS_SQL SET str = 'Updated value' WHERE key > 20 AND key < 40";

    SQLRETURN ret = SQLExecDirect(m_statement, update_req, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    SQLLEN affected = 0;
    ret = SQLRowCount(m_statement, &affected);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    EXPECT_EQ(affected, 19);

    ret = SQLFreeStmt(m_statement, SQL_CLOSE);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    // Just selecting everything to make sure everything is OK
    SQLCHAR select_req[] = "SELECT key, str FROM TBL_ALL_COLUMNS_SQL ORDER BY key";

    ret = SQLExecDirect(m_statement, select_req, sizeof(select_req));

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    affected = -1;
    ret = SQLRowCount(m_statement, &affected);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    EXPECT_EQ(affected, -1);
}

TEST_F(queries_test, affected_rows_on_select) {
    odbc_connect(get_basic_connection_string() + ";PAGE_SIZE=12");

    const int records_num = 30;

    // Inserting values.
    insert_test_strings(records_num);

    // Just selecting everything to make sure everything is OK
    SQLCHAR select_req[] = "SELECT key, str FROM TBL_ALL_COLUMNS_SQL ORDER BY key";

    SQLRETURN ret = SQLExecDirect(m_statement, select_req, sizeof(select_req));

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    for (int i = 0; i < records_num; ++i) {
        SQLLEN affected = -1;
        ret = SQLRowCount(m_statement, &affected);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        EXPECT_EQ(affected, -1);

        ret = SQLFetch(m_statement);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));
    }
}

// TODO: IGNITE-19855 Multiple queries execution is not supported.
#ifdef MUTED
TEST_F(queries_test, multiple_selects) {
    odbc_connect(get_basic_connection_string());

    const int stmtCnt = 10;

    std::stringstream stream;
    for (int i = 0; i < stmtCnt; ++i)
        stream << "select " << i << "; ";

    stream << '\0';

    std::string query0 = stream.str();
    std::vector<SQLCHAR> query(query0.begin(), query0.end());

    SQLRETURN ret = SQLExecDirect(m_statement, &query[0], SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    long res = 0;

    ret = SQLBindCol(m_statement, 1, SQL_C_SLONG, &res, 0, nullptr);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    for (long i = 0; i < stmtCnt; ++i) {
        ret = SQLFetch(m_statement);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        EXPECT_EQ(res, i);

        ret = SQLFetch(m_statement);

        EXPECT_EQ(ret, SQL_NO_DATA);

        ret = SQLMoreResults(m_statement);

        if (i < stmtCnt - 1 && !SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));
        else if (i == stmtCnt - 1)
            EXPECT_EQ(ret, SQL_NO_DATA);
    }
}

TEST_F(queries_test, multiple_mixed_statements) {
    odbc_connect(get_basic_connection_string());

    const int stmtCnt = 10;

    std::stringstream stream;
    for (int i = 0; i < stmtCnt; ++i)
        stream << "select " << i << "; insert into TBL_ALL_COLUMNS_SQL(key) values(" << i << "); ";

    stream << '\0';

    std::string query0 = stream.str();
    std::vector<SQLCHAR> query(query0.begin(), query0.end());

    SQLRETURN ret = SQLExecDirect(m_statement, &query[0], SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    long res = 0;

    ret = SQLBindCol(m_statement, 1, SQL_C_SLONG, &res, 0, nullptr);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    for (long i = 0; i < stmtCnt; ++i) {
        ret = SQLFetch(m_statement);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        EXPECT_EQ(res, i);

        ret = SQLFetch(m_statement);

        EXPECT_EQ(ret, SQL_NO_DATA);

        ret = SQLMoreResults(m_statement);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        SQLLEN affected = 0;
        ret = SQLRowCount(m_statement, &affected);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        EXPECT_EQ(affected, 1);

        ret = SQLFetch(m_statement);

        EXPECT_EQ(ret, SQL_NO_DATA);

        ret = SQLMoreResults(m_statement);

        if (i < stmtCnt - 1 && !SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));
        else if (i == stmtCnt - 1)
            EXPECT_EQ(ret, SQL_NO_DATA);
    }
}

TEST_F(queries_test, multiple_mixed_statements_no_fetch) {
    odbc_connect(get_basic_connection_string());

    const int stmtCnt = 10;

    std::stringstream stream;
    for (int i = 0; i < stmtCnt; ++i)
        stream << "select " << i << "; insert into TBL_ALL_COLUMNS_SQL(key) values(" << i << "); ";

    stream << '\0';

    std::string query0 = stream.str();
    std::vector<SQLCHAR> query(query0.begin(), query0.end());

    SQLRETURN ret = SQLExecDirect(m_statement, &query[0], SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    long res = 0;

    ret = SQLBindCol(m_statement, 1, SQL_C_SLONG, &res, 0, nullptr);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    for (long i = 0; i < stmtCnt; ++i) {
        ret = SQLMoreResults(m_statement);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        SQLLEN affected = 0;
        ret = SQLRowCount(m_statement, &affected);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        EXPECT_EQ(affected, 1);

        ret = SQLFetch(m_statement);

        EXPECT_EQ(ret, SQL_NO_DATA);

        ret = SQLMoreResults(m_statement);

        if (i < stmtCnt - 1 && !SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));
        else if (i == stmtCnt - 1)
            EXPECT_EQ(ret, SQL_NO_DATA);
    }
}
#endif // MUTED

TEST_F(queries_test, close_after_empty_update) {
    odbc_connect(get_basic_connection_string());

    SQLCHAR query[] = "update TBL_ALL_COLUMNS_SQL set str='test' where key=42";

    SQLRETURN ret = SQLExecDirect(m_statement, &query[0], SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

    ret = SQLFreeStmt(m_statement, SQL_CLOSE);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));
}

TEST_F(queries_test, max_min_select) {
    odbc_connect(get_basic_connection_string());

    auto ret = exec_query("DROP TABLE IF EXISTS PUBLIC.MIN_MAX_SELECT");
    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    ret = exec_query("CREATE TABLE PUBLIC.MIN_MAX_SELECT(ID INT PRIMARY KEY, CAPACITY INT NOT NULL)");
    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    ret = exec_query("INSERT INTO PUBLIC.MIN_MAX_SELECT(ID, CAPACITY) VALUES(1, 1)");
    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    ret = exec_query("SELECT MIN(CAPACITY), MAX(CAPACITY) FROM PUBLIC.MIN_MAX_SELECT");
    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    ret = SQLFetch(m_statement);
    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

    ret = SQLFreeStmt(m_statement, SQL_CLOSE);
    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);
}

TEST_F(queries_test, insert_select_batch_100) {
    odbc_connect(get_basic_connection_string());

    insert_batch_select(100);
}

TEST_F(queries_test, insert_select_batch_2047) {
    odbc_connect(get_basic_connection_string());

    insert_batch_select(500);
}

TEST_F(queries_test, test_non_full_batch_select_100_60) {
    odbc_connect(get_basic_connection_string());

    insert_non_full_batch_select(100, 60);
}

TEST_F(queries_test, test_non_full_batch_select_1000_239) {
    odbc_connect(get_basic_connection_string());

    insert_non_full_batch_select(1000, 239);
}
