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

#ifdef _WIN32
# include <windows.h>
#endif

#include "odbc_test_utils.h"
#include "test_utils.h"

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string_view>
#include <thread>
#include <utility>

#include <sql.h>
#include <sqlext.h>

namespace ignite {

/**
 * Test suite.
 */
class odbc_connection {
public:
    /**
     * Destructor.
     */
    ~odbc_connection() { odbc_clean_up(); }

    /**
     * Prepare handles for connection.
     */
    void prepare_environment() { ignite::prepare_environment(m_env, m_conn); }

    /**
     * ODBC connect.
     *
     * @param connect_str Connect string.
     * @throws ignite_error on connection failed.
     */
    void odbc_connect_throw(std::string_view connect_str) {
        ignite::odbc_connect(connect_str, m_env, m_conn, m_statement);
    }

    /**
     * ODBC connect.
     *
     * @param connect_str Connect string.
     */
    void odbc_connect(std::string_view connect_str) {
        try {
            odbc_connect_throw(connect_str);
        } catch (const ignite_error &error) {
            FAIL() << error.what();
        }
    }

    /**
     * Disconnect.
     */
    void odbc_disconnect() { ignite::odbc_disconnect(m_conn, m_statement); }

    /**
     * Clean up handles.
     */
    void odbc_clean_up() { ignite::odbc_clean_up(m_env, m_conn, m_statement); }

    /**
     * Execute query.
     *
     * @param qry Query.
     * @return Result.
     */
    SQLRETURN exec_query(const std::string &qry) { // NOLINT(readability-make-member-function-const)
        auto sql = to_sqlchar(qry);
        return SQLExecDirect(m_statement, sql.data(), static_cast<SQLINTEGER>(sql.size()));
    }

    /**
     * Prepare query.
     *
     * @param qry Query.
     * @return Result.
     */
    SQLRETURN prepare_query(const std::string &qry) { // NOLINT(readability-make-member-function-const)
        auto sql = to_sqlchar(qry);
        return SQLPrepare(m_statement, sql.data(), static_cast<SQLINTEGER>(sql.size()));
    }

    /**
     * Make a certain number of retry of operation while it fails
     *
     * @param func Function.
     * @param attempts Attempts number.
     * @return @c true on success.
     */
    bool retry_on_fail(std::function<SQLRETURN()> func, int attempts = 5) {
        for (int i = 0; i < attempts; ++i) {
            auto res = func();
            if (SQL_SUCCEEDED(res))
                return true;
        }
        return false;
    }

    /**
     * Get statement error state.
     *
     * @return Statement error state.
     */
    [[nodiscard]] std::string get_statement_error_state() const {
        return get_odbc_error_state(SQL_HANDLE_STMT, m_statement);
    }

    /**
     * Get statement error message.
     *
     * @return Statement error message.
     */
    [[nodiscard]] std::string get_statement_error_message() const {
        return get_odbc_error_message(SQL_HANDLE_STMT, m_statement);
    }

    /**
     * Get connection error state.
     *
     * @return Connection error state.
     */
    [[nodiscard]] std::string get_connection_error_state() const {
        return get_odbc_error_state(SQL_HANDLE_DBC, m_conn);
    }

    /**
     * Wait for table to become available.
     *
     * @param table Table name to wait.
     * @param timeout Timeout.
     * @return @c true if table is available, @c false on timeout.
     */
    bool wait_for_table(const std::string &table, std::chrono::seconds timeout) {
        auto start_time = std::chrono::steady_clock::now();
        do {
            auto res = exec_query("select * from " + table);
            if (SQL_SUCCEEDED(res))
                return true;

            std::this_thread::sleep_for(std::chrono::seconds(1));
        } while ((std::chrono::steady_clock::now() - start_time) < timeout);
        return false;
    }

    /**
     * Generate test string for the index.
     *
     * @param idx Index.
     * @return Test string.
     */
    [[nodiscard]] static std::string get_test_string(int idx) { return "String#" + std::to_string(idx); }

    /**
     * Insert test strings.
     *
     * @param records_num Number of strings to insert.
     */
    void insert_test_strings(SQLSMALLINT records_num) const {
        SQLCHAR insert_req[] = "INSERT INTO TBL_ALL_COLUMNS_SQL(key, str) VALUES(?, ?)";

        SQLRETURN ret = SQLPrepare(m_statement, insert_req, SQL_NTS);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        std::int64_t key = 0;
        char str_field[1024] = {0};
        SQLLEN str_field_len = 0;

        // Binding parameters.
        ret = SQLBindParameter(m_statement, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT, 0, 0, &key, 0, nullptr);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        ret = SQLBindParameter(m_statement, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, sizeof(str_field),
            sizeof(str_field), &str_field, sizeof(str_field), &str_field_len);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        // Inserting values.
        for (SQLSMALLINT i = 0; i < records_num; ++i) {
            key = i + 1;
            std::string val = get_test_string(i);

            strncpy(str_field, val.c_str(), sizeof(str_field) - 1);
            str_field_len = SQL_NTS;

            ret = SQLExecute(m_statement);

            if (!SQL_SUCCEEDED(ret))
                FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement)) << ", step " << i;

            SQLLEN affected = 0;
            ret = SQLRowCount(m_statement, &affected);

            if (!SQL_SUCCEEDED(ret))
                FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

            EXPECT_EQ(affected, 1);

            ret = SQLMoreResults(m_statement);

            if (ret != SQL_NO_DATA)
                FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));
        }

        // Resetting parameters.
        ret = SQLFreeStmt(m_statement, SQL_RESET_PARAMS);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));
    }

    /**
     * Insert all types row.
     *
     * @param idx Index.
     */
    void insert_all_types_row(std::int16_t idx) const {
        SQLCHAR insert_req[] =
            "insert into TBL_ALL_COLUMNS_SQL("
            "   "
            "key,str,int8,int16,int32,int64,\"FLOAT\",\"DOUBLE\",\"UUID\",\"DATE\",\"TIME\",\"DATETIME\",\"DECIMAL\") "
            "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        SQLRETURN ret = SQLPrepare(m_statement, insert_req, SQL_NTS);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        std::int64_t key_arg = idx;
        std::string str_arg_val = std::to_string(idx * 2);
        auto int8_arg = std::int8_t(idx * 3);
        auto int16_arg = std::int16_t(idx * 4);
        auto int32_arg = std::int32_t(idx * 5);
        auto int64_arg = std::int64_t(idx * 6);
        auto float_arg = float(idx * 7.0);
        auto double_arg = double(idx * 8.0);

        SQLGUID uuid_arg;
        memset(&uuid_arg, 0, sizeof(uuid_arg));

        uuid_arg.Data1 = idx * 9;
        uuid_arg.Data2 = idx * 10;
        uuid_arg.Data3 = idx * 11;

        SQL_DATE_STRUCT date_arg;
        memset(&date_arg, 0, sizeof(date_arg));

        date_arg.year = SQLSMALLINT(2000 + idx);
        date_arg.month = 1 + (idx % 12);
        date_arg.day = 1 + (idx % 28);

        SQL_TIME_STRUCT time_arg;
        memset(&time_arg, 0, sizeof(time_arg));

        time_arg.hour = idx % 24;
        time_arg.minute = idx % 60;
        time_arg.second = idx % 60;

        SQL_TIMESTAMP_STRUCT datetime_arg;
        memset(&datetime_arg, 0, sizeof(datetime_arg));

        datetime_arg.year = SQLSMALLINT(2001 + idx);
        datetime_arg.month = 1 + (idx % 12 + 1);
        datetime_arg.day = 1 + (idx % 28 + 1);
        datetime_arg.hour = idx % 24;
        datetime_arg.minute = idx % 60;
        datetime_arg.second = idx % 60;
        datetime_arg.fraction = idx * 10000;

        SQL_NUMERIC_STRUCT decimal_arg;
        memset(&decimal_arg, 0, sizeof(decimal_arg));

        decimal_arg.sign = (idx % 2 == 0) ? 1 : 0;
        decimal_arg.scale = 0;
        decimal_arg.precision = 1;
        decimal_arg.val[0] = (idx % 10);

        auto str_field = to_sqlchar(str_arg_val);
        auto str_field_len = SQLLEN(str_field.size());

        // Binding parameters.
        ret = SQLBindParameter(m_statement, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT, 0, 0, &key_arg, 0, nullptr);
        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        ret = SQLBindParameter(m_statement, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, str_field.size(), 0,
            str_field.data(), str_field_len, &str_field_len);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        ret =
            SQLBindParameter(m_statement, 3, SQL_PARAM_INPUT, SQL_C_STINYINT, SQL_TINYINT, 0, 0, &int8_arg, 0, nullptr);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        ret =
            SQLBindParameter(m_statement, 4, SQL_PARAM_INPUT, SQL_C_SSHORT, SQL_SMALLINT, 0, 0, &int16_arg, 0, nullptr);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        ret = SQLBindParameter(m_statement, 5, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 0, 0, &int32_arg, 0, nullptr);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        ret =
            SQLBindParameter(m_statement, 6, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &int64_arg, 0, nullptr);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        ret = SQLBindParameter(m_statement, 7, SQL_PARAM_INPUT, SQL_C_FLOAT, SQL_FLOAT, 0, 0, &float_arg, 0, nullptr);
        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        ret =
            SQLBindParameter(m_statement, 8, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, 0, 0, &double_arg, 0, nullptr);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        ret = SQLBindParameter(m_statement, 9, SQL_PARAM_INPUT, SQL_C_GUID, SQL_GUID, 0, 0, &uuid_arg, 0, nullptr);
        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        ret = SQLBindParameter(m_statement, 10, SQL_PARAM_INPUT, SQL_C_DATE, SQL_DATE, 0, 0, &date_arg, 0, nullptr);
        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        ret = SQLBindParameter(m_statement, 11, SQL_PARAM_INPUT, SQL_C_TIME, SQL_TIME, 0, 0, &time_arg, 0, nullptr);
        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        ret = SQLBindParameter(
            m_statement, 12, SQL_PARAM_INPUT, SQL_C_TIMESTAMP, SQL_TIMESTAMP, 0, 0, &datetime_arg, 0, nullptr);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        ret = SQLBindParameter(
            m_statement, 13, SQL_PARAM_INPUT, SQL_C_NUMERIC, SQL_DECIMAL, 0, 0, &decimal_arg, 0, nullptr);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        ret = SQLExecute(m_statement);
        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        SQLLEN affected = 0;
        ret = SQLRowCount(m_statement, &affected);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));

        EXPECT_EQ(affected, 1);

        // Resetting parameters.
        ret = SQLFreeStmt(m_statement, SQL_RESET_PARAMS);

        if (!SQL_SUCCEEDED(ret))
            FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));
    }

    /** Environment handle. */
    SQLHENV m_env{SQL_NULL_HANDLE};

    /** Connection handle. */
    SQLHDBC m_conn{SQL_NULL_HANDLE};

    /** Statement handle. */
    SQLHSTMT m_statement{SQL_NULL_HANDLE};
};

} // namespace ignite
