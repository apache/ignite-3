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
     * Returns test std::int8_t value based on index.
     *
     * @param idx Index.
     * @return Test value.
     */
    [[nodiscard]] static std::int8_t get_test_i8(std::int64_t idx) { return static_cast<std::int8_t>(idx * 3); }

    /**
     * Checks std::int8_t value against given.
     *
     * @param idx Index.
     * @param val Value to test.
     */
    static void check_i8(int idx, std::int8_t value) { EXPECT_EQ(value, get_test_i8(idx)); }

    /**
     * Returns test std::int16_t value based on index.
     *
     * @param idx Index.
     * @return Test value.
     */
    [[nodiscard]] static std::int16_t get_test_i16(std::int64_t idx) { return static_cast<std::int16_t>(idx * 4); }

    /**
     * Checks std::int16_t value against given.
     *
     * @param idx Index.
     * @param val Value to test.
     */
    static void check_i16(int idx, std::int16_t value) { EXPECT_EQ(value, get_test_i16(idx)); }

    /**
     * Returns test std::int32_t value based on index.
     *
     * @param idx Index.
     * @return Test value.
     */
    [[nodiscard]] static std::int32_t get_test_i32(std::int64_t idx) { return static_cast<std::int32_t>(idx * 5); }

    /**
     * Checks std::int32_t value against given.
     *
     * @param idx Index.
     * @param val Value to test.
     */
    static void check_i32(int idx, std::int32_t value) { EXPECT_EQ(value, get_test_i32(idx)); }

    /**
     * Returns test std::int64_t value based on index.
     *
     * @param idx Index.
     * @return Test value.
     */
    [[nodiscard]] static std::int64_t get_test_i64(std::int64_t idx) { return static_cast<std::int64_t>(idx * 6); }

    /**
     * Checks std::int64_t value against given.
     *
     * @param idx Index.
     * @param val Value to test.
     */
    static void check_i64(int idx, std::int64_t value) { EXPECT_EQ(value, get_test_i64(idx)); }

    /**
     * Returns test SQLGUID value based on index.
     *
     * @param idx Index.
     * @return Test value.
     */
    [[nodiscard]] static SQLGUID get_test_uuid(int idx) {
        SQLGUID val;
        memset(&val, 0, sizeof(val));

        val.Data1 = idx * 9;
        val.Data2 = idx * 10;
        val.Data3 = idx * 11;

        return val;
    }

    /**
     * Checks SQLGUID value against given.
     *
     * @param idx Index.
     * @param val Value to test.
     */
    static void check_uuid(int idx, const SQLGUID &val) {
        SQLGUID expected = get_test_uuid(idx);

        EXPECT_EQ(val.Data1, expected.Data1);
        EXPECT_EQ(val.Data2, expected.Data2);
        EXPECT_EQ(val.Data3, expected.Data3);
    }

    /**
     * Returns test string value based on index.
     *
     * @param idx Index.
     * @return Test value.
     */
    [[nodiscard]] static std::string get_test_string(std::int64_t idx) { return std::to_string(idx * 2); }

    /**
     * Checks string value against given.
     *
     * @param idx Index.
     * @param val Value to test.
     */
    static void check_string(int idx, const std::string &value) { EXPECT_EQ(value, get_test_string(idx)); }

    /**
     * Returns test float value based on index.
     *
     * @param idx Index.
     * @return Test value.
     */
    [[nodiscard]] static float get_test_float(std::int64_t idx) { return static_cast<float>(idx * 7.0); }

    /**
     * Checks float value against given.
     *
     * @param idx Index.
     * @param val Value to test.
     */
    static void check_float(int idx, float value) { EXPECT_EQ(value, get_test_float(idx)); }

    /**
     * Returns test double value based on index.
     *
     * @param idx Index.
     * @return Test value.
     */
    [[nodiscard]] static double get_test_double(std::int64_t idx) { return static_cast<double>(idx * 8.0); }

    /**
     * Checks double value against given.
     *
     * @param idx Index.
     * @param val Value to test.
     */
    static void check_double(int idx, double value) { EXPECT_EQ(value, get_test_double(idx)); }

    /**
     * Returns test SQL_DATE_STRUCT value based on index.
     *
     * @param idx Index.
     * @return Test value.
     */
    [[nodiscard]] static SQL_DATE_STRUCT get_test_date(int64_t idx) {
        SQL_DATE_STRUCT val;
        memset(&val, 0, sizeof(val));

        val.year = static_cast<SQLSMALLINT>(2000 + idx);
        val.month = static_cast<SQLUSMALLINT>(1 + (idx % 12));
        val.day = static_cast<SQLUSMALLINT>(1 + (idx % 28));

        return val;
    }

    /**
     * Checks SQL_DATE_STRUCT value against given.
     *
     * @param idx Index.
     * @param val Value to test.
     */
    static void check_date(int idx, const SQL_DATE_STRUCT &val) {
        SQL_DATE_STRUCT expected = get_test_date(idx);

        EXPECT_EQ(val.year, expected.year);
        EXPECT_EQ(val.month, expected.month);
        EXPECT_EQ(val.day, expected.day);
    }

    /**
     * Returns test SQL_TIME_STRUCT value based on index.
     *
     * @param idx Index.
     * @return Test value.
     */
    [[nodiscard]] static SQL_TIME_STRUCT get_test_time(std::int64_t idx) {
        SQL_TIME_STRUCT val;
        memset(&val, 0, sizeof(val));

        val.hour = idx % 24;
        val.minute = idx % 60;
        val.second = idx % 60;

        return val;
    }

    /**
     * Checks SQL_TIME_STRUCT value against given.
     *
     * @param idx Index.
     * @param val Value to test.
     */
    static void check_time(int idx, const SQL_TIME_STRUCT &val) {
        SQL_TIME_STRUCT expected = get_test_time(idx);

        EXPECT_EQ(val.hour, expected.hour);
        EXPECT_EQ(val.minute, expected.minute);
        EXPECT_EQ(val.second, expected.second);
    }

    /**
     * Returns test SQL_TIMESTAMP_STRUCT value based on index.
     *
     * @param idx Index.
     * @return Test value.
     */
    [[nodiscard]] static SQL_TIMESTAMP_STRUCT get_test_timestamp(std::int64_t idx) {
        SQL_TIMESTAMP_STRUCT val;
        memset(&val, 0, sizeof(val));

        SQL_DATE_STRUCT date = get_test_date(idx + 1);
        SQL_TIME_STRUCT time = get_test_time(idx);

        val.year = date.year; // + 1 for old tests, make it distinguishable from the test date
        val.month = date.month;
        val.day = date.day;
        val.hour = time.hour;
        val.minute = time.minute;
        val.second = time.second;
        val.fraction = idx * 10000;

        return val;
    }

    /**
     * Checks SQL_TIMESTAMP_STRUCT value against given.
     *
     * @param idx Index.
     * @param val Value to test.
     */
    static void check_timestamp(int idx, const SQL_TIMESTAMP_STRUCT &val) {
        SQL_TIMESTAMP_STRUCT expected = get_test_timestamp(idx);

        EXPECT_EQ(val.year, expected.year);
        EXPECT_EQ(val.month, expected.month);
        EXPECT_EQ(val.day, expected.day);
        EXPECT_EQ(val.hour, expected.hour);
        EXPECT_EQ(val.minute, expected.minute);
        EXPECT_EQ(val.second, expected.second);
        EXPECT_EQ(val.fraction, expected.fraction);
    }

    /**
     * Returns test SQL_NUMERIC_STRUCT value based on index.
     *
     * @param idx Index.
     * @return Test value.
     */
    [[nodiscard]] static SQL_NUMERIC_STRUCT get_test_decimal(int idx) {
        SQL_NUMERIC_STRUCT val;
        memset(&val, 0, sizeof(val));

        val.sign = (idx % 2 == 0) ? 1 : 0;
        val.scale = 0;
        val.precision = 1;
        val.val[0] = (idx % 10);

        return val;
    }

    /**
     * Checks SQL_NUMERIC_STRUCT value against given.
     *
     * @param idx Index.
     * @param val Value to test.
     */
    static void check_decimal(int idx, const SQL_NUMERIC_STRUCT &val) {
        SQL_NUMERIC_STRUCT expected = get_test_decimal(idx);

        EXPECT_EQ(val.sign, expected.sign);
        EXPECT_EQ(val.scale, expected.scale);
        EXPECT_EQ(val.precision, expected.precision);
        EXPECT_EQ(val.val[0], expected.val[0]);
    }

    /**
     * Insert test strings.
     *
     * @param records_num Number of strings to insert.
     */
    void insert_test_strings(SQLSMALLINT records_num) const {
        SQLCHAR insert_req[] = "INSERT INTO TBL_ALL_COLUMNS_SQL(key, str) VALUES(?, ?)";

        SQLRETURN ret = SQLPrepare(m_statement, insert_req, SQL_NTS);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        std::int64_t key = 0;
        char str_field[1024] = {0};
        SQLLEN str_field_len = 0;

        // Binding parameters.
        ret = SQLBindParameter(m_statement, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT, 0, 0, &key, 0, nullptr);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = SQLBindParameter(m_statement, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, sizeof(str_field),
            sizeof(str_field), &str_field, sizeof(str_field), &str_field_len);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        // Inserting values.
        for (SQLSMALLINT i = 0; i < records_num; ++i) {
            key = i + 1;
            std::string val = get_test_string(i);

            strncpy(str_field, val.c_str(), sizeof(str_field) - 1);
            str_field_len = SQL_NTS;

            ret = SQLExecute(m_statement);
            ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

            SQLLEN affected = 0;
            ret = SQLRowCount(m_statement, &affected);
            ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

            EXPECT_EQ(affected, 1);

            ret = SQLMoreResults(m_statement);

            if (ret != SQL_NO_DATA)
                FAIL() << (get_odbc_error_message(SQL_HANDLE_STMT, m_statement));
        }

        // Resetting parameters.
        ret = SQLFreeStmt(m_statement, SQL_RESET_PARAMS);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);
    }

    /**
     * Inserts batch of rows with the given indices without insertion checks.
     *
     * @param from Start index.
     * @param to End index.
     */
    void insert_test_batch_no_check(int from, int to, SQLRETURN &ret) const {
        static const std::size_t STRING_FIELD_MAX_SIZE = 1024;

        SQLCHAR insert_req[] =
            "insert into TBL_ALL_COLUMNS_SQL("
            "   "
            "key,str,int8,int16,int32,int64,\"FLOAT\",\"DOUBLE\",\"UUID\",\"DATE\",\"TIME\",\"DATETIME\",\"DECIMAL\") "
            "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        ptrdiff_t records_num = to - from;

        ret = SQLSetStmtAttr(m_statement, SQL_ATTR_PARAMSET_SIZE, reinterpret_cast<SQLPOINTER>(records_num), 0);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = SQLSetStmtAttr(m_statement, SQL_ATTR_PARAM_BIND_TYPE, SQL_PARAM_BIND_BY_COLUMN, 0);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        auto keys = std::make_unique<std::int64_t[]>(records_num);
        auto str_fields = std::make_unique<char[]>(records_num * 1024);
        auto str_fields_len = std::make_unique<SQLLEN[]>(records_num);
        auto i8_fields = std::make_unique<std::int8_t[]>(records_num);
        auto i16_fields = std::make_unique<std::int16_t[]>(records_num);
        auto i32_fields = std::make_unique<std::int32_t[]>(records_num);
        auto i64_fields = std::make_unique<std::int64_t[]>(records_num);
        auto float_fields = std::make_unique<float[]>(records_num);
        auto double_fields = std::make_unique<double[]>(records_num);
        auto uuid_fields = std::make_unique<SQLGUID[]>(records_num);
        auto date_fields = std::make_unique<SQL_DATE_STRUCT[]>(records_num);
        auto time_fields = std::make_unique<SQL_TIME_STRUCT[]>(records_num);
        auto timestamp_fields = std::make_unique<SQL_TIMESTAMP_STRUCT[]>(records_num);
        auto decimal_fields = std::make_unique<SQL_NUMERIC_STRUCT[]>(records_num);

        for (int i = 0; i < records_num; ++i) {
            int seed = from + i;

            keys[i] = seed;
            i8_fields[i] = get_test_i8(seed);
            i16_fields[i] = get_test_i16(seed);
            i32_fields[i] = get_test_i32(seed);
            i64_fields[i] = get_test_i64(seed);
            float_fields[i] = get_test_float(seed);
            double_fields[i] = get_test_double(seed);
            uuid_fields[i] = get_test_uuid(seed);

            date_fields[i] = get_test_date(seed);
            time_fields[i] = get_test_time(seed);
            timestamp_fields[i] = get_test_timestamp(seed);
            decimal_fields[i] = get_test_decimal(seed);

            std::string val = get_test_string(seed);
            strncpy(str_fields.get() + i * STRING_FIELD_MAX_SIZE, val.c_str(), STRING_FIELD_MAX_SIZE - 1);
            str_fields_len[i] = val.size();
        }

        ret = SQLBindParameter(m_statement, 1, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, keys.get(), 0, 0);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = SQLBindParameter(m_statement, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, STRING_FIELD_MAX_SIZE, 0,
            str_fields.get(), STRING_FIELD_MAX_SIZE, str_fields_len.get());
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret =
            SQLBindParameter(m_statement, 3, SQL_PARAM_INPUT, SQL_C_STINYINT, SQL_TINYINT, 0, 0, i8_fields.get(), 0, 0);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret =
            SQLBindParameter(m_statement, 4, SQL_PARAM_INPUT, SQL_C_SSHORT, SQL_SMALLINT, 0, 0, i16_fields.get(), 0, 0);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = SQLBindParameter(m_statement, 5, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 0, 0, i32_fields.get(), 0, 0);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret =
            SQLBindParameter(m_statement, 6, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, i64_fields.get(), 0, 0);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = SQLBindParameter(m_statement, 7, SQL_PARAM_INPUT, SQL_C_FLOAT, SQL_FLOAT, 0, 0, float_fields.get(), 0, 0);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = SQLBindParameter(
            m_statement, 8, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, 0, 0, double_fields.get(), 0, 0);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = SQLBindParameter(m_statement, 9, SQL_PARAM_INPUT, SQL_C_GUID, SQL_GUID, 0, 0, uuid_fields.get(), 0, 0);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = SQLBindParameter(
            m_statement, 10, SQL_PARAM_INPUT, SQL_C_TYPE_DATE, SQL_TYPE_DATE, 0, 0, date_fields.get(), 0, 0);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = SQLBindParameter(
            m_statement, 11, SQL_PARAM_INPUT, SQL_C_TYPE_TIME, SQL_TYPE_TIME, 0, 0, time_fields.get(), 0, 0);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = SQLBindParameter(m_statement, 12, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP, SQL_TYPE_TIMESTAMP, 0, 0,
            timestamp_fields.get(), 0, 0);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = SQLBindParameter(
            m_statement, 13, SQL_PARAM_INPUT, SQL_C_NUMERIC, SQL_DECIMAL, 0, 0, decimal_fields.get(), 0, nullptr);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = SQLPrepare(m_statement, insert_req, SQL_NTS);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = SQLExecute(m_statement);
    }

    void insert_test_batch(int from, int to, int expected_to_affect) const {
        SQLULEN sets_processed = 0;

        insert_test_batch(from, to, expected_to_affect, sets_processed);

        EXPECT_EQ(sets_processed, expected_to_affect);
    }

    /**
     * Inserts batch of rows with the given indices and checks affected rows.
     *
     * @param from Start index.
     * @param to End index.
     * @param expected_to_affect Expected number of affected rows.
     */
    void insert_test_batch(int from, int to, int expected_to_affect, SQLULEN& sets_processed) const {
        SQLRETURN ret = SQLSetStmtAttr(m_statement, SQL_ATTR_PARAMS_PROCESSED_PTR, &sets_processed, 0);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        insert_test_batch_no_check(from, to, ret);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        SQLLEN totally_affected = 0;

        do {
            SQLLEN affected = 0;
            ret = SQLRowCount(m_statement, &affected);
            ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

            totally_affected += affected;

            ret = SQLMoreResults(m_statement);

            ODBC_FAIL_ON_ERROR_OR_NE(ret, SQL_NO_DATA, SQL_HANDLE_STMT, m_statement);
        } while (ret != SQL_NO_DATA);

        EXPECT_EQ(totally_affected, expected_to_affect);

        ret = SQLFreeStmt(m_statement, SQL_RESET_PARAMS);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = SQLSetStmtAttr(m_statement, SQL_ATTR_PARAMSET_SIZE, reinterpret_cast<SQLPOINTER>(1), 0);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        EXPECT_EQ(sets_processed, expected_to_affect);
    }

    void select_batch(int records_num) const {
        int64_t key = 0;
        char str_field[1024] = {0};
        SQLLEN str_field_len = 0;

        // Binding columns.
        SQLRETURN ret = SQLBindCol(m_statement, 1, SQL_C_SLONG, &key, 0, 0);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        // Binding columns.
        ret = SQLBindCol(m_statement, 2, SQL_C_CHAR, &str_field, sizeof(str_field), &str_field_len);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        // Just selecting everything to make sure everything is OK
        SQLCHAR select_req[] = "SELECT key, str FROM TBL_ALL_COLUMNS_SQL ORDER BY key";

        ret = SQLExecDirect(m_statement, select_req, sizeof(select_req));
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        int selected_records_num = 0;

        ret = SQL_SUCCESS;

        while (ret == SQL_SUCCESS) {
            ret = SQLFetch(m_statement);

            if (ret == SQL_NO_DATA)
                break;

            ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

            check_string(selected_records_num, std::string(str_field, str_field_len));
            EXPECT_EQ(key, selected_records_num);

            ++selected_records_num;
        }

        EXPECT_EQ(records_num, selected_records_num);
    }

    /**
     * Inserts batch of rows with the given indices and checks rows with the select query.
     *
     * @param records_num Count of rows.
     */
    void insert_batch_select(int records_num) {
        // Inserting values.
        SQLULEN sets_processed = 0;
        insert_test_batch(0, records_num, records_num, sets_processed);

        EXPECT_EQ(sets_processed, records_num);

        select_batch(records_num);
    }

    /**
     * Inserts two batches of rows with the given indices and checks rows with the select query.
     *
     * @param records_num Count of rows.
     * @param split_at Index to split rows between queries.
     */
    void insert_non_full_batch_select(int records_num, int split_at) {
        SQLULEN sets_processed = 0;
        std::vector<SQLUSMALLINT> statuses(records_num);

        // Binding statuses array.
        SQLRETURN ret = SQLSetStmtAttr(m_statement, SQL_ATTR_PARAM_STATUS_PTR, &statuses[0], 0);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        // Inserting values.
        insert_test_batch(split_at, records_num, records_num - split_at, sets_processed);

        EXPECT_EQ(sets_processed, records_num - split_at);

        // Check the statuses which will be written from index 0.
        for (int i = 0; i < records_num - split_at; ++i)
            EXPECT_EQ(statuses[i], SQL_PARAM_SUCCESS) << "index=" << i;

        insert_test_batch_no_check(0, records_num, ret);

        // Expect to affect the remaining split_at rows.
        EXPECT_EQ(sets_processed, split_at);

        // Check the statuses for the whole batch. We expect that first [split_at] rows should be written successfully
        // and next [records_num - split_at] should not, because we have already written these lines before.
        for (int i = 0; i < split_at; ++i)
            EXPECT_EQ(statuses[i], SQL_PARAM_SUCCESS) << "index=" << i;

        for (int i = split_at; i < records_num; ++i)
            EXPECT_EQ(statuses[i], SQL_PARAM_ERROR) << "index=" << i;

        SQLFreeStmt(m_statement, SQL_RESET_PARAMS);
        SQLFreeStmt(m_statement, SQL_UNBIND);

        select_batch(records_num);
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
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        std::int64_t key_arg = idx;
        std::string str_arg_val = get_test_string(idx);
        auto int8_arg = get_test_i8(idx);
        auto int16_arg = get_test_i16(idx);
        auto int32_arg = get_test_i32(idx);
        auto int64_arg = get_test_i64(idx);
        auto float_arg = get_test_float(idx);
        auto double_arg = get_test_double(idx);
        SQLGUID uuid_arg = get_test_uuid(idx);
        SQL_DATE_STRUCT date_arg = get_test_date(idx);
        SQL_TIME_STRUCT time_arg = get_test_time(idx);
        SQL_TIMESTAMP_STRUCT datetime_arg = get_test_timestamp(idx);
        SQL_NUMERIC_STRUCT decimal_arg = get_test_decimal(idx);

        auto str_field = to_sqlchar(str_arg_val);
        auto str_field_len = SQLLEN(str_field.size());

        // Binding parameters.
        ret = SQLBindParameter(m_statement, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT, 0, 0, &key_arg, 0, nullptr);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = SQLBindParameter(m_statement, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, str_field.size(), 0,
            str_field.data(), str_field_len, &str_field_len);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret =
            SQLBindParameter(m_statement, 3, SQL_PARAM_INPUT, SQL_C_STINYINT, SQL_TINYINT, 0, 0, &int8_arg, 0, nullptr);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret =
            SQLBindParameter(m_statement, 4, SQL_PARAM_INPUT, SQL_C_SSHORT, SQL_SMALLINT, 0, 0, &int16_arg, 0, nullptr);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = SQLBindParameter(m_statement, 5, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 0, 0, &int32_arg, 0, nullptr);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret =
            SQLBindParameter(m_statement, 6, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &int64_arg, 0, nullptr);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = SQLBindParameter(m_statement, 7, SQL_PARAM_INPUT, SQL_C_FLOAT, SQL_FLOAT, 0, 0, &float_arg, 0, nullptr);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret =
            SQLBindParameter(m_statement, 8, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, 0, 0, &double_arg, 0, nullptr);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = SQLBindParameter(m_statement, 9, SQL_PARAM_INPUT, SQL_C_GUID, SQL_GUID, 0, 0, &uuid_arg, 0, nullptr);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = SQLBindParameter(m_statement, 10, SQL_PARAM_INPUT, SQL_C_DATE, SQL_DATE, 0, 0, &date_arg, 0, nullptr);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = SQLBindParameter(m_statement, 11, SQL_PARAM_INPUT, SQL_C_TIME, SQL_TIME, 0, 0, &time_arg, 0, nullptr);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = SQLBindParameter(
            m_statement, 12, SQL_PARAM_INPUT, SQL_C_TIMESTAMP, SQL_TIMESTAMP, 0, 0, &datetime_arg, 0, nullptr);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = SQLBindParameter(
            m_statement, 13, SQL_PARAM_INPUT, SQL_C_NUMERIC, SQL_DECIMAL, 0, 0, &decimal_arg, 0, nullptr);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        ret = SQLExecute(m_statement);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        SQLLEN affected = 0;
        ret = SQLRowCount(m_statement, &affected);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);

        EXPECT_EQ(affected, 1);

        // Resetting parameters.
        ret = SQLFreeStmt(m_statement, SQL_RESET_PARAMS);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, m_statement);
    }

    /** Environment handle. */
    SQLHENV m_env{SQL_NULL_HANDLE};

    /** Connection handle. */
    SQLHDBC m_conn{SQL_NULL_HANDLE};

    /** Statement handle. */
    SQLHSTMT m_statement{SQL_NULL_HANDLE};
};

} // namespace ignite
