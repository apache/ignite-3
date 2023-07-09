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
#   include <windows.h>
#endif

#include "ignite_runner.h"
#include "odbc_test_utils.h"
#include "test_utils.h"

#include <gtest/gtest.h>

#include <memory>
#include <string_view>

#include <sql.h>
#include <sqlext.h>

namespace ignite {

using namespace std::string_view_literals;

/**
 * Test suite.
 */
class odbc_suite : public ::testing::Test {
public:
    static constexpr std::string_view TABLE_1 = "tbl1"sv;
    static constexpr std::string_view TABLE_NAME_ALL_COLUMNS = "tbl_all_columns"sv;
    static constexpr std::string_view TABLE_NAME_ALL_COLUMNS_SQL = "tbl_all_columns_sql"sv;

    static constexpr const char *KEY_COLUMN = "key";
    static constexpr const char *VAL_COLUMN = "val";

    static inline const std::string DRIVER_NAME = "Apache Ignite 3";

    /**
     * Get node addresses to use for tests.
     *
     * @return Addresses.
     */
    static std::string get_nodes_address() {
        std::string res;
        for (const auto &addr : ignite_runner::get_node_addrs())
            res += addr + ',';

        return res;
    }

    /**
     * Get node addresses to use for tests.
     *
     * @return Addresses.
     */
    static std::string get_basic_connection_string() {
        return "driver={" + DRIVER_NAME + "};address=" + get_nodes_address() + ';';
    }

    /**
     * Prepare handles for connection.
     */
    void prepare_environment() {
        ignite::prepare_environment(m_env, m_conn);
    }

    /**
     * ODBC connect.
     *
     * @param connect_str Connect string.
     */
    void odbc_connect(std::string_view connect_str) {
        ignite::odbc_connect(connect_str, m_env, m_conn, m_statement);
    }

    /**
     * Disconnect.
     */
    void odbc_disconnect() {
        ignite::odbc_disconnect(m_conn, m_statement);
    }

    /**
     * Clean up handles.
     */
    void odbc_clean_up() {
        ignite::odbc_clean_up(m_env, m_conn, m_statement);
    }

    /**
     * Execute query.
     *
     * @param qry Query.
     * @return Result.
     */
    SQLRETURN exec_query(const std::string& qry) { // NOLINT(readability-make-member-function-const)
        auto sql = make_odbc_string(qry);
        return SQLExecDirect(m_statement, sql.data(), static_cast<SQLINTEGER>(sql.size()));
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
     * Get connection error state.
     *
     * @return Connection error state.
     */
    [[nodiscard]] std::string get_connection_error_state() const {
        return get_odbc_error_state(SQL_HANDLE_DBC, m_conn);
    }

    /** Environment handle. */
    SQLHENV m_env{SQL_NULL_HANDLE};

    /** Connection handle. */
    SQLHDBC m_conn{SQL_NULL_HANDLE};

    /** Statement handle. */
    SQLHSTMT m_statement{SQL_NULL_HANDLE};
};

} // namespace ignite
