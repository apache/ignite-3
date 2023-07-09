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

#include "odbc_test_utils.h"
#include "test_utils.h"

#include <gtest/gtest.h>

#include <memory>
#include <string_view>
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
     * Constructor.
     *
     * @param connect_str Connection string.
     */
    explicit odbc_connection(std::string connect_str)
        : m_connect_str(std::move(connect_str)) {}

    ~odbc_connection() {
        ignite::odbc_clean_up(m_env, m_conn, m_statement);
    }

    /**
     * Prepare handles for connection.
     */
    void connect() {
        ignite::odbc_connect(m_connect_str, m_env, m_conn, m_statement);
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

    /** Connection string. */
    std::string m_connect_str;

    /** Environment handle. */
    SQLHENV m_env{SQL_NULL_HANDLE};

    /** Connection handle. */
    SQLHDBC m_conn{SQL_NULL_HANDLE};

    /** Statement handle. */
    SQLHSTMT m_statement{SQL_NULL_HANDLE};
};

} // namespace ignite
