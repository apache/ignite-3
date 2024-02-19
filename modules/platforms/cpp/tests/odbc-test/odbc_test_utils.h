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

#include "test_utils.h"

#include <gtest/gtest.h>

#include <memory>
#include <string_view>

#include <sql.h>
#include <sqlext.h>

#define ODBC_FAIL_ON_ERROR(ret, type, handle)                                                                          \
 if (!SQL_SUCCEEDED(ret))                                                                                              \
 FAIL() << get_odbc_error_message(type, handle)

#define ODBC_FAIL_ON_ERROR_OR_NE(ret, expected, type, handle)                                                          \
 if (!SQL_SUCCEEDED(ret) && (ret) != (expected))                                                                       \
 FAIL() << get_odbc_error_message(type, handle)

#define ODBC_THROW_ON_ERROR(ret, type, handle)                                                                         \
 if (!SQL_SUCCEEDED(ret))                                                                                              \
  throw odbc_exception {                                                                                               \
   get_odbc_error_message(type, handle), get_odbc_error_state(type, handle)                                            \
  }

namespace ignite {

/**
 * Utility error type for testing.
 */
struct odbc_exception : public std::exception {
    /**
     * Constructor.
     */
    odbc_exception(std::string message, std::string sql_state)
        : message(std::move(message))
        , sql_state(std::move(sql_state)) {}

    /** Message. */
    std::string message;

    /** SQL state. */
    std::string sql_state;

    [[nodiscard]] char const *what() const noexcept override { return message.c_str(); }
};

constexpr size_t ODBC_BUFFER_SIZE = 1024;

/**
 * Get ODBC error state code.
 *
 * @param handle_type Type of the handle.
 * @param handle Handle.
 * @param idx Index of record to get.
 * @return Error state code.
 */
[[nodiscard]] inline std::string get_odbc_error_state(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT idx = 1) {
    SQLCHAR sqlstate[7] = {};
    SQLINTEGER native_code;

    SQLCHAR message[ODBC_BUFFER_SIZE];
    SQLSMALLINT real_len = 0;

    SQLGetDiagRec(handle_type, handle, idx, sqlstate, &native_code, message, ODBC_BUFFER_SIZE, &real_len);

    return {reinterpret_cast<char *>(sqlstate)};
}

/**
 * Get ODBC error message.
 *
 * @param handle_type Type of the handle.
 * @param handle Handle.
 * @param idx Index of record to get.
 * @return Error message.
 */
[[nodiscard]] inline std::string get_odbc_error_message(
    SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT idx = 1) {
    SQLCHAR sqlstate[7] = {};
    SQLINTEGER native_code{};

    SQLCHAR message[ODBC_BUFFER_SIZE] = {};
    SQLSMALLINT real_len{};

    SQLGetDiagRec(handle_type, handle, idx, sqlstate, &native_code, message, ODBC_BUFFER_SIZE, &real_len);

    std::string res(reinterpret_cast<char *>(sqlstate));

    if (!res.empty())
        res.append(": ").append(reinterpret_cast<char *>(message), real_len);
    else
        res = "No results";

    return res;
}

/**
 * Convert string to SQLCHAR array.
 * @param str String.
 * @return SQLCHAR vector.
 */
[[nodiscard]] inline std::vector<SQLCHAR> to_sqlchar(std::string_view str) {
    std::vector<SQLCHAR> res{str.begin(), str.end()};
    res.push_back(0);
    return res;
}

/**
 * Prepare handles for connection.
 *
 * @param env Environment handle.
 * @param conn Connection handle.
 */
inline void prepare_environment(SQLHENV &env, SQLHDBC &conn) {
    // Allocate an environment handle
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

    EXPECT_TRUE(env != SQL_NULL_HANDLE);

    // We want ODBC 3.8 support
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void *>(SQL_OV_ODBC3_80), 0);

    // Allocate a connection handle
    SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

    EXPECT_TRUE(conn != SQL_NULL_HANDLE);
}

/**
 * ODBC connect.
 *
 * @param connect_str Connect string.
 * @param env Environment handle.
 * @param conn Connection handle.
 * @param stmt Statement handle.
 */
inline void odbc_connect(std::string_view connect_str, SQLHENV &env, SQLHDBC &conn, SQLHSTMT &stmt) {
    prepare_environment(env, conn);

    // Connect string
    auto connect_str0 = to_sqlchar(connect_str);

    SQLCHAR out_str[ODBC_BUFFER_SIZE];
    SQLSMALLINT out_str_len;

    // Connecting to ODBC server.
    SQLRETURN ret = SQLDriverConnect(conn, nullptr, &connect_str0[0], static_cast<SQLSMALLINT>(connect_str0.size()),
        out_str, sizeof(out_str), &out_str_len, SQL_DRIVER_COMPLETE);

    if (!SQL_SUCCEEDED(ret)) {
        throw ignite_error(get_odbc_error_message(SQL_HANDLE_DBC, conn));
    }

    // Allocate a statement handle
    SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);

    EXPECT_TRUE(stmt != SQL_NULL_HANDLE);
}

/**
 * Disconnect.
 * @param conn Connection handle.
 * @param stmt Statement handle.
 */
inline void odbc_disconnect(SQLHDBC &conn, SQLHSTMT &stmt) {
    if (stmt) {
        // Releasing the statement handle.
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        stmt = SQL_NULL_HANDLE;
    }

    if (conn) {
        // Disconnecting from the server.
        SQLDisconnect(conn);

        // Releasing allocated handles.
        SQLFreeHandle(SQL_HANDLE_DBC, conn);
        conn = SQL_NULL_HANDLE;
    }
}

/**
 * Clean up handles.
 * @param env Environment handle.
 * @param conn Connection handle.
 * @param stmt Statement handle.
 */
inline void odbc_clean_up(SQLHENV &env, SQLHDBC &conn, SQLHSTMT &stmt) {
    odbc_disconnect(conn, stmt);

    if (env) {
        // Releasing allocated handles.
        SQLFreeHandle(SQL_HANDLE_ENV, env);
        env = SQL_NULL_HANDLE;
    }
}

} // namespace ignite
