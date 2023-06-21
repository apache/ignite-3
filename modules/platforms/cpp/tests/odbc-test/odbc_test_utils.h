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

#include "test_utils.h"

#include <gtest/gtest.h>

#include <memory>
#include <string_view>

#include <sql.h>
#include <sqlext.h>

#define ODBC_FAIL_ON_ERROR(ret, type, handle)           \
    if (!SQL_SUCCEEDED(ret))                            \
        FAIL() << get_odbc_error_message(type, handle)

namespace ignite {

constexpr size_t ODBC_BUFFER_SIZE = 1024;

/**
 * Get ODBC error state code.
 *
 * @param handle_type Type of the handle.
 * @param handle Handle.
 * @param idx Index of record to get.
 * @return Error state code.
 */
[[nodiscard]] inline std::string get_odbc_error_state(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT idx = 1)
{
    SQLCHAR sqlstate[7] = {};
    SQLINTEGER native_code;
    
    SQLCHAR message[ODBC_BUFFER_SIZE];
    SQLSMALLINT real_len = 0;

    SQLGetDiagRec(handle_type, handle, idx, sqlstate, &native_code, message, ODBC_BUFFER_SIZE, &real_len);

    return {reinterpret_cast<char*>(sqlstate)};
}

/**
 * Get ODBC error message.
 *
 * @param handle_type Type of the handle.
 * @param handle Handle.
 * @param idx Index of record to get.
 * @return Error message.
 */
[[nodiscard]] inline std::string get_odbc_error_message(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT idx = 1)
{
    SQLCHAR sqlstate[7] = {};
    SQLINTEGER native_code{};

    SQLCHAR message[ODBC_BUFFER_SIZE] = {};
    SQLSMALLINT real_len{};

    SQLGetDiagRec(handle_type, handle, idx, sqlstate, &native_code, message, ODBC_BUFFER_SIZE, &real_len);

    std::string res(reinterpret_cast<char*>(sqlstate));

    if (!res.empty())
        res.append(": ").append(reinterpret_cast<char*>(message), real_len);
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
    return {str.begin(), str.end()};
}

} // namespace ignite
