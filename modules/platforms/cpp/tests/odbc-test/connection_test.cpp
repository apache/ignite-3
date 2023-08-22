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
 * Test suite.
 */
class connection_test : public ignite::odbc_suite {};

TEST_F(connection_test, connection_success) {
    odbc_connect(get_basic_connection_string());
}

TEST_F(connection_test, odbc3_supported) {
    // Allocate an environment handle
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &m_env);

    EXPECT_TRUE(m_env != SQL_NULL_HANDLE);

    // We want ODBC 3.8 support
    SQLSetEnvAttr(m_env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void *>(SQL_OV_ODBC3), 0);

    // Allocate a connection handle
    SQLAllocHandle(SQL_HANDLE_DBC, m_env, &m_conn);

    EXPECT_TRUE(m_conn != SQL_NULL_HANDLE);

    // Connect string
    auto connect_str0 = to_sqlchar(get_basic_connection_string());

    SQLCHAR out_str[ODBC_BUFFER_SIZE];
    SQLSMALLINT out_str_len;

    // Connecting to ODBC server.
    SQLRETURN ret = SQLDriverConnect(m_conn, nullptr, &connect_str0[0], static_cast<SQLSMALLINT>(connect_str0.size()),
        out_str, sizeof(out_str), &out_str_len, SQL_DRIVER_COMPLETE);

    if (!SQL_SUCCEEDED(ret)) {
        FAIL() << get_odbc_error_message(SQL_HANDLE_DBC, m_conn);
    }
}
