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

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <regex>
#include <string>

using namespace ignite;

/**
 * Test suite.
 */
class connection_test : public odbc_suite {};

TEST_F(connection_test, dbms_version) {
    EXPECT_NO_THROW(odbc_connect_throw(get_basic_connection_string()));

    SQLCHAR buffer[ODBC_BUFFER_SIZE];
    SQLSMALLINT resLen = 0;

    SQLRETURN ret = SQLGetInfo(m_conn, SQL_DBMS_VER, buffer, ODBC_BUFFER_SIZE, &resLen);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_DBC, m_conn));

    // Format: XX.XX.XXXX PATCH PRE_RELEASE
    EXPECT_TRUE(std::regex_match(
        std::string(reinterpret_cast<char *>(buffer)), std::regex(R"((\d\d\.\d\d\.\d\d\d\d)(\s\d+)?(\s[a-zA-Z]+)?)")));
}

TEST_F(connection_test, dbms_cluster_name) {
    EXPECT_NO_THROW(odbc_connect_throw(get_basic_connection_string()));

    SQLCHAR buffer[ODBC_BUFFER_SIZE];
    SQLSMALLINT resLen = 0;

    SQLRETURN ret = SQLGetInfo(m_conn, SQL_SERVER_NAME, buffer, ODBC_BUFFER_SIZE, &resLen);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_DBC, m_conn));

    // Test cluster name: see PlatformTestNodeRunner.
    EXPECT_EQ(std::string("cluster"), std::string(reinterpret_cast<char *>(buffer)));
}