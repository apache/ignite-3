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

#ifdef _WIN32
# include <windows.h>
#endif

#include <sql.h>
#include <sqlext.h>

#include "configuration.h"
#include "connection_info.h"
#include "ignite/odbc/common_types.h"

#include <gtest/gtest.h>

using namespace ignite;

/**
 * Test suite.
 */
class connection_info_test : public ::testing::Test {};

TEST_F(connection_info_test, supported_info) {
    char buffer[4096];
    short res_len = 0;

    configuration cfg;
    connection_info info(cfg);

    sql_result result;

#ifdef SQL_DRIVER_NAME
    result = info.get_info(SQL_DRIVER_NAME, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_DRIVER_NAME

#ifdef SQL_DBMS_NAME
    result = info.get_info(SQL_DBMS_NAME, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_DBMS_NAME

#ifdef SQL_DRIVER_ODBC_VER
    result = info.get_info(SQL_DRIVER_ODBC_VER, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_DRIVER_ODBC_VER

#ifdef SQL_DBMS_VER
    result = info.get_info(SQL_DBMS_VER, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_DBMS_VER

#ifdef SQL_DRIVER_VER
    result = info.get_info(SQL_DRIVER_VER, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_DRIVER_VER

#ifdef SQL_COLUMN_ALIAS
    result = info.get_info(SQL_COLUMN_ALIAS, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_COLUMN_ALIAS

#ifdef SQL_IDENTIFIER_QUOTE_CHAR
    result = info.get_info(SQL_IDENTIFIER_QUOTE_CHAR, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_IDENTIFIER_QUOTE_CHAR

#ifdef SQL_CATALOG_NAME_SEPARATOR
    result = info.get_info(SQL_CATALOG_NAME_SEPARATOR, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_CATALOG_NAME_SEPARATOR

#ifdef SQL_SPECIAL_CHARACTERS
    result = info.get_info(SQL_SPECIAL_CHARACTERS, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_SPECIAL_CHARACTERS

#ifdef SQL_CATALOG_TERM
    result = info.get_info(SQL_CATALOG_TERM, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_CATALOG_TERM

#ifdef SQL_TABLE_TERM
    result = info.get_info(SQL_TABLE_TERM, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_TABLE_TERM

#ifdef SQL_SCHEMA_TERM
    result = info.get_info(SQL_SCHEMA_TERM, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_SCHEMA_TERM

#ifdef SQL_ASYNC_DBC_FUNCTIONS
    result = info.get_info(SQL_ASYNC_DBC_FUNCTIONS, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_ASYNC_DBC_FUNCTIONS

#ifdef SQL_GETDATA_EXTENSIONS
    result = info.get_info(SQL_GETDATA_EXTENSIONS, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_GETDATA_EXTENSIONS

#ifdef SQL_ODBC_INTERFACE_CONFORMANCE
    result = info.get_info(SQL_ODBC_INTERFACE_CONFORMANCE, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_ODBC_INTERFACE_CONFORMANCE

#ifdef SQL_SQL_CONFORMANCE
    result = info.get_info(SQL_SQL_CONFORMANCE, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_SQL_CONFORMANCE

#ifdef SQL_CATALOG_USAGE
    result = info.get_info(SQL_CATALOG_USAGE, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_CATALOG_USAGE

#ifdef SQL_SCHEMA_USAGE
    result = info.get_info(SQL_SCHEMA_USAGE, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_SCHEMA_USAGE

#ifdef SQL_MAX_IDENTIFIER_LEN
    result = info.get_info(SQL_MAX_IDENTIFIER_LEN, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_MAX_IDENTIFIER_LEN

#ifdef SQL_AGGREGATE_FUNCTIONS
    result = info.get_info(SQL_AGGREGATE_FUNCTIONS, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_AGGREGATE_FUNCTIONS

#ifdef SQL_AGGREGATE_FUNCTIONS
    result = info.get_info(SQL_NUMERIC_FUNCTIONS, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_AGGREGATE_FUNCTIONS

#ifdef SQL_STRING_FUNCTIONS
    result = info.get_info(SQL_STRING_FUNCTIONS, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_STRING_FUNCTIONS

#ifdef SQL_TIMEDATE_FUNCTIONS
    result = info.get_info(SQL_TIMEDATE_FUNCTIONS, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_TIMEDATE_FUNCTIONS

#ifdef SQL_TIMEDATE_ADD_INTERVALS
    result = info.get_info(SQL_TIMEDATE_ADD_INTERVALS, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_TIMEDATE_ADD_INTERVALS

#ifdef SQL_TIMEDATE_DIFF_INTERVALS
    result = info.get_info(SQL_TIMEDATE_DIFF_INTERVALS, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_TIMEDATE_DIFF_INTERVALS

#ifdef SQL_DATETIME_LITERALS
    result = info.get_info(SQL_DATETIME_LITERALS, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_DATETIME_LITERALS

#ifdef SQL_SYSTEM_FUNCTIONS
    result = info.get_info(SQL_SYSTEM_FUNCTIONS, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_SYSTEM_FUNCTIONS

#ifdef SQL_CONVERT_FUNCTIONS
    result = info.get_info(SQL_CONVERT_FUNCTIONS, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_CONVERT_FUNCTIONS

#ifdef SQL_OJ_CAPABILITIES
    result = info.get_info(SQL_OJ_CAPABILITIES, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_OJ_CAPABILITIES

#ifdef SQL_POS_OPERATIONS
    result = info.get_info(SQL_POS_OPERATIONS, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_POS_OPERATIONS

#ifdef SQL_MAX_CONCURRENT_ACTIVITIES
    result = info.get_info(SQL_MAX_CONCURRENT_ACTIVITIES, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_MAX_CONCURRENT_ACTIVITIES

#ifdef SQL_CURSOR_COMMIT_BEHAVIOR
    result = info.get_info(SQL_CURSOR_COMMIT_BEHAVIOR, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_CURSOR_COMMIT_BEHAVIOR

#ifdef SQL_CURSOR_ROLLBACK_BEHAVIOR
    result = info.get_info(SQL_CURSOR_ROLLBACK_BEHAVIOR, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_CURSOR_ROLLBACK_BEHAVIOR

#ifdef SQL_TXN_CAPABLE
    result = info.get_info(SQL_TXN_CAPABLE, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_TXN_CAPABLE

#ifdef SQL_QUOTED_IDENTIFIER_CASE
    result = info.get_info(SQL_QUOTED_IDENTIFIER_CASE, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
#endif // SQL_QUOTED_IDENTIFIER_CASE
}

TEST_F(connection_info_test, default_username) {
    char buffer[4096];
    short res_len = 0;

    configuration cfg;
    connection_info info(cfg);

    sql_result result;

#ifdef SQL_USER_NAME
    result = info.get_info(SQL_USER_NAME, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
    EXPECT_EQ(res_len, 0);
#endif // SQL_USER_NAME
}

TEST_F(connection_info_test, username) {
    char buffer[4096];
    short res_len = 0;

    std::string identity = "username-1";
    std::string secret = "secret-1";

    configuration cfg(identity, secret);
    connection_info info(cfg);

    sql_result result;

#ifdef SQL_USER_NAME
    result = info.get_info(SQL_USER_NAME, buffer, sizeof(buffer), &res_len);
    EXPECT_EQ(result, sql_result::AI_SUCCESS);
    EXPECT_EQ(res_len, identity.size());
    EXPECT_EQ(std::string(buffer), identity);
#endif // SQL_USER_NAME
}
