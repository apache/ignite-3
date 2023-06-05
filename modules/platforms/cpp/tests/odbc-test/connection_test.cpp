/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifdef _WIN32
#   include <windows.h>
#endif

#include <sql.h>
#include <sqlext.h>

#include <string>

#include <gtest/gtest.h>

#include "test_utils.h"
#include "ignite_runner.h"


constexpr size_t ODBC_BUFFER_SIZE = 1024;

std::string get_odbc_error_message(SQLSMALLINT handleType, SQLHANDLE handle, SQLSMALLINT idx = 1)
{
    SQLCHAR sqlstate[7] = {};
    SQLINTEGER nativeCode;

    SQLCHAR message[ODBC_BUFFER_SIZE];
    SQLSMALLINT real_len = 0;

    SQLGetDiagRec(handleType, handle, idx, sqlstate, &nativeCode, message, ODBC_BUFFER_SIZE, &real_len);

    std::string res(reinterpret_cast<char*>(sqlstate));

    if (!res.empty())
        res.append(": ").append(reinterpret_cast<char*>(message), real_len);
    else
        res = "No results";

    return res;
}

void odbc_connect(const std::string& connect_str) {
    // Allocate an environment handle
    SQLHENV env;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

    EXPECT_TRUE(env != nullptr);

    // We want ODBC 3 support
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(SQL_OV_ODBC3), 0);

    // Allocate a connection handle
    SQLHDBC conn;
    SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

    EXPECT_TRUE(conn != nullptr);

    // Connect string
    std::vector<SQLCHAR> connect_str0(connect_str.begin(), connect_str.end());

    SQLCHAR outstr[ODBC_BUFFER_SIZE];
    SQLSMALLINT outstrlen;

    // Connecting to ODBC server.
    SQLRETURN ret = SQLDriverConnect(conn, NULL, &connect_str0[0], static_cast<SQLSMALLINT>(connect_str0.size()),
        outstr, sizeof(outstr), &outstrlen, SQL_DRIVER_COMPLETE);

    if (!SQL_SUCCEEDED(ret)) {
        FAIL() << get_odbc_error_message(SQL_HANDLE_DBC, conn);
    }

    // Allocate a statement handle
    SQLHSTMT statement;
    SQLAllocHandle(SQL_HANDLE_STMT, conn, &statement);

    EXPECT_TRUE(statement != nullptr);
}

/**
 * Test suite.
 */
class connection_test : public ::testing::Test {};

TEST_F(connection_test, connection_success) {
    std::string addr_str;
    auto addrs = ignite::ignite_runner::get_node_addrs();
    for (auto &addr : addrs)
        addr_str += addr;

    odbc_connect("DRIVER={Apache Ignite};ADDRESS=" + addr_str);
}
