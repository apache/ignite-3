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

using namespace ignite;

/**
 * Test suite.
 */
class auth_test : public odbc_suite, public basic_auth_test_suite {
public:
    /**
     * Tear down.
     */
    static void TearDownTestSuite() { set_authentication_enabled(false); }

    /**
     * Get node addresses and user credentials to use for tests.
     *
     * @return Addresses and credentials.
     */
    static std::string get_auth_connection_string() {
        return get_basic_connection_string() + "identity=" + CORRECT_USERNAME + ';' + "secret=" + CORRECT_PASSWORD
            + ';';
    }

    /**
     * Get node addresses and user credentials with incorrect identity to use for tests.
     *
     * @return Addresses and credentials.
     */
    static std::string get_incorrect_identity_auth_connection_string() {
        return get_basic_connection_string() + "identity=" + INCORRECT_USERNAME + ';' + "secret=" + CORRECT_PASSWORD
            + ';';
    }

    /**
     * Get node addresses and user credentials with incorrect secret to use for tests.
     *
     * @return Addresses and credentials.
     */
    static std::string get_incorrect_secret_auth_connection_string() {
        return get_basic_connection_string() + "identity=" + CORRECT_USERNAME + ';' + "secret=" + INCORRECT_PASSWORD
            + ';';
    }

    /**
     * Get node addresses and user credentials with incorrect identity and secret to use for tests.
     *
     * @return Addresses and credentials.
     */
    static std::string get_incorrect_auth_connection_string() {
        return get_basic_connection_string() + "identity=" + INCORRECT_USERNAME + ';' + "secret=" + INCORRECT_PASSWORD
            + ';';
    }
};

TEST_F(auth_test, connection_success) {
    set_authentication_enabled(false);
    odbc_connect(get_basic_connection_string());
}

TEST_F(auth_test, auth_connection_disabled_on_server) {
    set_authentication_enabled(false);
    EXPECT_NO_THROW(odbc_connect_throw(get_auth_connection_string()));
}

TEST_F(auth_test, odbc3_supported) {
    set_authentication_enabled(false);
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

TEST_F(auth_test, username_no_auth) {
    set_authentication_enabled(false);
    EXPECT_NO_THROW(odbc_connect_throw(get_basic_connection_string()));

    SQLCHAR buffer[ODBC_BUFFER_SIZE];
    SQLSMALLINT resLen = 0;

    SQLRETURN ret = SQLGetInfo(m_conn, SQL_USER_NAME, buffer, ODBC_BUFFER_SIZE, &resLen);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_DBC, m_conn));

    EXPECT_EQ(std::string(""), std::string(reinterpret_cast<char *>(buffer)));
}

TEST_F(auth_test, username) {
    set_authentication_enabled(true);
    EXPECT_NO_THROW(odbc_connect_throw(get_auth_connection_string()));

    SQLCHAR buffer[ODBC_BUFFER_SIZE];
    SQLSMALLINT resLen = 0;

    SQLRETURN ret = SQLGetInfo(m_conn, SQL_USER_NAME, buffer, ODBC_BUFFER_SIZE, &resLen);

    if (!SQL_SUCCEEDED(ret))
        FAIL() << (get_odbc_error_message(SQL_HANDLE_DBC, m_conn));

    EXPECT_EQ(CORRECT_USERNAME, std::string(reinterpret_cast<char *>(buffer)));
}

TEST_F(auth_test, auth_connection_success) {
    set_authentication_enabled(true);
    EXPECT_NO_THROW(odbc_connect_throw(get_auth_connection_string()));
}

TEST_F(auth_test, auth_connection_disabled_on_client) {
    set_authentication_enabled(true);
    EXPECT_THROW(
        try { odbc_connect_throw(get_basic_connection_string()); } catch (const ignite_error &e) {
            EXPECT_THAT(e.what_str(), testing::HasSubstr("Authentication failed"));
            throw;
        },
        ignite_error);
}

TEST_F(auth_test, auth_connection_incorrect_creds) {
    set_authentication_enabled(true);
    auto test_conn_str = [this](const std::string &conn_str) {
        EXPECT_THROW(
            try { odbc_connect_throw(conn_str); } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), testing::HasSubstr("Authentication failed"));
                throw;
            },
            ignite_error);
    };
    test_conn_str(get_incorrect_identity_auth_connection_string());
    test_conn_str(get_incorrect_secret_auth_connection_string());
    test_conn_str(get_incorrect_auth_connection_string());
}