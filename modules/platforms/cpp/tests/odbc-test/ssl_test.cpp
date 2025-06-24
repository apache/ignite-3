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
#include <gmock/gmock-matchers.h>

using namespace ignite;

/**
 * Test suite.
 */
class ssl_test : public odbc_suite {
public:
    /**
     * Get a path to a SSL file.
     * @param file
     * @return
     */
    static std::string get_ssl_file(const std::string &file)
    {
        auto test_dir = resolve_test_dir();
        auto ssl_files_dir = test_dir / "odbc-test" / "ssl";
        if (!std::filesystem::is_directory(ssl_files_dir))
            throw ignite_error("Can not find an 'ssl' directory in the current 'tests' directory: " + ssl_files_dir.string());

        return (ssl_files_dir / file).string();
    }

    /**
     * Get SSL connection string params.
     *
     * @param key_file Key file.
     * @param cert_file Certificate file.
     * @param ca_file CA file.
     * @return SSL connection string.
     */
    static std::string get_ssl_connection_string_params(std::string key_file, std::string cert_file, std::string ca_file) {
        std::string conn_str{};

        conn_str += ";ssl_mode=require";
        conn_str += ";ssl_cert_file=" + get_ssl_file(key_file);
        conn_str += ";ssl_key_file=" + get_ssl_file(cert_file);
        conn_str += ";ssl_ca_file=" + get_ssl_file(ca_file);

        return conn_str;
    }

    /**
     * Get SSL connection string.
     *
     * @param key_file Key file.
     * @param cert_file Certificate file.
     * @param ca_file CA file.
     * @return SSL connection string.
     */
    static std::string get_ssl_connection_string(std::string key_file, std::string cert_file, std::string ca_file) {
        auto addresses = get_nodes_address(ignite_runner::get_ssl_node_addrs());
        auto conn_str = get_basic_connection_string(addresses)
            + get_ssl_connection_string_params(key_file, cert_file, ca_file);

        return conn_str;
    }

    /**
     * Get SSL connection string.
     *
     * @param key_file Key file.
     * @param cert_file Certificate file.
     * @param ca_file CA file.
     * @return SSL connection string.
     */
    static std::string get_ssl_ca_connection_string(std::string key_file, std::string cert_file, std::string ca_file) {
        auto addresses = get_nodes_address(ignite_runner::get_ssl_node_ca_addrs());
        auto conn_str = get_basic_connection_string(addresses)
            + get_ssl_connection_string_params(key_file, cert_file, ca_file);

        return conn_str;
    }

    /**
     * Try to connect to SSL server successfully.
     * @return Client.
     */
    void connect_successfully_to_ssl_server() {
        odbc_connect(get_ssl_connection_string("client.pem", "client.pem", "ca.pem"));
    }
};

TEST_F(ssl_test, ssl_connection_success)
{
    connect_successfully_to_ssl_server();
}

TEST_F(ssl_test, ssl_connection_unknown)
{
    auto conn_str = get_ssl_connection_string("client_unknown.pem", "client_unknown.pem", "ca.pem");
    odbc_connect(conn_str);
}

TEST_F(ssl_test, ssl_connection_unknown_2)
{
    EXPECT_THROW(
        {
            try {
                auto conn_str = get_ssl_ca_connection_string("client_unknown.pem", "client_unknown.pem", "ca.pem");
                odbc_connect(conn_str);
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), testing::HasSubstr("08001: Failed to get handshake response (Did you forget to enable SSL?)"));
                throw;
            }
        },
        ignite_error);
}
