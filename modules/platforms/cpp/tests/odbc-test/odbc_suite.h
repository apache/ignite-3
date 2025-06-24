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

#include "ignite_runner.h"
#include "odbc_connection.h"
#include "test_utils.h"

#include "tests/test-common/basic_auth_test_suite.h"

#include <gtest/gtest.h>

namespace ignite {

/**
 * Test suite.
 */
class odbc_suite : public virtual ::testing::Test, public odbc_connection {
public:
    static inline const std::string TABLE_1 = "TBL1";
    static inline const std::string TABLE_NAME_ALL_COLUMNS = "TBL_ALL_COLUMNS";
    static inline const std::string TABLE_NAME_ALL_COLUMNS_SQL = "TBL_ALL_COLUMNS_SQL";

    static constexpr const char *KEY_COLUMN = "key";
    static constexpr const char *VAL_COLUMN = "val";

    static inline const std::string DRIVER_NAME = "Apache Ignite 3";

    /**
     * Get node addresses to use for tests.
     *
     * @return Addresses.
     */
    static std::string get_nodes_address(const std::vector<std::string> &addresses) {
        std::string res;
        for (const auto &addr : addresses)
            res += addr + ',';

        return res;
    }

    /**
     * Get node addresses to use for tests.
     *
     * @return Addresses.
     */
    static std::string get_nodes_address() {
        return get_nodes_address(ignite_runner::get_node_addrs());
    }

    /**
     * Get basic connection string with specified addresses.
     *
     * @return Basic connection string with specified addresses.
     */
    static std::string get_basic_connection_string(const std::string &addrs) {
        return "driver={" + DRIVER_NAME + "};address=" + addrs + ';';
    }

    /**
     * Get basic connection string with default addresses.
     *
     * @return Basic connection string with default addresses.
     */
    static std::string get_basic_connection_string() {
        return get_basic_connection_string(get_nodes_address());
    }

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
     * Try to connect to SSL server successfully.
     * @return Client.
     */
    void connect_successfully_to_ssl_server() {
        auto addresses = get_nodes_address(ignite_runner::get_ssl_node_addrs());
        auto conn_str = get_basic_connection_string(addresses);

        conn_str += ";ssl_mode=require";
        conn_str += ";ssl_cert_file=" + get_ssl_file("client.pem");
        conn_str += ";ssl_key_file=" + get_ssl_file("client.pem");
        conn_str += ";ssl_ca_file=" + get_ssl_file("ca.pem");

        odbc_connect(conn_str);
    }
};

} // namespace ignite
