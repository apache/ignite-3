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

#include "ignite/client/ignite_client.h"
#include "ignite/client/ignite_client_configuration.h"

#include "gtest_logger.h"
#include "ignite_runner.h"
#include "test_utils.h"

#include <gtest/gtest.h>

#include <memory>
#include <string_view>

namespace ignite {

using namespace std::string_view_literals;

/**
 * Test suite.
 */
class ignite_runner_suite : public virtual ::testing::Test {
public:
    static constexpr std::string_view TABLE_1 = "TBL1"sv;
    static constexpr std::string_view TABLE_NAME_ALL_COLUMNS = "TBL_ALL_COLUMNS"sv;
    static constexpr std::string_view TABLE_NAME_ALL_COLUMNS_SQL = "TBL_ALL_COLUMNS_SQL"sv;


    inline static const std::string PLATFORM_TEST_NODE_RUNNER =
        "org.apache.ignite.internal.runner.app.PlatformTestNodeRunner";

    inline static const std::string ENABLE_AUTHN_JOB = PLATFORM_TEST_NODE_RUNNER + "$EnableAuthenticationJob";

    inline static const std::string IT_THIN_CLIENT_COMPUTE_TEST =
        "org.apache.ignite.internal.runner.app.client.ItThinClientComputeTest";

    inline static const std::string NODE_NAME_JOB = IT_THIN_CLIENT_COMPUTE_TEST + "$NodeNameJob";
    inline static const std::string SLEEP_JOB = IT_THIN_CLIENT_COMPUTE_TEST + "$SleepJob";
    inline static const std::string TO_STRING_JOB = IT_THIN_CLIENT_COMPUTE_TEST + "$ToStringJob";
    inline static const std::string CONCAT_JOB = IT_THIN_CLIENT_COMPUTE_TEST + "$ConcatJob";
    inline static const std::string ERROR_JOB = IT_THIN_CLIENT_COMPUTE_TEST + "$IgniteExceptionJob";
    inline static const std::string ECHO_JOB = IT_THIN_CLIENT_COMPUTE_TEST + "$EchoJob";
    inline static const std::string RETURN_NULL_JOB = IT_THIN_CLIENT_COMPUTE_TEST + "$ReturnNullJob";

    static constexpr const char *KEY_COLUMN = "KEY";
    static constexpr const char *VAL_COLUMN = "VAL";

    /**
     * Get logger.
     *
     * @return Logger for tests.
     */
    static std::shared_ptr<gtest_logger> get_logger() { return std::make_shared<gtest_logger>(false, true); }

    /**
     * Get tuple for specified column values.
     *
     * @param id ID.
     * @param val Value.
     * @return Ignite tuple instance.
     */
    static ignite_tuple get_tuple(int64_t id, std::string val) {
        return {{KEY_COLUMN, id}, {VAL_COLUMN, std::move(val)}};
    }

    /**
     * Get tuple for specified column values.
     *
     * @param id ID.
     * @return Ignite tuple instance.
     */
    static ignite_tuple get_tuple(int64_t id) { return {{KEY_COLUMN, id}}; }

    /**
     * Get tuple for specified column values.
     *
     * @param val Value.
     * @return Ignite tuple instance.
     */
    static ignite_tuple get_tuple(std::string val) { return {{VAL_COLUMN, std::move(val)}}; }

    /**
     * Get node addresses to use for tests.
     *
     * @return Addresses.
     */
    static std::vector<std::string> get_node_addrs() { return ignite_runner::get_node_addrs(); }

    /**
     * Get node addresses to use for tests.
     *
     * @return Addresses.
     */
    static std::vector<std::string> get_ssl_node_addrs() { return ignite_runner::get_ssl_node_addrs(); }

    /**
     * Get node addresses to use for tests.
     *
     * @return Addresses.
     */
    static std::vector<std::string> get_ssl_node_ca_addrs() { return ignite_runner::get_ssl_node_ca_addrs(); }

    /**
     * Clear table @c TABLE_1.
     */
    static void clear_table1() {
        ignite_client_configuration cfg{get_node_addrs()};
        cfg.set_logger(get_logger());
        auto client = ignite_client::start(cfg, std::chrono::seconds(30));

        client.get_sql().execute(nullptr, nullptr, {"DELETE FROM " + std::string(TABLE_1)}, {});
    }

    /**
     * Get a path to an SSL file.
     * @param file
     * @return
     */
    static std::string get_ssl_file(const std::string& file)
    {
        auto test_dir = resolve_test_dir();
        auto ssl_files_dir = test_dir / "client-test" / "ssl";
        if (!std::filesystem::is_directory(ssl_files_dir))
            throw ignite_error("Can not find an 'ssl' directory in the current 'tests' directory: " + ssl_files_dir.string());

        return (ssl_files_dir / file).string();
    }

    /**
     * Try to connect to ssl server successfully.
     * @param timeout Timeout.
     * @return Client.
     */
    static ignite_client connect_successfully_to_ssl_server(std::chrono::seconds timeout) {
        ignite_client_configuration cfg{get_ssl_node_addrs()};
        cfg.set_logger(get_logger());

        cfg.set_ssl_mode(ssl_mode::REQUIRE);
        cfg.set_ssl_cert_file(get_ssl_file("client.pem"));
        cfg.set_ssl_key_file(get_ssl_file("client.pem"));
        cfg.set_ssl_ca_file(get_ssl_file("ca.pem"));

        return ignite_client::start(cfg, timeout);
    }
};

} // namespace ignite
