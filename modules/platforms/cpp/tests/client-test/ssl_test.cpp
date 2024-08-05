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

#include "ignite_runner_suite.h"
#include "tests/test-common/test_utils.h"

#include "ignite/client/ignite_client.h"
#include "ignite/client/ignite_client_configuration.h"

#include <gtest/gtest.h>
#include <gmock/gmock-matchers.h>

using namespace ignite;

/**
 * Test suite.
 */
class ssl_test : public ignite_runner_suite { };

/**
 * Get a path to a SSL file.
 * @param file
 * @return
 */
std::string get_ssl_file(const std::string& file)
{
    auto test_dir = resolve_test_dir();
    auto ssl_files_dir = test_dir / "client-test" / "ssl";
    if (std::filesystem::is_directory(ssl_files_dir))
        throw ignite_error("Can not find a 'ssl' directory in the current 'tests' directory: " + test_dir.string());

    return (ssl_files_dir / file).string();
}

TEST_F(ssl_test, ssl_connection_success)
{
    ignite_client_configuration cfg{get_ssl_node_addrs()};

    cfg.set_ssl_mode(ssl_mode::REQUIRE);
    cfg.set_ssl_cert_file(get_ssl_file("client_full.pem"));
    cfg.set_ssl_key_file(get_ssl_file("client_full.pem"));
    cfg.set_ssl_ca_file(get_ssl_file("ca.pem"));

    auto client = ignite_client::start(cfg, std::chrono::seconds(30));
}

TEST_F(ssl_test, ssl_connection_reject)
{
    ignite_client_configuration cfg{get_ssl_node_addrs()};

    cfg.set_ssl_mode(ssl_mode::REQUIRE);
    cfg.set_ssl_cert_file(get_ssl_file("client_unknown.pem"));
    cfg.set_ssl_key_file(get_ssl_file("client_unknown.pem"));
    cfg.set_ssl_ca_file(get_ssl_file("ca.pem"));

    EXPECT_THROW(
        {
            try {
                (void) ignite_client::start(cfg, std::chrono::seconds(30));
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), testing::HasSubstr("Rejected"));
                throw;
            }
        },
        ignite_error);
}

TEST_F(ssl_test, ssl_connection_reject_2)
{
    ignite_client_configuration cfg{get_ssl_node_addrs()};

    cfg.set_ssl_mode(ssl_mode::DISABLE);

    EXPECT_THROW(
        {
            try {
                (void) ignite_client::start(cfg, std::chrono::seconds(30));
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), testing::HasSubstr("Rejected"));
                throw;
            }
        },
        ignite_error);
}

TEST_F(ssl_test, ssl_connection_rejected_3)
{
    ignite_client_configuration cfg{get_ssl_node_addrs()};

    cfg.set_ssl_mode(ssl_mode::REQUIRE);
    cfg.set_ssl_cert_file(get_ssl_file("client_full.pem"));
    cfg.set_ssl_key_file(get_ssl_file("client_full.pem"));
    cfg.set_ssl_ca_file(get_ssl_file("ca.pem"));

    EXPECT_THROW(
        {
            try {
                (void) ignite_client::start(cfg, std::chrono::seconds(30));
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), testing::HasSubstr("Rejected"));
                throw;
            }
        },
        ignite_error);
}

