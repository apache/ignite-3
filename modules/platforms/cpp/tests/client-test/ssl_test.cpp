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

TEST_F(ssl_test, ssl_connection_success)
{
    auto client = connect_successfully_to_ssl_server(std::chrono::seconds(30));
}

TEST_F(ssl_test, ssl_connection_unknown)
{
    ignite_client_configuration cfg{get_ssl_node_addrs()};
    cfg.set_logger(get_logger());

    cfg.set_ssl_mode(ssl_mode::REQUIRE);
    cfg.set_ssl_cert_file(get_ssl_file("client_unknown.pem"));
    cfg.set_ssl_key_file(get_ssl_file("client_unknown.pem"));
    cfg.set_ssl_ca_file(get_ssl_file("ca.pem"));

    auto client = ignite_client::start(cfg, std::chrono::seconds(30));
}

TEST_F(ssl_test, ssl_connection_unknown_2)
{
    ignite_client_configuration cfg{get_ssl_node_ca_addrs()};
    cfg.set_logger(get_logger());

    cfg.set_ssl_mode(ssl_mode::REQUIRE);
    cfg.set_ssl_cert_file(get_ssl_file("client_unknown.pem"));
    cfg.set_ssl_key_file(get_ssl_file("client_unknown.pem"));
    cfg.set_ssl_ca_file(get_ssl_file("ca.pem"));
    EXPECT_THROW(
        {
            try {
                (void) ignite_client::start(cfg, std::chrono::seconds(5));
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), testing::HasSubstr("Can not establish connection within timeout"));
                throw;
            }
        },
        ignite_error);
}

TEST_F(ssl_test, ssl_connection_reject_2)
{
    ignite_client_configuration cfg{get_ssl_node_addrs()};
    cfg.set_logger(get_logger());

    cfg.set_ssl_mode(ssl_mode::DISABLE);

    EXPECT_THROW(
        {
            try {
                (void) ignite_client::start(cfg, std::chrono::seconds(5));
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), testing::HasSubstr("Can not establish connection within timeout"));
                throw;
            }
        },
        ignite_error);
}

TEST_F(ssl_test, ssl_connection_rejected_3)
{
    ignite_client_configuration cfg{get_node_addrs()};
    cfg.set_logger(get_logger());

    cfg.set_ssl_mode(ssl_mode::REQUIRE);
    cfg.set_ssl_cert_file(get_ssl_file("client.pem"));
    cfg.set_ssl_key_file(get_ssl_file("client.pem"));
    cfg.set_ssl_ca_file(get_ssl_file("ca.pem"));

    EXPECT_THROW(
        {
            try {
                (void) ignite_client::start(cfg, std::chrono::seconds(5));
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), testing::HasSubstr("Connection closed during SSL/TLS handshake"));
                throw;
            }
        },
        ignite_error);
}

TEST_F(ssl_test, ssl_cache_client_put_all_get_all)
{
    enum { BATCH_SIZE = 200 };

    auto client = connect_successfully_to_ssl_server(std::chrono::seconds(30));
    auto table = client.get_tables().get_table(TABLE_1);

    auto tuple_view = table->get_record_binary_view();

    clear_table1();

    std::vector<ignite_tuple> values;
    values.reserve(BATCH_SIZE);
    for (int i = 0; i < BATCH_SIZE; ++i)
        values.emplace_back(get_tuple(i, "Str_" + std::to_string(i)));

    tuple_view.upsert_all(nullptr, values);
    clear_table1();
}

TEST_F(ssl_test, ssl_cache_client_put_get)
{
    enum { OPS_NUM = 100 };

    auto client = connect_successfully_to_ssl_server(std::chrono::seconds(30));
    auto table = client.get_tables().get_table(TABLE_1);

    auto tuple_view = table->get_record_binary_view();

    clear_table1();
    for (int32_t i = 0; i < OPS_NUM; ++i)
    {
        auto to_insert = get_tuple(i, "Str_" + std::to_string(i*2));
        tuple_view.upsert(nullptr, to_insert);
        auto retrieved = tuple_view.get(nullptr, get_tuple(i));

        ASSERT_TRUE(retrieved.has_value());
        EXPECT_EQ(to_insert.column_count(), retrieved->column_count());
        EXPECT_EQ(to_insert.get<int64_t>("key"), retrieved->get<int64_t>("key"));
        EXPECT_EQ(to_insert.get<std::string>("val"), retrieved->get<std::string>("val"));
    }
    clear_table1();
}

TEST_F(ssl_test, ssl_connection_no_certs)
{
    ignite_client_configuration cfg{get_ssl_node_ca_addrs()};
    cfg.set_logger(get_logger());

    cfg.set_ssl_mode(ssl_mode::REQUIRE);
    cfg.set_ssl_ca_file(get_ssl_file("ca.pem"));
    EXPECT_THROW(
        {
            try {
                (void) ignite_client::start(cfg, std::chrono::seconds(5));
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), testing::HasSubstr("Can not establish connection within timeout"));
                throw;
            }
        },
        ignite_error);
}

TEST_F(ssl_test, ssl_connection_error_non_existing_ca)
{
    ignite_client_configuration cfg{get_ssl_node_ca_addrs()};
    cfg.set_logger(get_logger());

    cfg.set_ssl_mode(ssl_mode::REQUIRE);
    cfg.set_ssl_ca_file(get_ssl_file("non_existing_ca.pem"));
    EXPECT_THROW(
        {
            try {
                (void) ignite_client::start(cfg, std::chrono::seconds(5));
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), testing::HasSubstr("Can not set Certificate Authority path for secure connection"));
                EXPECT_THAT(e.what_str(), AnyOf(testing::HasSubstr("No such file or directory"), testing::HasSubstr("no such file")));
                throw;
            }
        },
        ignite_error);
}

TEST_F(ssl_test, ssl_connection_error_non_existing_key)
{
    ignite_client_configuration cfg{get_ssl_node_ca_addrs()};
    cfg.set_logger(get_logger());

    cfg.set_ssl_mode(ssl_mode::REQUIRE);
    cfg.set_ssl_key_file(get_ssl_file("non_existing_key.pem"));
    EXPECT_THROW(
        {
            try {
                (void) ignite_client::start(cfg, std::chrono::seconds(5));
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), testing::HasSubstr("Can not set private key file for secure connection"));
                EXPECT_THAT(e.what_str(), AnyOf(testing::HasSubstr("No such file or directory"), testing::HasSubstr("no such file")));
                throw;
            }
        },
        ignite_error);
}

TEST_F(ssl_test, ssl_connection_error_non_existing_cert)
{
    ignite_client_configuration cfg{get_ssl_node_ca_addrs()};
    cfg.set_logger(get_logger());

    cfg.set_ssl_mode(ssl_mode::REQUIRE);
    cfg.set_ssl_cert_file(get_ssl_file("non_existing_cert.pem"));
    EXPECT_THROW(
        {
            try {
                (void) ignite_client::start(cfg, std::chrono::seconds(5));
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), testing::HasSubstr("Can not set client certificate file for secure connection"));
                EXPECT_THAT(e.what_str(), AnyOf(testing::HasSubstr("No such file or directory"), testing::HasSubstr("no such file")));
                throw;
            }
        },
        ignite_error);
}
