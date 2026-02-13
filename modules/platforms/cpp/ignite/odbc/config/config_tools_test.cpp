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

#include "config_tools.h"
#include "configuration.h"
#include "ignite/odbc/odbc_error.h"

#include <gtest/gtest.h>

using namespace ignite;

/**
 * Test suite.
 */
class config_tools_test : public ::testing::Test {};

TEST_F(config_tools_test, parse_address_basic) {
    auto test_parse_address = [](const std::vector<end_point> &exp, std::string_view in) {
        auto res = parse_address(in);
        ASSERT_EQ(exp.size(), res.size());

        for (size_t i = 0; i < exp.size(); ++i) {
            EXPECT_EQ(exp[i], res[i]) << "Vectors differ at index " << i;
        }
    };

    test_parse_address({{"127.0.0.1", 10800}}, "127.0.0.1");
    test_parse_address({{"127.0.0.1", 10800}, {"127.0.0.1", 10800}}, "127.0.0.1,127.0.0.1");
    test_parse_address({{"127.0.0.1", 42}}, "127.0.0.1:42");

    test_parse_address(
        {{"127.0.0.1", 42}, {"localhost", 1550}, {"0.0.0.0", 10800}}, "127.0.0.1:42, localhost:1550,0.0.0.0    ");

    test_parse_address({}, "");
    test_parse_address({}, ",,,");
    test_parse_address({}, ",,,");
    test_parse_address({{"127.0.0.1", 10800}}, ",,,,127.0.0.1,,,,");
}

TEST_F(config_tools_test, normalize_argument_string_basic) {
    auto test_normalize_argument_string = [](std::string_view exp, std::string_view in) {
        EXPECT_EQ(normalize_argument_string(in), exp);
    };

    test_normalize_argument_string("", "");
    test_normalize_argument_string("abc", "abc");
    test_normalize_argument_string("abc", "Abc");
    test_normalize_argument_string("abc", "ABC");
    test_normalize_argument_string("a b c", " A B C ");
}

TEST_F(config_tools_test, parse_connection_string_basic) {
    auto test_parse_connection_string = [](const config_map &exp, std::string_view in) {
        EXPECT_EQ(parse_connection_string(in), exp);
    };

    test_parse_connection_string({{"key1", "value1"}, {"key2", "value2"}}, "key1=value1;key2 = value2;");

    test_parse_connection_string({}, "");
    test_parse_connection_string({}, ";;");

    test_parse_connection_string({{"k1", "v1"}}, "k1=v1;k1=v2;k1=v3");
}

TEST_F(config_tools_test, parse_connection_string_heartbeat) {
    auto test_parse_connection_string_return_map = [](const config_map &exp, std::string_view in) {
        auto map = parse_connection_string(in);
        EXPECT_EQ(map, exp);
        return map;
    };

    configuration config;
    EXPECT_EQ(config.get_heartbeat_interval().get_value(), std::chrono::seconds(30));

    auto map = test_parse_connection_string_return_map({{"heartbeat_interval", "2000"}, {"host", "127.0.0.1:5000"}},
        "host=127.0.0.1:5000;heartbeat_interval=2000");
    config.from_config_map(map);
    EXPECT_EQ(config.get_heartbeat_interval().get_value(), std::chrono::milliseconds(2000));

    map = test_parse_connection_string_return_map({{"heartbeat_interval", "0"}, {"host", "127.0.0.1:5000"}},
        "host=127.0.0.1:5000;heartbeat_interval=0");
    config.from_config_map(map);
    EXPECT_EQ(config.get_heartbeat_interval().get_value(), std::chrono::milliseconds(0));
}

TEST_F(config_tools_test, parse_connection_string_negative_heartbeat) {
    EXPECT_THROW(
        try {
            auto config_map = parse_connection_string("host=127.0.0.1:5000;heartbeat_interval=-1");
            ignite::configuration configuration;
            configuration.from_config_map(config_map);
        } catch (const ignite::odbc_error& error) {
            EXPECT_EQ(error.what(), std::string("Heartbeat interval value could not be negative: -1"));
            throw;
        },
        ignite::odbc_error
    );
}

TEST_F(config_tools_test, parse_connection_string_invalid_heartbeat) {
    EXPECT_THROW(
        try {
            auto config_map = parse_connection_string("host=127.0.0.1:5000;heartbeat_interval=abc");
            ignite::configuration configuration;
            configuration.from_config_map(config_map);
        } catch (const ignite::odbc_error& error) {
            EXPECT_EQ(error.what(), std::string("Invalid heartbeat interval value: abc"));
            throw;
        },
        ignite::odbc_error
    );
}
