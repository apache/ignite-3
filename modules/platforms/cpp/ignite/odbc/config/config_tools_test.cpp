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
#include "ignite/odbc/common_types.h"

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
