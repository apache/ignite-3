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

#include "ignite/odbc/common_types.h"
#include "string_utils.h"

#include <gtest/gtest.h>

using namespace ignite;

/**
 * Test suite.
 */
class string_utils_test : public ::testing::Test {};

TEST_F(string_utils_test, ltrim_basic) {
    auto test_ltrim = [](std::string_view expected, std::string_view in) { EXPECT_EQ(ltrim(in), expected); };

    test_ltrim("", "");
    test_ltrim("", " ");
    test_ltrim("", "      ");
    test_ltrim("abc", "abc");
    test_ltrim("abc ", "abc ");
    test_ltrim("abc", " abc");
    test_ltrim("abc ", " abc ");
    test_ltrim("abc     ", "     abc     ");
    test_ltrim("a b  c ", " a b  c ");
}

TEST_F(string_utils_test, rtrim_basic) {
    auto test_rtrim = [](std::string_view expected, std::string_view in) { EXPECT_EQ(rtrim(in), expected); };

    test_rtrim("", "");
    test_rtrim("", " ");
    test_rtrim("", "      ");
    test_rtrim("abc", "abc");
    test_rtrim("abc", "abc ");
    test_rtrim(" abc", " abc");
    test_rtrim(" abc", " abc ");
    test_rtrim("     abc", "     abc     ");
    test_rtrim(" a b  c", " a b  c ");
}

TEST_F(string_utils_test, trim_basic) {
    auto test_trim = [](std::string_view expected, std::string_view in) { EXPECT_EQ(trim(in), expected); };

    test_trim("", "");
    test_trim("", " ");
    test_trim("", "      ");
    test_trim("abc", "abc");
    test_trim("abc", "abc ");
    test_trim("abc", " abc");
    test_trim("abc", " abc ");
    test_trim("abc", "     abc     ");
    test_trim("a b  c", " a b  c ");
}

TEST_F(string_utils_test, split_once_basic) {
    auto test_split_once = [](std::string_view p1, std::string_view p2, std::string_view in, char d) {
        auto res = split_once(in, d);
        EXPECT_EQ(p1, res.first);
        EXPECT_EQ(p2, res.second);
    };

    test_split_once("a1", "a2,a3,a4,a5", "a1,a2,a3,a4,a5", ',');
    test_split_once("a2", "a3,a4,a5", "a2,a3,a4,a5", ',');
    test_split_once("a3", "a4,a5", "a3,a4,a5", ',');
    test_split_once("a4", "a5", "a4,a5", ',');
    test_split_once("a5", "", "a5", ',');
    test_split_once("", "", "", ',');

    test_split_once("", ",,", ",,,", ',');

    test_split_once("a1", "a2;a3;a4;a5", "a1;a2;a3;a4;a5", ';');
    test_split_once("a1;a2;a3;a4;a5", "", "a1;a2;a3;a4;a5", ',');
}

TEST_F(string_utils_test, split_basic) {
    auto test_split = [](std::vector<std::string_view> exp, std::string_view in, char d) {
        std::vector<std::string_view> res;
        for_every_delimited(in, d, [&res](auto s) { res.push_back(s); });

        ASSERT_EQ(exp.size(), res.size());

        for (size_t i = 0; i < exp.size(); ++i) {
            EXPECT_EQ(exp[i], res[i]) << "Vectors differ at index " << i;
        }
    };

    test_split({"a", "b"}, "a,b", ',');
    test_split({"a", "b", "c", "d", "bar"}, "a,b,c,d,bar", ',');

    test_split({}, "", ',');
    test_split({"abc"}, "abc", ',');
    test_split({"abc"}, "abc,", ',');
    test_split({"", "abc"}, ",abc", ',');
    test_split({"a", "", "b"}, "a,,b", ',');
}

TEST_F(string_utils_test, to_lower_basic) {
    auto test_to_lower = [](std::string_view exp, std::string in) { EXPECT_EQ(to_lower(std::move(in)), exp); };

    test_to_lower("lorem ipsum", "Lorem Ipsum");
    test_to_lower("lorem ipsum", "LOREM IPSUM");
    test_to_lower("lorem ipsum", "LoRem IpsUM");
    test_to_lower("lorem ipsum", "lorem ipsum");
}
