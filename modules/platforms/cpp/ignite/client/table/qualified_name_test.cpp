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

#include "ignite/client/table/qualified_name.h"
#include "ignite/common/ignite_error.h"

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

using namespace ignite;
using namespace detail;


TEST(client_qualified_name, create_empty_schema_empty_name_throws) {
    EXPECT_THROW(
        {
            try {
                (void)qualified_name::create({}, {});
            } catch (const ignite_error &e) {
                EXPECT_EQ(e.get_status_code(), error::code::ILLEGAL_ARGUMENT);
                EXPECT_THAT(e.what_str(), testing::HasSubstr("Object name can not be empty"));
                throw;
            }
        },
        ignite_error);
}

TEST(client_qualified_name, create_empty_name_throws) {
    EXPECT_THROW(
        {
            try {
                (void)qualified_name::create("TEST", {});
            } catch (const ignite_error &e) {
                EXPECT_EQ(e.get_status_code(), error::code::ILLEGAL_ARGUMENT);
                EXPECT_THAT(e.what_str(), testing::HasSubstr("Object name can not be empty"));
                throw;
            }
        },
        ignite_error);
}

TEST(client_qualified_name, parse_empty_name_throws) {
    EXPECT_THROW(
        {
            try {
                (void)qualified_name::parse({});
            } catch (const ignite_error &e) {
                EXPECT_EQ(e.get_status_code(), error::code::ILLEGAL_ARGUMENT);
                EXPECT_THAT(e.what_str(), testing::HasSubstr("Object name can not be empty"));
                throw;
            }
        },
        ignite_error);
}

TEST(client_qualified_name, create_empty_schema_default) {
    auto name = qualified_name::create({}, "TEST");
    EXPECT_EQ(name.get_schema_name(), qualified_name::DEFAULT_SCHEMA_NAME);
}

TEST(client_qualified_name, parse_implicit_schema_default) {
    auto name = qualified_name::parse("TEST");
    EXPECT_EQ(name.get_schema_name(), qualified_name::DEFAULT_SCHEMA_NAME);
}

class canonical_values_fixture : public ::testing::TestWithParam<std::string> {};

TEST_P(canonical_values_fixture, canonical_name_parse) {
    auto canonical_name = GetParam();

    EXPECT_EQ(canonical_name, qualified_name::parse(canonical_name).get_canonical_name());
}

INSTANTIATE_TEST_SUITE_P(
    client_name_utils, canonical_values_fixture,
    ::testing::Values(
        "FOO.FOO",
        "_FOO._FOO",
        "_._",
        "A\xCC\x80.A\xC2\xB7",
        R"("foo"."bar")",
        "\"\xF0\x9F\x98\x85\".\"\xC2\xB7\""
    )
);

// class quote_if_needed_fixture : public ::testing::TestWithParam<std::tuple<std::string, std::string>> {};
//
// TEST_P(quote_if_needed_fixture, quote_if_needed) {
//     auto [name, expected] = GetParam();
//
//     EXPECT_EQ(expected, quote_if_needed(name, qualified_name::QUOTE_CHAR));
//     EXPECT_EQ(name, parse_identifier(quote_if_needed(
//         name, qualified_name::QUOTE_CHAR), qualified_name::QUOTE_CHAR, qualified_name::SEPARATOR_CHAR));
// }
//
//
// INSTANTIATE_TEST_SUITE_P(
//     client_name_utils, quote_if_needed_fixture,
//     ::testing::Values(
//         std::make_tuple("foo", "\"foo\""),
//         std::make_tuple("fOo", "\"fOo\""),
//         std::make_tuple("FOO", "FOO"),
//         std::make_tuple("_FOO", "_FOO"),
//         std::make_tuple("_", "_"),
//         std::make_tuple("__", "__"),
//         std::make_tuple("_\xC2\xB7", "_\xC2\xB7"),
//         std::make_tuple("A\xCC\x80", "A\xCC\x80"),
//         std::make_tuple("1o0", "\"1o0\""),
//         std::make_tuple("@#$", "\"@#$\""),
//         std::make_tuple("f16", "\"f16\""),
//         std::make_tuple("F16", "F16"),
//         std::make_tuple("Ff16", "\"Ff16\""),
//         std::make_tuple("FF16", "FF16"),
//         std::make_tuple(" ", "\" \""),
//         std::make_tuple(" F", "\" F\""),
//         std::make_tuple(" ,", "\" ,\""),
//         std::make_tuple("\xF0\x9F\x98\x85", "\"\xF0\x9F\x98\x85\""),
//         std::make_tuple("\"foo\"", "\"\"\"foo\"\"\""),
//         std::make_tuple("\"fOo\"", "\"\"\"fOo\"\"\""),
//         std::make_tuple("\"f.f\"", "\"\"\"f.f\"\"\""),
//         std::make_tuple("foo\"bar\"", "\"foo\"\"bar\"\"\""),
//         std::make_tuple("foo\"bar", "\"foo\"\"bar\"")
//     )
// );
