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

#include "tests/test-common/hidden_param.h"

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

class canonical_values_fixture : public ::testing::TestWithParam<hidden> {};

TEST_P(canonical_values_fixture, canonical_name_parse) {
    auto canonical_name = unhide(GetParam());

    EXPECT_EQ(canonical_name, qualified_name::parse(canonical_name).get_canonical_name());
}

INSTANTIATE_TEST_SUITE_P(
    client_qualified_name, canonical_values_fixture,
    ::testing::Values(
        "FOO.FOO",
        "_FOO._FOO",
        "_._",
        "A\xCC\x80.A\xC2\xB7",
        R"("foo"."bar")",
        "\"\xF0\x9F\x98\x85\".\"\xC2\xB7\""
    ),
    print_test_index<canonical_values_fixture>
);

class valid_simple_names_fixture : public ::testing::TestWithParam<std::tuple<hidden, hidden>> {};

TEST_P(valid_simple_names_fixture, valid_simple_name) {
    auto [to_parse, expected] = unhide(GetParam());

    auto parsed = qualified_name::parse(to_parse).get_object_name();
    auto created = qualified_name::create({}, to_parse).get_object_name();

    EXPECT_EQ(expected, parsed);
    EXPECT_EQ(expected, created);

    EXPECT_EQ(parsed, created);
}

INSTANTIATE_TEST_SUITE_P(
    client_qualified_name, valid_simple_names_fixture,
    ::testing::Values(
        std::make_tuple("foo", "FOO"),
        std::make_tuple("fOo", "FOO"),
        std::make_tuple("FOO", "FOO"),
        std::make_tuple("f23", "F23"),
        std::make_tuple("\"23f\"", "23f"),
        std::make_tuple("foo_", "FOO_"),
        std::make_tuple("foo_1", "FOO_1"),
        std::make_tuple("_foo", "_FOO"),
        std::make_tuple("__foo", "__FOO"),
        std::make_tuple("\"FOO\"", "FOO"),
        std::make_tuple("\"foo\"", "foo"),
        std::make_tuple("\"fOo\"", "fOo"),
        std::make_tuple("\"_foo\"", "_foo"),
        std::make_tuple("\"$foo\"", "$foo"),
        std::make_tuple("\"%foo\"", "%foo"),
        std::make_tuple("\"foo_\"", "foo_"),
        std::make_tuple("\"foo$\"", "foo$"),
        std::make_tuple("\"foo%\"", "foo%"),
        std::make_tuple("\"@#$\"", "@#$"),
        std::make_tuple("\"f.f\"", "f.f"),
        std::make_tuple("\"   \"", "   "),
        std::make_tuple("\"\xF0\x9F\x98\x85\"", "\xF0\x9F\x98\x85"),
        std::make_tuple("\"f\"\"f\"", "f\"f"),
        std::make_tuple("\"f\"\"\"\"f\"", "f\"\"f"),
        std::make_tuple("\"\"\"bar\"\"\"", "\"bar\""),
        std::make_tuple("\"\"\"\"\"bar\"\"\"", "\"\"bar\"")
    ),
    print_test_index<valid_simple_names_fixture>
);

class malformed_simple_name_fixture : public ::testing::TestWithParam<hidden> {};

TEST_P(malformed_simple_name_fixture, malformed_simple_name) {
    auto malformed = unhide(GetParam());

    EXPECT_THROW(
        {
            try {
                (void) qualified_name::create({}, malformed);
            } catch (const ignite_error &e) {
                EXPECT_EQ(e.get_status_code(), error::code::ILLEGAL_ARGUMENT);
                throw;
            }
        },
        ignite_error);
}

TEST_P(malformed_simple_name_fixture, malformed_schema_name) {
    auto malformed = unhide(GetParam());

    EXPECT_THROW(
        {
            try {
                (void) qualified_name::create(malformed, {});
            } catch (const ignite_error &e) {
                EXPECT_EQ(e.get_status_code(), error::code::ILLEGAL_ARGUMENT);
                throw;
            }
        },
        ignite_error);
}

TEST_P(malformed_simple_name_fixture, malformed_parse) {
    auto malformed = unhide(GetParam());

    EXPECT_THROW(
        {
            try {
                (void) qualified_name::parse(malformed);
            } catch (const ignite_error &e) {
                EXPECT_EQ(e.get_status_code(), error::code::ILLEGAL_ARGUMENT);
                throw;
            }
        },
        ignite_error);
}

INSTANTIATE_TEST_SUITE_P(
    client_qualified_name, malformed_simple_name_fixture,
    ::testing::Values(
        "",
        " ",
        ".f",
        "f.",
        ".",
        "f f",
        "1o0",
        "@#$",
        "foo$",
        "foo%",
        "foo&",
        "f😅",
        "😅f",
        "f\"f",
        "f\"\"f",
        "\"foo",
        "\"fo\"o\""
    ),
    print_test_index<malformed_simple_name_fixture>
);

class malformed_canonical_name_fixture : public ::testing::TestWithParam<hidden> {};

TEST_P(malformed_canonical_name_fixture, malformed_parse) {
    auto malformed = unhide(GetParam());

    EXPECT_THROW(
        {
            try {
                (void) qualified_name::parse(malformed);
            } catch (const ignite_error &e) {
                EXPECT_EQ(e.get_status_code(), error::code::ILLEGAL_ARGUMENT);
                throw;
            }
        },
        ignite_error);
}

INSTANTIATE_TEST_SUITE_P(
    client_qualified_name, malformed_canonical_name_fixture,
    ::testing::Values(
        "foo.",
        ".bar",
        ".",
        "foo..bar",
        "foo.bar.",
        "foo..",
        "@#$.bar",
        "foo.@#$",
        "@#$",
        "1oo.bar",
        "foo.1ar",
        "1oo"
    ),
    print_test_index<malformed_canonical_name_fixture>
);

class valid_simple_name_fixture : public ::testing::TestWithParam<std::tuple<hidden, hidden>> {};

TEST_P(valid_simple_name_fixture, valid_simple_name) {
    auto [to_parse, expected] = unhide(GetParam());

    auto parsed = qualified_name::parse(to_parse).get_object_name();
    auto created = qualified_name::create({}, to_parse).get_object_name();

    EXPECT_EQ(expected, parsed);
    EXPECT_EQ(expected, created);

    EXPECT_EQ(parsed, created);
}

INSTANTIATE_TEST_SUITE_P(
    client_qualified_name, valid_simple_name_fixture,
    ::testing::Values(
        std::make_tuple("foo", "FOO"),
        std::make_tuple("fOo", "FOO"),
        std::make_tuple("FOO", "FOO"),
        std::make_tuple("f23", "F23"),
        std::make_tuple("\"23f\"", "23f"),
        std::make_tuple("foo_", "FOO_"),
        std::make_tuple("foo_1", "FOO_1"),
        std::make_tuple("_foo", "_FOO"),
        std::make_tuple("__foo", "__FOO"),
        std::make_tuple("\"FOO\"", "FOO"),
        std::make_tuple("\"foo\"", "foo"),
        std::make_tuple("\"fOo\"", "fOo"),
        std::make_tuple("\"_foo\"", "_foo"),
        std::make_tuple("\"$foo\"", "$foo"),
        std::make_tuple("\"%foo\"", "%foo"),
        std::make_tuple("\"foo_\"", "foo_"),
        std::make_tuple("\"foo$\"", "foo$"),
        std::make_tuple("\"foo%\"", "foo%"),
        std::make_tuple("\"@#$\"", "@#$"),
        std::make_tuple("\"f.f\"", "f.f"),
        std::make_tuple("\"   \"", "   "),
        std::make_tuple("\"\xF0\x9F\x98\x85\"", "\xF0\x9F\x98\x85"),
        std::make_tuple("\"f\"\"f\"", "f\"f"),
        std::make_tuple("\"f\"\"\"\"f\"", "f\"\"f"),
        std::make_tuple("\"\"\"bar\"\"\"", "\"bar\""),
        std::make_tuple("\"\"\"\"\"bar\"\"\"", "\"\"bar\"")
    ),
    print_test_index<valid_simple_name_fixture>
);

class valid_canonical_name_fixture : public ::testing::TestWithParam<std::tuple<hidden, hidden, hidden>> {};

TEST_P(valid_canonical_name_fixture, valid_canonical_name) {
    auto [full, schema, object] = unhide(GetParam());

    auto parsed = qualified_name::parse(full);

    EXPECT_EQ(schema, parsed.get_schema_name());
    EXPECT_EQ(object, parsed.get_object_name());

    auto parsed2 = qualified_name::parse(parsed.get_canonical_name());

    EXPECT_EQ(schema, parsed2.get_schema_name());
    EXPECT_EQ(object, parsed2.get_object_name());
}

INSTANTIATE_TEST_SUITE_P(
    client_qualified_name, valid_canonical_name_fixture,
    ::testing::Values(
        std::make_tuple("\"foo.bar\".baz", "foo.bar", "BAZ"),
        std::make_tuple("foo.\"bar.baz\"", "FOO", "bar.baz"),
        std::make_tuple("\"foo.\"\"bar\"\"\".baz", "foo.\"bar\"", "BAZ"),
        std::make_tuple("foo.\"bar.\"\"baz\"", "FOO", "bar.\"baz"),
        std::make_tuple("_foo.bar", "_FOO", "BAR"),
        std::make_tuple("foo._bar", "FOO", "_BAR")
    ),
    print_test_index<valid_canonical_name_fixture>
);

class parsing_error_fixture : public ::testing::TestWithParam<std::tuple<hidden, hidden>> {};

TEST_P(parsing_error_fixture, parsing_error) {
    auto [name, exception] = unhide(GetParam());

    EXPECT_THROW(
        {
            try {
                (void) qualified_name::parse(name);
            } catch (const ignite_error &e) {
                EXPECT_EQ(e.get_status_code(), error::code::ILLEGAL_ARGUMENT);
                EXPECT_THAT(e.what_str(), testing::HasSubstr(exception));
                throw;
            }
        },
        ignite_error);
}

INSTANTIATE_TEST_SUITE_P(
    client_qualified_name, parsing_error_fixture,
    ::testing::Values(
        std::make_tuple("x.", "Object part of the canonical name can not be empty"),
        std::make_tuple(".x", "Invalid identifier start '46' : .x. Unquoted identifiers must begin with a letter or an underscore."),
        std::make_tuple("\"x", "Missing closing quote: '\"x"),
        std::make_tuple("y.\"x", "Missing closing quote: 'y.\"x"),
        std::make_tuple("\"xx\"yy\"", "Unexpected character '121' after quote: '\"xx\"yy\"'"),
        std::make_tuple("123", "Invalid identifier start '49' : 123. Unquoted identifiers must begin with a letter or an underscore."),
        std::make_tuple("x.y.z", "Canonical name should have at most two parts: 'x.y.z'")
    ),
    print_test_index<parsing_error_fixture>
);
