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

#include "ignite/client/detail/table/name_utils.h"
#include "ignite/client/table/qualified_name.h"
#include "ignite/common/ignite_error.h"
#include "tests/test-common/hidden_param.h"

#include <gtest/gtest.h>

using namespace ignite;
using namespace detail;

class quote_if_needed_fixture : public ::testing::TestWithParam<std::tuple<hidden, hidden>> {};

TEST_P(quote_if_needed_fixture, quote_if_needed) {
    auto [name, expected] = unhide(GetParam());

    EXPECT_EQ(expected, quote_if_needed(name, qualified_name::QUOTE_CHAR));
    EXPECT_EQ(name, parse_identifier(quote_if_needed(
        name, qualified_name::QUOTE_CHAR), qualified_name::QUOTE_CHAR, qualified_name::SEPARATOR_CHAR));
}


INSTANTIATE_TEST_SUITE_P(
    client_name_utils, quote_if_needed_fixture,
    ::testing::Values(
        std::make_tuple("foo", "\"foo\""),
        std::make_tuple("fOo", "\"fOo\""),
        std::make_tuple("FOO", "FOO"),
        std::make_tuple("_FOO", "_FOO"),
        std::make_tuple("_", "_"),
        std::make_tuple("__", "__"),
        std::make_tuple("_\xC2\xB7", "_\xC2\xB7"),
        std::make_tuple("A\xCC\x80", "A\xCC\x80"),
        std::make_tuple("1o0", "\"1o0\""),
        std::make_tuple("@#$", "\"@#$\""),
        std::make_tuple("f16", "\"f16\""),
        std::make_tuple("F16", "F16"),
        std::make_tuple("Ff16", "\"Ff16\""),
        std::make_tuple("FF16", "FF16"),
        std::make_tuple(" ", "\" \""),
        std::make_tuple(" F", "\" F\""),
        std::make_tuple(" ,", "\" ,\""),
        std::make_tuple("\xF0\x9F\x98\x85", "\"\xF0\x9F\x98\x85\""),
        std::make_tuple("\"foo\"", "\"\"\"foo\"\"\""),
        std::make_tuple("\"fOo\"", "\"\"\"fOo\"\"\""),
        std::make_tuple("\"f.f\"", "\"\"\"f.f\"\"\""),
        std::make_tuple("foo\"bar\"", "\"foo\"\"bar\"\"\""),
        std::make_tuple("foo\"bar", "\"foo\"\"bar\"")
    ),
    print_test_index<quote_if_needed_fixture>
);


class malformed_identifiers_fixture : public ::testing::TestWithParam<hidden> {};

TEST_P(malformed_identifiers_fixture, parse_identifier_malformed) {
    auto malformed = unhide(GetParam());

    EXPECT_THROW(
        {
            try {
                (void) parse_identifier(malformed, qualified_name::QUOTE_CHAR, qualified_name::SEPARATOR_CHAR);
            } catch (const ignite_error &e) {
                EXPECT_EQ(e.get_status_code(), error::code::ILLEGAL_ARGUMENT);
                throw;
            }
        },
        ignite_error);
}

INSTANTIATE_TEST_SUITE_P(
    client_name_utils, malformed_identifiers_fixture,
    ::testing::Values(
        " ",
        "foo-1",
        "f.f",
        "f f",
        "f\"f",
        "f\"\"f",
        "\"foo",
        "\"fo\"o\"",
        "1o0",
        "@#$",
        "\xF0\x9F\x98\x85",
        "f\xF0\x9F\x98\x85",
        "A\xF0",
        "$foo",
        "foo$"
    ),
    print_test_index<malformed_identifiers_fixture>
);


class valid_identifiers_fixture : public ::testing::TestWithParam<std::tuple<hidden, hidden>> {};

TEST_P(valid_identifiers_fixture, parse_identifier_valid) {
    auto [name, expected] = unhide(GetParam());

    EXPECT_EQ(name, parse_identifier(quote_if_needed(
        name, qualified_name::QUOTE_CHAR), qualified_name::QUOTE_CHAR, qualified_name::SEPARATOR_CHAR));
}

INSTANTIATE_TEST_SUITE_P(
    client_name_utils, valid_identifiers_fixture,
    ::testing::Values(
        std::make_tuple("foo", "FOO"),
        std::make_tuple("fOo", "FOO"),
        std::make_tuple("FOO", "FOO"),
        std::make_tuple("fo_o", "FO_O"),
        std::make_tuple("_foo", "_FOO"),
        std::make_tuple("_\xC2\xB7", "_\xC2\xB7"),
        std::make_tuple("A\xCC\x80", "A\xCC\x80"),
        std::make_tuple("\"FOO\"", "FOO"),
        std::make_tuple("\"foo\"", "foo"),
        std::make_tuple("\"fOo\"", "fOo"),
        std::make_tuple("\"$fOo\"", "$fOo"),
        std::make_tuple("\"f.f\"", "f.f"),
        std::make_tuple("\"f\"\"f\"", "f\"f"),
        std::make_tuple("\" \"", " "),
        std::make_tuple("\"   \"", "   "),
        std::make_tuple("\",\"", ","),
        std::make_tuple("\"\xF0\x9F\x98\x85\"", "\xF0\x9F\x98\x85"),
        std::make_tuple("\"f\xF0\x9F\x98\x85\"", "f\xF0\x9F\x98\x85")
    ),
    print_test_index<valid_identifiers_fixture>
);

