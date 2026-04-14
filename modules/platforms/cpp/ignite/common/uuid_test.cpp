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

#include "uuid.h"

#include <gtest/gtest.h>

#include <sstream>

TEST(uuid, construct) {
    {
        ignite::uuid uuid;
        EXPECT_EQ(0, uuid.get_most_significant_bits());
        EXPECT_EQ(0, uuid.get_least_significant_bits());
    }
    {
        ignite::uuid uuid(1, 2);
        EXPECT_EQ(1, uuid.get_most_significant_bits());
        EXPECT_EQ(2, uuid.get_least_significant_bits());
    }
    {
        ignite::uuid uuid(1, 2);
        ignite::uuid uuid2(uuid);
        EXPECT_EQ(1, uuid2.get_most_significant_bits());
        EXPECT_EQ(2, uuid2.get_least_significant_bits());
        EXPECT_EQ(uuid, uuid2);
    }
}

TEST(uuid, stream) {
    std::string uuidString = "123e4567-e89b-12d3-a456-426614174000";

    std::stringstream stream;
    stream << uuidString;

    ignite::uuid uuid;
    stream >> uuid;

    EXPECT_EQ(0x123e4567e89b12d3, uuid.get_most_significant_bits());
    EXPECT_EQ(0xa456426614174000, uuid.get_least_significant_bits());

    EXPECT_EQ(1, uuid.version());
    EXPECT_EQ(2, uuid.variant());

    std::stringstream stream2;
    stream2 << uuid;

    std::string uuidString2;
    stream2 >> uuidString2;

    EXPECT_EQ(uuidString, uuidString2);
}

TEST(uuid, incorrect_string_representation) {
    auto uuidText = "foo";

    auto resOpt = ignite::uuid::from_string(uuidText);

    EXPECT_FALSE(resOpt.has_value());
}

class uuid_string_presentation_fixture: public ::testing::TestWithParam<std::tuple<std::string, ignite::uuid>> {};

TEST_P(uuid_string_presentation_fixture, from_string) {
    auto [uuidText, uuid] = GetParam();

    auto resOpt = ignite::uuid::from_string(uuidText);

    ASSERT_TRUE(resOpt.has_value());

    EXPECT_EQ(uuid.get_most_significant_bits(), resOpt->get_most_significant_bits());
    EXPECT_EQ(uuid.get_least_significant_bits(), resOpt->get_least_significant_bits());
}

// We check that two representation are consistent both ways.
TEST_P(uuid_string_presentation_fixture, to_string) {
    auto [uuidText, uuid] = GetParam();

    std::stringstream ss;
    ss << uuid;

    EXPECT_EQ(uuidText, ss.str());
}

TEST_P(uuid_string_presentation_fixture, circular_convertation0) {
    auto [uuidText, uuid] = GetParam();

    std::stringstream ss;
    ss << uuid;

    auto convertedText = ss.str();

    auto converterUuid = ignite::uuid::from_string(convertedText);

    EXPECT_EQ(converterUuid, uuid);
}

TEST_P(uuid_string_presentation_fixture, circular_convertation1) {
    auto [uuidText, uuid] = GetParam();

    auto converterUuid = ignite::uuid::from_string(uuidText);

    ASSERT_TRUE(converterUuid.has_value());

    std::stringstream ss;
    ss << *converterUuid;

    auto convertedText = ss.str();

    EXPECT_EQ(convertedText, uuidText);
}

// Values has taken randomly from java program, as this implementation should have same behaviour as java.lang.UUID for compatibility reasons.
INSTANTIATE_TEST_SUITE_P(
    various_uuids,
    uuid_string_presentation_fixture,
    ::testing::Values(
        std::tuple{"4b62e46a-d380-460f-94ea-9b4320752634", ignite::uuid{5432155248028304911LL, -7716184298936261068LL}},
        std::tuple{"cabf7114-7763-4ad5-b312-5ffadcbf48c8", ignite::uuid{-3837224024780092715, -5543262660289673016}},
        std::tuple{"8ee62c79-5636-44b9-adc4-c9b4272a2ba6", ignite::uuid{-8149777576031271751, -5925389434124358746}},
        std::tuple{"3e69a946-fc2a-4da1-9938-d1f082dd36ce", ignite::uuid{4497311825249586593, -7405938756292888882}},
        std::tuple{"7cc9f130-821b-4580-bfda-955072540643", ignite::uuid{8991983321665455488, -4622217894794361277}},
        std::tuple{"1f86b0b1-dfab-4108-8b7e-8b3e4177ceb0", ignite::uuid{2271697340063236360, -8395119555869421904}},
        std::tuple{"88cc69d9-d191-482d-b295-aa458cf99167", ignite::uuid{-8589374005057599443, -5578365347733859993}},
        std::tuple{"a8b6298c-126e-4c4d-a521-de3989a31492", ignite::uuid{-6289794147994940339, -6547708044516322158}},
        std::tuple{"03b210a8-5a6a-4cae-b4fc-f30df9f1eebd", ignite::uuid{266293643225746606, -5405178211397931331}},
        std::tuple{"f145e277-bb58-4f89-adfa-3166a975b71b", ignite::uuid{-1061193133303771255, -5910357243970865381}},
        std::tuple{"dc8b5857-067f-486e-aa5b-52cbdb26dd9c", ignite::uuid{-2554851232808220562, -6171247828872536676}}
    )
);