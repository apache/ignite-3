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
