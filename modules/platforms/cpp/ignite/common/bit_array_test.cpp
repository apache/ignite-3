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

#include "bit_array.h"

#include <gtest/gtest.h>

using namespace ignite;

void check_index_out_of_bounds(bit_array &array, std::int32_t index) {
    std::string error_string =
        "Index is out of bounds: index=" + std::to_string(index) + ", size=" + std::to_string(array.get_size());

    EXPECT_THROW(
        {
            try {
                (void) array.test(index);
            } catch (const ignite_error &e) {
                EXPECT_EQ(error_string, e.what());
                throw;
            }
        },
        ignite_error);
    EXPECT_THROW(
        {
            try {
                array.set(index, true);
            } catch (const ignite_error &e) {
                EXPECT_EQ(error_string, e.what());
                throw;
            }
        },
        ignite_error);

    EXPECT_THROW(
        {
            try {
                array.set(index, false);
            } catch (const ignite_error &e) {
                EXPECT_EQ(error_string, e.what());
                throw;
            }
        },
        ignite_error);
}

TEST(bit_array, empty) {
    bit_array empty;

    EXPECT_TRUE(empty.is_empty());
    EXPECT_TRUE(empty.get_raw().empty());

    EXPECT_EQ(empty.get_size(), 0);
    EXPECT_EQ(empty.get_raw().size(), 0);

    check_index_out_of_bounds(empty, 0);
    check_index_out_of_bounds(empty, 1);
    check_index_out_of_bounds(empty, -1);
}

TEST(bit_array, one_false) {
    bit_array value(1);

    EXPECT_TRUE(!value.is_empty());
    EXPECT_TRUE(!value.get_raw().empty());

    EXPECT_EQ(value.get_size(), 1);
    EXPECT_EQ(value.get_raw().size(), 1);

    EXPECT_FALSE(value.test(0));
    value.set(0, true);
    EXPECT_TRUE(value.test(0));
    value.set(0, true);
    EXPECT_TRUE(value.test(0));

    check_index_out_of_bounds(value, 1);
    check_index_out_of_bounds(value, -1);
}

TEST(bit_array, one_true) {
    bit_array value(1, true);

    EXPECT_TRUE(!value.is_empty());
    EXPECT_TRUE(!value.get_raw().empty());

    EXPECT_EQ(value.get_size(), 1);
    EXPECT_EQ(value.get_raw().size(), 1);

    EXPECT_TRUE(value.test(0));
    value.set(0, false);
    EXPECT_FALSE(value.test(0));
    value.set(0, false);
    EXPECT_FALSE(value.test(0));

    check_index_out_of_bounds(value, 1);
    check_index_out_of_bounds(value, -1);
}

TEST(bit_array, different_short) {
    bit_array value(8);

    EXPECT_TRUE(!value.is_empty());
    EXPECT_TRUE(!value.get_raw().empty());

    EXPECT_EQ(value.get_size(), 8);
    EXPECT_EQ(value.get_raw().size(), 1);

    for (int i = 0; i < 8; ++i)
        EXPECT_FALSE(value.test(i));

    for (int i = 0; i < 8; i += 2) {
        value.set(i, true);
        EXPECT_TRUE(value.test(i));
    }

    for (int i = 1; i < 8; i += 2)
        EXPECT_FALSE(value.test(i));

    check_index_out_of_bounds(value, 8);
    check_index_out_of_bounds(value, -1);
}

TEST(bit_array, different_long) {
    constexpr auto size = 387;
    bit_array value(size);

    EXPECT_TRUE(!value.is_empty());
    EXPECT_TRUE(!value.get_raw().empty());

    EXPECT_EQ(value.get_size(), size);
    EXPECT_EQ(value.get_raw().size(), (size + 7) / 8);

    for (int step = 1; step < size; ++step) {
        for (int i = 0; i < size; ++i) {
            value.set(i, false);
            EXPECT_FALSE(value.test(i));
        }

        for (int i = 0; i < size; i += step) {
            value.set(i, true);
            EXPECT_TRUE(value.test(i));
        }

        for (int i = 0; i < size; ++i) {
            if (i % step == 0)
                EXPECT_TRUE(value.test(i));
            else
                EXPECT_FALSE(value.test(i));
        }
    }

    check_index_out_of_bounds(value, 387);
    check_index_out_of_bounds(value, -1);
}

TEST(bit_array, from_raw_empty) {
    std::vector<std::byte> raw;
    bit_array value(raw);

    EXPECT_TRUE(value.is_empty());
    EXPECT_TRUE(value.get_raw().empty());

    EXPECT_EQ(value.get_size(), 0);
    EXPECT_EQ(value.get_raw().size(), 0);

    check_index_out_of_bounds(value, 0);
    check_index_out_of_bounds(value, -1);
}

TEST(bit_array, from_raw) {
    std::vector<std::byte> raw;
    raw.push_back(std::byte(0));
    raw.push_back(std::byte(1));
    raw.push_back(std::byte(2));
    raw.push_back(std::byte(0xFF));

    constexpr auto bit_size = 8 * 3 + 5;

    bit_array value(raw, bit_size);

    EXPECT_TRUE(!value.is_empty());
    EXPECT_TRUE(!value.get_raw().empty());

    EXPECT_EQ(value.get_size(), bit_size);
    EXPECT_EQ(value.get_raw().size(), 4);

    for (int i = 0; i < bit_size; ++i) {
        if (i == 8 || i == 17 || i > 23)
            EXPECT_TRUE(value.test(i));
        else
            EXPECT_FALSE(value.test(i));
    }

    check_index_out_of_bounds(value, bit_size);
    check_index_out_of_bounds(value, bit_size + 1);
    check_index_out_of_bounds(value, -1);
}

TEST(bit_array, to_raw) {
    constexpr auto bit_size = 8 * 3 + 5;

    bit_array value(bit_size);

    EXPECT_TRUE(!value.is_empty());
    EXPECT_TRUE(!value.get_raw().empty());

    EXPECT_EQ(value.get_size(), bit_size);

    for (int i = 0; i < bit_size; ++i)
        value.set(i, i == 8 || i == 17 || i > 23);

    check_index_out_of_bounds(value, bit_size);
    check_index_out_of_bounds(value, bit_size + 1);
    check_index_out_of_bounds(value, -1);

    auto raw = value.get_raw();

    EXPECT_EQ(value.get_raw().size(), 4);

    EXPECT_EQ(raw[0], std::byte(0));
    EXPECT_EQ(raw[1], std::byte(1));
    EXPECT_EQ(raw[2], std::byte(2));
    EXPECT_EQ(raw[3] & std::byte(0x1F), std::byte(0x1F));
}
