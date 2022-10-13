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

#include "bits.h"

#include <gtest/gtest.h>

TEST(bits, countRZero) {
    EXPECT_EQ(8, ignite::countr_zero(uint8_t(0)));
    EXPECT_EQ(0, ignite::countr_zero(uint8_t(1)));
    EXPECT_EQ(1, ignite::countr_zero(uint8_t(2)));
    EXPECT_EQ(0, ignite::countr_zero(uint8_t(3)));
    EXPECT_EQ(2, ignite::countr_zero(uint8_t(4)));
    EXPECT_EQ(0, ignite::countr_zero(uint8_t(5)));
    EXPECT_EQ(1, ignite::countr_zero(uint8_t(6)));
    EXPECT_EQ(0, ignite::countr_zero(uint8_t(7)));
    EXPECT_EQ(3, ignite::countr_zero(uint8_t(8)));
    EXPECT_EQ(2, ignite::countr_zero(uint8_t(12)));
    EXPECT_EQ(0, ignite::countr_zero(uint8_t(15)));
    EXPECT_EQ(4, ignite::countr_zero(uint8_t(16)));
    EXPECT_EQ(7, ignite::countr_zero(uint8_t(128)));
    EXPECT_EQ(0, ignite::countr_zero(uint8_t(255)));

    EXPECT_EQ(16, ignite::countr_zero(uint16_t(0)));
    EXPECT_EQ(0, ignite::countr_zero(uint16_t(1)));
    EXPECT_EQ(1, ignite::countr_zero(uint16_t(2)));
    EXPECT_EQ(0, ignite::countr_zero(uint16_t(3)));
    EXPECT_EQ(2, ignite::countr_zero(uint16_t(4)));
    EXPECT_EQ(0, ignite::countr_zero(uint16_t(5)));
    EXPECT_EQ(1, ignite::countr_zero(uint16_t(6)));
    EXPECT_EQ(0, ignite::countr_zero(uint16_t(7)));
    EXPECT_EQ(3, ignite::countr_zero(uint16_t(8)));
    EXPECT_EQ(2, ignite::countr_zero(uint16_t(12)));
    EXPECT_EQ(0, ignite::countr_zero(uint16_t(15)));
    EXPECT_EQ(4, ignite::countr_zero(uint16_t(16)));
    EXPECT_EQ(7, ignite::countr_zero(uint16_t(128)));
    EXPECT_EQ(0, ignite::countr_zero(uint16_t(255)));
    EXPECT_EQ(11, ignite::countr_zero(uint16_t(0x0800)));
    EXPECT_EQ(0, ignite::countr_zero(uint16_t(0x0fff)));
    EXPECT_EQ(15, ignite::countr_zero(uint16_t(0x8000)));
    EXPECT_EQ(0, ignite::countr_zero(uint16_t(0xffff)));

    EXPECT_EQ(32, ignite::countr_zero(uint32_t(0)));
    EXPECT_EQ(0, ignite::countr_zero(uint32_t(1)));
    EXPECT_EQ(1, ignite::countr_zero(uint32_t(2)));
    EXPECT_EQ(0, ignite::countr_zero(uint32_t(3)));
    EXPECT_EQ(2, ignite::countr_zero(uint32_t(4)));
    EXPECT_EQ(0, ignite::countr_zero(uint32_t(5)));
    EXPECT_EQ(1, ignite::countr_zero(uint32_t(6)));
    EXPECT_EQ(0, ignite::countr_zero(uint32_t(7)));
    EXPECT_EQ(3, ignite::countr_zero(uint32_t(8)));
    EXPECT_EQ(2, ignite::countr_zero(uint32_t(12)));
    EXPECT_EQ(0, ignite::countr_zero(uint32_t(15)));
    EXPECT_EQ(4, ignite::countr_zero(uint32_t(16)));
    EXPECT_EQ(7, ignite::countr_zero(uint32_t(128)));
    EXPECT_EQ(0, ignite::countr_zero(uint32_t(255)));
    EXPECT_EQ(11, ignite::countr_zero(uint32_t(0x0800)));
    EXPECT_EQ(0, ignite::countr_zero(uint32_t(0x0fff)));
    EXPECT_EQ(15, ignite::countr_zero(uint32_t(0x8000)));
    EXPECT_EQ(0, ignite::countr_zero(uint32_t(0xffff)));
    EXPECT_EQ(23, ignite::countr_zero(uint32_t(0x00800000)));
    EXPECT_EQ(0, ignite::countr_zero(uint32_t(0x00ffffff)));
    EXPECT_EQ(31, ignite::countr_zero(uint32_t(0x80000000)));
    EXPECT_EQ(0, ignite::countr_zero(uint32_t(0xffffffff)));

    EXPECT_EQ(64, ignite::countr_zero(uint64_t(0)));
    EXPECT_EQ(0, ignite::countr_zero(uint64_t(1)));
    EXPECT_EQ(1, ignite::countr_zero(uint64_t(2)));
    EXPECT_EQ(0, ignite::countr_zero(uint64_t(3)));
    EXPECT_EQ(2, ignite::countr_zero(uint64_t(4)));
    EXPECT_EQ(0, ignite::countr_zero(uint64_t(5)));
    EXPECT_EQ(1, ignite::countr_zero(uint64_t(6)));
    EXPECT_EQ(0, ignite::countr_zero(uint64_t(7)));
    EXPECT_EQ(3, ignite::countr_zero(uint64_t(8)));
    EXPECT_EQ(2, ignite::countr_zero(uint64_t(12)));
    EXPECT_EQ(0, ignite::countr_zero(uint64_t(15)));
    EXPECT_EQ(4, ignite::countr_zero(uint64_t(16)));
    EXPECT_EQ(7, ignite::countr_zero(uint64_t(128)));
    EXPECT_EQ(0, ignite::countr_zero(uint64_t(255)));
    EXPECT_EQ(11, ignite::countr_zero(uint64_t(0x0800)));
    EXPECT_EQ(0, ignite::countr_zero(uint64_t(0x0fff)));
    EXPECT_EQ(15, ignite::countr_zero(uint64_t(0x8000)));
    EXPECT_EQ(0, ignite::countr_zero(uint64_t(0xffff)));
    EXPECT_EQ(31, ignite::countr_zero(uint64_t(0x80000000)));
    EXPECT_EQ(0, ignite::countr_zero(uint64_t(0xffffffff)));
    EXPECT_EQ(47, ignite::countr_zero(uint64_t(0x0000800000000000)));
    EXPECT_EQ(0, ignite::countr_zero(uint64_t(0x0000ffffffffffff)));
    EXPECT_EQ(63, ignite::countr_zero(uint64_t(0x8000000000000000)));
    EXPECT_EQ(0, ignite::countr_zero(uint64_t(0xffffffffffffffff)));
}

TEST(bits, countLZero) {
    EXPECT_EQ(8, ignite::countl_zero(uint8_t(0)));
    EXPECT_EQ(7, ignite::countl_zero(uint8_t(1)));
    EXPECT_EQ(6, ignite::countl_zero(uint8_t(2)));
    EXPECT_EQ(6, ignite::countl_zero(uint8_t(3)));
    EXPECT_EQ(5, ignite::countl_zero(uint8_t(4)));
    EXPECT_EQ(5, ignite::countl_zero(uint8_t(5)));
    EXPECT_EQ(5, ignite::countl_zero(uint8_t(6)));
    EXPECT_EQ(5, ignite::countl_zero(uint8_t(7)));
    EXPECT_EQ(4, ignite::countl_zero(uint8_t(8)));
    EXPECT_EQ(4, ignite::countl_zero(uint8_t(12)));
    EXPECT_EQ(4, ignite::countl_zero(uint8_t(15)));
    EXPECT_EQ(3, ignite::countl_zero(uint8_t(16)));
    EXPECT_EQ(0, ignite::countl_zero(uint8_t(128)));
    EXPECT_EQ(0, ignite::countl_zero(uint8_t(255)));

    EXPECT_EQ(16, ignite::countl_zero(uint16_t(0)));
    EXPECT_EQ(15, ignite::countl_zero(uint16_t(1)));
    EXPECT_EQ(14, ignite::countl_zero(uint16_t(2)));
    EXPECT_EQ(14, ignite::countl_zero(uint16_t(3)));
    EXPECT_EQ(13, ignite::countl_zero(uint16_t(4)));
    EXPECT_EQ(13, ignite::countl_zero(uint16_t(5)));
    EXPECT_EQ(13, ignite::countl_zero(uint16_t(6)));
    EXPECT_EQ(13, ignite::countl_zero(uint16_t(7)));
    EXPECT_EQ(12, ignite::countl_zero(uint16_t(8)));
    EXPECT_EQ(12, ignite::countl_zero(uint16_t(12)));
    EXPECT_EQ(12, ignite::countl_zero(uint16_t(15)));
    EXPECT_EQ(11, ignite::countl_zero(uint16_t(16)));
    EXPECT_EQ(8, ignite::countl_zero(uint16_t(128)));
    EXPECT_EQ(8, ignite::countl_zero(uint16_t(255)));
    EXPECT_EQ(4, ignite::countl_zero(uint16_t(0x0800)));
    EXPECT_EQ(4, ignite::countl_zero(uint16_t(0x0fff)));
    EXPECT_EQ(0, ignite::countl_zero(uint16_t(0x8000)));
    EXPECT_EQ(0, ignite::countl_zero(uint16_t(0xffff)));

    EXPECT_EQ(32, ignite::countl_zero(uint32_t(0)));
    EXPECT_EQ(31, ignite::countl_zero(uint32_t(1)));
    EXPECT_EQ(30, ignite::countl_zero(uint32_t(2)));
    EXPECT_EQ(30, ignite::countl_zero(uint32_t(3)));
    EXPECT_EQ(29, ignite::countl_zero(uint32_t(4)));
    EXPECT_EQ(29, ignite::countl_zero(uint32_t(5)));
    EXPECT_EQ(29, ignite::countl_zero(uint32_t(6)));
    EXPECT_EQ(29, ignite::countl_zero(uint32_t(7)));
    EXPECT_EQ(28, ignite::countl_zero(uint32_t(8)));
    EXPECT_EQ(28, ignite::countl_zero(uint32_t(12)));
    EXPECT_EQ(28, ignite::countl_zero(uint32_t(15)));
    EXPECT_EQ(27, ignite::countl_zero(uint32_t(16)));
    EXPECT_EQ(24, ignite::countl_zero(uint32_t(128)));
    EXPECT_EQ(24, ignite::countl_zero(uint32_t(255)));
    EXPECT_EQ(20, ignite::countl_zero(uint32_t(0x0800)));
    EXPECT_EQ(20, ignite::countl_zero(uint32_t(0x0fff)));
    EXPECT_EQ(16, ignite::countl_zero(uint32_t(0x8000)));
    EXPECT_EQ(16, ignite::countl_zero(uint32_t(0xffff)));
    EXPECT_EQ(8, ignite::countl_zero(uint32_t(0x00800000)));
    EXPECT_EQ(8, ignite::countl_zero(uint32_t(0x00ffffff)));
    EXPECT_EQ(0, ignite::countl_zero(uint32_t(0x80000000)));
    EXPECT_EQ(0, ignite::countl_zero(uint32_t(0xffffffff)));

    EXPECT_EQ(64, ignite::countl_zero(uint64_t(0)));
    EXPECT_EQ(63, ignite::countl_zero(uint64_t(1)));
    EXPECT_EQ(62, ignite::countl_zero(uint64_t(2)));
    EXPECT_EQ(62, ignite::countl_zero(uint64_t(3)));
    EXPECT_EQ(61, ignite::countl_zero(uint64_t(4)));
    EXPECT_EQ(61, ignite::countl_zero(uint64_t(5)));
    EXPECT_EQ(61, ignite::countl_zero(uint64_t(6)));
    EXPECT_EQ(61, ignite::countl_zero(uint64_t(7)));
    EXPECT_EQ(60, ignite::countl_zero(uint64_t(8)));
    EXPECT_EQ(60, ignite::countl_zero(uint64_t(12)));
    EXPECT_EQ(60, ignite::countl_zero(uint64_t(15)));
    EXPECT_EQ(59, ignite::countl_zero(uint64_t(16)));
    EXPECT_EQ(56, ignite::countl_zero(uint64_t(128)));
    EXPECT_EQ(56, ignite::countl_zero(uint64_t(255)));
    EXPECT_EQ(52, ignite::countl_zero(uint64_t(0x0800)));
    EXPECT_EQ(52, ignite::countl_zero(uint64_t(0x0fff)));
    EXPECT_EQ(48, ignite::countl_zero(uint64_t(0x8000)));
    EXPECT_EQ(48, ignite::countl_zero(uint64_t(0xffff)));
    EXPECT_EQ(40, ignite::countl_zero(uint64_t(0x00800000)));
    EXPECT_EQ(40, ignite::countl_zero(uint64_t(0x00ffffff)));
    EXPECT_EQ(32, ignite::countl_zero(uint64_t(0x80000000)));
    EXPECT_EQ(32, ignite::countl_zero(uint64_t(0xffffffff)));
    EXPECT_EQ(16, ignite::countl_zero(uint64_t(0x0000800000000000)));
    EXPECT_EQ(16, ignite::countl_zero(uint64_t(0x0000ffffffffffff)));
    EXPECT_EQ(0, ignite::countl_zero(uint64_t(0x8000000000000000)));
    EXPECT_EQ(0, ignite::countl_zero(uint64_t(0xffffffffffffffff)));
}

TEST(bits, bitWidth) {
    EXPECT_EQ(0, ignite::bit_width(uint8_t(0)));
    EXPECT_EQ(1, ignite::bit_width(uint8_t(1)));
    EXPECT_EQ(2, ignite::bit_width(uint8_t(2)));
    EXPECT_EQ(2, ignite::bit_width(uint8_t(3)));
    EXPECT_EQ(3, ignite::bit_width(uint8_t(4)));
    EXPECT_EQ(3, ignite::bit_width(uint8_t(5)));
    EXPECT_EQ(3, ignite::bit_width(uint8_t(6)));
    EXPECT_EQ(3, ignite::bit_width(uint8_t(7)));
    EXPECT_EQ(4, ignite::bit_width(uint8_t(8)));
    EXPECT_EQ(4, ignite::bit_width(uint8_t(12)));
    EXPECT_EQ(4, ignite::bit_width(uint8_t(15)));
    EXPECT_EQ(5, ignite::bit_width(uint8_t(16)));
    EXPECT_EQ(8, ignite::bit_width(uint8_t(128)));
    EXPECT_EQ(8, ignite::bit_width(uint8_t(255)));

    EXPECT_EQ(0, ignite::bit_width(uint16_t(0)));
    EXPECT_EQ(1, ignite::bit_width(uint16_t(1)));
    EXPECT_EQ(2, ignite::bit_width(uint16_t(2)));
    EXPECT_EQ(2, ignite::bit_width(uint16_t(3)));
    EXPECT_EQ(3, ignite::bit_width(uint16_t(4)));
    EXPECT_EQ(3, ignite::bit_width(uint16_t(5)));
    EXPECT_EQ(3, ignite::bit_width(uint16_t(6)));
    EXPECT_EQ(3, ignite::bit_width(uint16_t(7)));
    EXPECT_EQ(4, ignite::bit_width(uint16_t(8)));
    EXPECT_EQ(4, ignite::bit_width(uint16_t(12)));
    EXPECT_EQ(4, ignite::bit_width(uint16_t(15)));
    EXPECT_EQ(5, ignite::bit_width(uint16_t(16)));
    EXPECT_EQ(8, ignite::bit_width(uint16_t(128)));
    EXPECT_EQ(8, ignite::bit_width(uint16_t(255)));
    EXPECT_EQ(12, ignite::bit_width(uint16_t(0x0800)));
    EXPECT_EQ(12, ignite::bit_width(uint16_t(0x0fff)));
    EXPECT_EQ(16, ignite::bit_width(uint16_t(0x8000)));
    EXPECT_EQ(16, ignite::bit_width(uint16_t(0xffff)));

    EXPECT_EQ(0, ignite::bit_width(uint32_t(0)));
    EXPECT_EQ(1, ignite::bit_width(uint32_t(1)));
    EXPECT_EQ(2, ignite::bit_width(uint32_t(2)));
    EXPECT_EQ(2, ignite::bit_width(uint32_t(3)));
    EXPECT_EQ(3, ignite::bit_width(uint32_t(4)));
    EXPECT_EQ(3, ignite::bit_width(uint32_t(5)));
    EXPECT_EQ(3, ignite::bit_width(uint32_t(6)));
    EXPECT_EQ(3, ignite::bit_width(uint32_t(7)));
    EXPECT_EQ(4, ignite::bit_width(uint32_t(8)));
    EXPECT_EQ(4, ignite::bit_width(uint32_t(12)));
    EXPECT_EQ(4, ignite::bit_width(uint32_t(15)));
    EXPECT_EQ(5, ignite::bit_width(uint32_t(16)));
    EXPECT_EQ(8, ignite::bit_width(uint32_t(128)));
    EXPECT_EQ(8, ignite::bit_width(uint32_t(255)));
    EXPECT_EQ(12, ignite::bit_width(uint32_t(0x0800)));
    EXPECT_EQ(12, ignite::bit_width(uint32_t(0x0fff)));
    EXPECT_EQ(16, ignite::bit_width(uint32_t(0x8000)));
    EXPECT_EQ(16, ignite::bit_width(uint32_t(0xffff)));
    EXPECT_EQ(24, ignite::bit_width(uint32_t(0x00800000)));
    EXPECT_EQ(24, ignite::bit_width(uint32_t(0x00ffffff)));
    EXPECT_EQ(32, ignite::bit_width(uint32_t(0x80000000)));
    EXPECT_EQ(32, ignite::bit_width(uint32_t(0xffffffff)));

    EXPECT_EQ(0, ignite::bit_width(uint64_t(0)));
    EXPECT_EQ(1, ignite::bit_width(uint64_t(1)));
    EXPECT_EQ(2, ignite::bit_width(uint64_t(2)));
    EXPECT_EQ(2, ignite::bit_width(uint64_t(3)));
    EXPECT_EQ(3, ignite::bit_width(uint64_t(4)));
    EXPECT_EQ(3, ignite::bit_width(uint64_t(5)));
    EXPECT_EQ(3, ignite::bit_width(uint64_t(6)));
    EXPECT_EQ(3, ignite::bit_width(uint64_t(7)));
    EXPECT_EQ(4, ignite::bit_width(uint64_t(8)));
    EXPECT_EQ(4, ignite::bit_width(uint64_t(12)));
    EXPECT_EQ(4, ignite::bit_width(uint64_t(15)));
    EXPECT_EQ(5, ignite::bit_width(uint64_t(16)));
    EXPECT_EQ(8, ignite::bit_width(uint64_t(128)));
    EXPECT_EQ(8, ignite::bit_width(uint64_t(255)));
    EXPECT_EQ(12, ignite::bit_width(uint64_t(0x0800)));
    EXPECT_EQ(12, ignite::bit_width(uint64_t(0x0fff)));
    EXPECT_EQ(16, ignite::bit_width(uint64_t(0x8000)));
    EXPECT_EQ(16, ignite::bit_width(uint64_t(0xffff)));
    EXPECT_EQ(24, ignite::bit_width(uint64_t(0x00800000)));
    EXPECT_EQ(24, ignite::bit_width(uint64_t(0x00ffffff)));
    EXPECT_EQ(32, ignite::bit_width(uint64_t(0x80000000)));
    EXPECT_EQ(32, ignite::bit_width(uint64_t(0xffffffff)));
    EXPECT_EQ(48, ignite::bit_width(uint64_t(0x0000800000000000)));
    EXPECT_EQ(48, ignite::bit_width(uint64_t(0x0000ffffffffffff)));
    EXPECT_EQ(64, ignite::bit_width(uint64_t(0x8000000000000000)));
    EXPECT_EQ(64, ignite::bit_width(uint64_t(0xffffffffffffffff)));
}

TEST(bits, bitFloor) {
    EXPECT_EQ(0, ignite::bit_floor(uint8_t(0)));
    EXPECT_EQ(1, ignite::bit_floor(uint8_t(1)));
    EXPECT_EQ(2, ignite::bit_floor(uint8_t(2)));
    EXPECT_EQ(2, ignite::bit_floor(uint8_t(3)));
    EXPECT_EQ(4, ignite::bit_floor(uint8_t(4)));
    EXPECT_EQ(4, ignite::bit_floor(uint8_t(5)));
    EXPECT_EQ(4, ignite::bit_floor(uint8_t(6)));
    EXPECT_EQ(4, ignite::bit_floor(uint8_t(7)));
    EXPECT_EQ(8, ignite::bit_floor(uint8_t(8)));
    EXPECT_EQ(8, ignite::bit_floor(uint8_t(9)));
    EXPECT_EQ(8, ignite::bit_floor(uint8_t(10)));
    EXPECT_EQ(8, ignite::bit_floor(uint8_t(15)));
    EXPECT_EQ(16, ignite::bit_floor(uint8_t(16)));
    EXPECT_EQ(128, ignite::bit_floor(uint8_t(128)));
    EXPECT_EQ(128, ignite::bit_floor(uint8_t(255)));

    EXPECT_EQ(0, ignite::bit_floor(uint16_t(0)));
    EXPECT_EQ(1, ignite::bit_floor(uint16_t(1)));
    EXPECT_EQ(2, ignite::bit_floor(uint16_t(2)));
    EXPECT_EQ(2, ignite::bit_floor(uint16_t(3)));
    EXPECT_EQ(4, ignite::bit_floor(uint16_t(4)));
    EXPECT_EQ(4, ignite::bit_floor(uint16_t(5)));
    EXPECT_EQ(4, ignite::bit_floor(uint16_t(6)));
    EXPECT_EQ(4, ignite::bit_floor(uint16_t(7)));
    EXPECT_EQ(8, ignite::bit_floor(uint16_t(8)));
    EXPECT_EQ(8, ignite::bit_floor(uint16_t(9)));
    EXPECT_EQ(8, ignite::bit_floor(uint16_t(10)));
    EXPECT_EQ(8, ignite::bit_floor(uint16_t(15)));
    EXPECT_EQ(16, ignite::bit_floor(uint16_t(16)));
    EXPECT_EQ(128, ignite::bit_floor(uint16_t(128)));
    EXPECT_EQ(128, ignite::bit_floor(uint16_t(255)));
    EXPECT_EQ(0x8000, ignite::bit_floor(uint16_t(0x8000)));
    EXPECT_EQ(0x8000, ignite::bit_floor(uint16_t(0xffff)));

    EXPECT_EQ(0, ignite::bit_floor(uint32_t(0)));
    EXPECT_EQ(1, ignite::bit_floor(uint32_t(1)));
    EXPECT_EQ(2, ignite::bit_floor(uint32_t(2)));
    EXPECT_EQ(2, ignite::bit_floor(uint32_t(3)));
    EXPECT_EQ(4, ignite::bit_floor(uint32_t(4)));
    EXPECT_EQ(4, ignite::bit_floor(uint32_t(5)));
    EXPECT_EQ(4, ignite::bit_floor(uint32_t(6)));
    EXPECT_EQ(4, ignite::bit_floor(uint32_t(7)));
    EXPECT_EQ(8, ignite::bit_floor(uint32_t(8)));
    EXPECT_EQ(8, ignite::bit_floor(uint32_t(9)));
    EXPECT_EQ(8, ignite::bit_floor(uint32_t(10)));
    EXPECT_EQ(8, ignite::bit_floor(uint32_t(15)));
    EXPECT_EQ(16, ignite::bit_floor(uint32_t(16)));
    EXPECT_EQ(128, ignite::bit_floor(uint32_t(128)));
    EXPECT_EQ(128, ignite::bit_floor(uint32_t(255)));
    EXPECT_EQ(0x8000, ignite::bit_floor(uint32_t(0x8000)));
    EXPECT_EQ(0x8000, ignite::bit_floor(uint32_t(0xffff)));
    EXPECT_EQ(0x80000000, ignite::bit_floor(uint32_t(0x80000000)));
    EXPECT_EQ(0x80000000, ignite::bit_floor(uint32_t(0xffffffff)));

    EXPECT_EQ(0, ignite::bit_floor(uint64_t(0)));
    EXPECT_EQ(1, ignite::bit_floor(uint64_t(1)));
    EXPECT_EQ(2, ignite::bit_floor(uint64_t(2)));
    EXPECT_EQ(2, ignite::bit_floor(uint64_t(3)));
    EXPECT_EQ(4, ignite::bit_floor(uint64_t(4)));
    EXPECT_EQ(4, ignite::bit_floor(uint64_t(5)));
    EXPECT_EQ(4, ignite::bit_floor(uint64_t(6)));
    EXPECT_EQ(4, ignite::bit_floor(uint64_t(7)));
    EXPECT_EQ(8, ignite::bit_floor(uint64_t(8)));
    EXPECT_EQ(8, ignite::bit_floor(uint64_t(9)));
    EXPECT_EQ(8, ignite::bit_floor(uint64_t(10)));
    EXPECT_EQ(8, ignite::bit_floor(uint64_t(15)));
    EXPECT_EQ(16, ignite::bit_floor(uint64_t(16)));
    EXPECT_EQ(128, ignite::bit_floor(uint64_t(128)));
    EXPECT_EQ(128, ignite::bit_floor(uint64_t(255)));
    EXPECT_EQ(0x8000, ignite::bit_floor(uint64_t(0x8000)));
    EXPECT_EQ(0x8000, ignite::bit_floor(uint64_t(0xff00)));
    EXPECT_EQ(0x80000000, ignite::bit_floor(uint64_t(0x80000000)));
    EXPECT_EQ(0x80000000, ignite::bit_floor(uint64_t(0xffffffff)));
    EXPECT_EQ(0x8000000000000000, ignite::bit_floor(uint64_t(0x8000000000000000)));
    EXPECT_EQ(0x8000000000000000, ignite::bit_floor(uint64_t(0xffffffffffffffff)));
}

TEST(bits, bitCeil) {
    EXPECT_EQ(1, ignite::bit_ceil(uint8_t(0)));
    EXPECT_EQ(1, ignite::bit_ceil(uint8_t(1)));
    EXPECT_EQ(2, ignite::bit_ceil(uint8_t(2)));
    EXPECT_EQ(4, ignite::bit_ceil(uint8_t(3)));
    EXPECT_EQ(4, ignite::bit_ceil(uint8_t(4)));
    EXPECT_EQ(8, ignite::bit_ceil(uint8_t(5)));
    EXPECT_EQ(8, ignite::bit_ceil(uint8_t(6)));
    EXPECT_EQ(8, ignite::bit_ceil(uint8_t(7)));
    EXPECT_EQ(8, ignite::bit_ceil(uint8_t(8)));
    EXPECT_EQ(16, ignite::bit_ceil(uint8_t(9)));
    EXPECT_EQ(16, ignite::bit_ceil(uint8_t(10)));
    EXPECT_EQ(16, ignite::bit_ceil(uint8_t(15)));
    EXPECT_EQ(16, ignite::bit_ceil(uint8_t(16)));
    EXPECT_EQ(128, ignite::bit_ceil(uint8_t(65)));
    EXPECT_EQ(128, ignite::bit_ceil(uint8_t(128)));

    EXPECT_EQ(1, ignite::bit_ceil(uint16_t(0)));
    EXPECT_EQ(1, ignite::bit_ceil(uint16_t(1)));
    EXPECT_EQ(2, ignite::bit_ceil(uint16_t(2)));
    EXPECT_EQ(4, ignite::bit_ceil(uint16_t(3)));
    EXPECT_EQ(4, ignite::bit_ceil(uint16_t(4)));
    EXPECT_EQ(8, ignite::bit_ceil(uint16_t(5)));
    EXPECT_EQ(8, ignite::bit_ceil(uint16_t(6)));
    EXPECT_EQ(8, ignite::bit_ceil(uint16_t(7)));
    EXPECT_EQ(8, ignite::bit_ceil(uint16_t(8)));
    EXPECT_EQ(16, ignite::bit_ceil(uint16_t(9)));
    EXPECT_EQ(16, ignite::bit_ceil(uint16_t(10)));
    EXPECT_EQ(16, ignite::bit_ceil(uint16_t(15)));
    EXPECT_EQ(16, ignite::bit_ceil(uint16_t(16)));
    EXPECT_EQ(128, ignite::bit_ceil(uint16_t(65)));
    EXPECT_EQ(128, ignite::bit_ceil(uint16_t(128)));
    EXPECT_EQ(0x8000, ignite::bit_ceil(uint16_t(0x4001)));
    EXPECT_EQ(0x8000, ignite::bit_ceil(uint16_t(0x8000)));

    EXPECT_EQ(1, ignite::bit_ceil(uint32_t(0)));
    EXPECT_EQ(1, ignite::bit_ceil(uint32_t(1)));
    EXPECT_EQ(2, ignite::bit_ceil(uint32_t(2)));
    EXPECT_EQ(4, ignite::bit_ceil(uint32_t(3)));
    EXPECT_EQ(4, ignite::bit_ceil(uint32_t(4)));
    EXPECT_EQ(8, ignite::bit_ceil(uint32_t(5)));
    EXPECT_EQ(8, ignite::bit_ceil(uint32_t(6)));
    EXPECT_EQ(8, ignite::bit_ceil(uint32_t(7)));
    EXPECT_EQ(8, ignite::bit_ceil(uint32_t(8)));
    EXPECT_EQ(16, ignite::bit_ceil(uint32_t(9)));
    EXPECT_EQ(16, ignite::bit_ceil(uint32_t(10)));
    EXPECT_EQ(16, ignite::bit_ceil(uint32_t(15)));
    EXPECT_EQ(16, ignite::bit_ceil(uint32_t(16)));
    EXPECT_EQ(128, ignite::bit_ceil(uint32_t(65)));
    EXPECT_EQ(128, ignite::bit_ceil(uint32_t(128)));
    EXPECT_EQ(0x8000, ignite::bit_ceil(uint32_t(0x4001)));
    EXPECT_EQ(0x8000, ignite::bit_ceil(uint32_t(0x8000)));
    EXPECT_EQ(0x80000000, ignite::bit_ceil(uint32_t(0x40000001)));
    EXPECT_EQ(0x80000000, ignite::bit_ceil(uint32_t(0x80000000)));

    EXPECT_EQ(1, ignite::bit_ceil(uint64_t(0)));
    EXPECT_EQ(1, ignite::bit_ceil(uint64_t(1)));
    EXPECT_EQ(2, ignite::bit_ceil(uint64_t(2)));
    EXPECT_EQ(4, ignite::bit_ceil(uint64_t(3)));
    EXPECT_EQ(4, ignite::bit_ceil(uint64_t(4)));
    EXPECT_EQ(8, ignite::bit_ceil(uint64_t(5)));
    EXPECT_EQ(8, ignite::bit_ceil(uint64_t(6)));
    EXPECT_EQ(8, ignite::bit_ceil(uint64_t(7)));
    EXPECT_EQ(8, ignite::bit_ceil(uint64_t(8)));
    EXPECT_EQ(16, ignite::bit_ceil(uint64_t(9)));
    EXPECT_EQ(16, ignite::bit_ceil(uint64_t(10)));
    EXPECT_EQ(16, ignite::bit_ceil(uint64_t(15)));
    EXPECT_EQ(16, ignite::bit_ceil(uint64_t(16)));
    EXPECT_EQ(128, ignite::bit_ceil(uint64_t(65)));
    EXPECT_EQ(128, ignite::bit_ceil(uint64_t(128)));
    EXPECT_EQ(0x8000, ignite::bit_ceil(uint64_t(0x4001)));
    EXPECT_EQ(0x8000, ignite::bit_ceil(uint64_t(0x8000)));
    EXPECT_EQ(0x80000000, ignite::bit_ceil(uint64_t(0x40000001)));
    EXPECT_EQ(0x80000000, ignite::bit_ceil(uint64_t(0x80000000)));
    EXPECT_EQ(0x8000000000000000, ignite::bit_ceil(uint64_t(0x4000000000000001)));
    EXPECT_EQ(0x8000000000000000, ignite::bit_ceil(uint64_t(0x8000000000000000)));
}
