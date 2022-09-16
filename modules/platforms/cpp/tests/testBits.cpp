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

#include <boost/test/unit_test.hpp>

#include "common/Bits.h"

BOOST_AUTO_TEST_CASE(NumberOfLeadingZeroes)
{
    BOOST_CHECK_EQUAL(32, ignite::countLZero(0u));
    BOOST_CHECK_EQUAL(31, ignite::countLZero(1u));

    BOOST_CHECK_EQUAL(30, ignite::countLZero(2u));
    BOOST_CHECK_EQUAL(30, ignite::countLZero(3u));

    BOOST_CHECK_EQUAL(29, ignite::countLZero(4u));
    BOOST_CHECK_EQUAL(29, ignite::countLZero(5u));
    BOOST_CHECK_EQUAL(29, ignite::countLZero(7u));

    BOOST_CHECK_EQUAL(28, ignite::countLZero(8u));
    BOOST_CHECK_EQUAL(28, ignite::countLZero(12u));
    BOOST_CHECK_EQUAL(28, ignite::countLZero(15u));

    BOOST_CHECK_EQUAL(0, ignite::countLZero(0xFFFFFFFFu));
    BOOST_CHECK_EQUAL(0, ignite::countLZero(0x80000000u));

    BOOST_CHECK_EQUAL(1, ignite::countLZero(0x7FFFFFFFu));
    BOOST_CHECK_EQUAL(1, ignite::countLZero(0x40000000u));

    BOOST_CHECK_EQUAL(8, ignite::countLZero(0x00FFFFFFu));
    BOOST_CHECK_EQUAL(8, ignite::countLZero(0x00F00000u));
    BOOST_CHECK_EQUAL(8, ignite::countLZero(0x00800000u));

    BOOST_CHECK_EQUAL(9, ignite::countLZero(0x00700000u));
    BOOST_CHECK_EQUAL(9, ignite::countLZero(0x00400000u));
    BOOST_CHECK_EQUAL(9, ignite::countLZero(0x006C0395u));
}

BOOST_AUTO_TEST_CASE(NumberOfTrailingZeroes)
{
    BOOST_CHECK_EQUAL(0, ignite::countRZero(1u));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(3u));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(5u));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(7u));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(15u));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(0xFFFFFFFFu));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(0x7FFFFFFFu));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(0x00FFFFFFu));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(0x006C0395u));

    BOOST_CHECK_EQUAL(1, ignite::countRZero(2u));

    BOOST_CHECK_EQUAL(2, ignite::countRZero(4u));
    BOOST_CHECK_EQUAL(2, ignite::countRZero(12u));

    BOOST_CHECK_EQUAL(3, ignite::countRZero(8u));

    BOOST_CHECK_EQUAL(20, ignite::countRZero(0xFFF00000u));
    BOOST_CHECK_EQUAL(20, ignite::countRZero(0x00F00000u));
    BOOST_CHECK_EQUAL(20, ignite::countRZero(0x00700000u));
    BOOST_CHECK_EQUAL(20, ignite::countRZero(0x80700000u));

    BOOST_CHECK_EQUAL(22, ignite::countRZero(0x00400000u));
    BOOST_CHECK_EQUAL(22, ignite::countRZero(0x80400000u));
    BOOST_CHECK_EQUAL(22, ignite::countRZero(0x10400000u));

    BOOST_CHECK_EQUAL(23, ignite::countRZero(0x00800000u));
    BOOST_CHECK_EQUAL(23, ignite::countRZero(0x80800000u));
    BOOST_CHECK_EQUAL(23, ignite::countRZero(0xFF800000u));

    BOOST_CHECK_EQUAL(30, ignite::countRZero(0x40000000u));
    BOOST_CHECK_EQUAL(30, ignite::countRZero(0xC0000000u));

    BOOST_CHECK_EQUAL(31, ignite::countRZero(0x80000000u));

    BOOST_CHECK_EQUAL(32, ignite::countRZero(0u));
}

BOOST_AUTO_TEST_CASE(bitWidth) {
    BOOST_CHECK_EQUAL(0, ignite::bitWidth(uint8_t(0)));
    BOOST_CHECK_EQUAL(1, ignite::bitWidth(uint8_t(1)));
    BOOST_CHECK_EQUAL(2, ignite::bitWidth(uint8_t(2)));
    BOOST_CHECK_EQUAL(2, ignite::bitWidth(uint8_t(3)));
    BOOST_CHECK_EQUAL(3, ignite::bitWidth(uint8_t(4)));
    BOOST_CHECK_EQUAL(8, ignite::bitWidth(uint8_t(128)));
    BOOST_CHECK_EQUAL(8, ignite::bitWidth(uint8_t(255)));

    BOOST_CHECK_EQUAL(0, ignite::bitWidth(uint16_t(0)));
    BOOST_CHECK_EQUAL(1, ignite::bitWidth(uint16_t(1)));
    BOOST_CHECK_EQUAL(2, ignite::bitWidth(uint16_t(2)));
    BOOST_CHECK_EQUAL(2, ignite::bitWidth(uint16_t(3)));
    BOOST_CHECK_EQUAL(3, ignite::bitWidth(uint16_t(4)));
    BOOST_CHECK_EQUAL(8, ignite::bitWidth(uint16_t(128)));
    BOOST_CHECK_EQUAL(8, ignite::bitWidth(uint16_t(255)));
    BOOST_CHECK_EQUAL(12, ignite::bitWidth(uint16_t(0x0fff)));
    BOOST_CHECK_EQUAL(12, ignite::bitWidth(uint16_t(0x0fff)));
    BOOST_CHECK_EQUAL(16, ignite::bitWidth(uint16_t(0xffff)));
    BOOST_CHECK_EQUAL(16, ignite::bitWidth(uint16_t(0xffff)));

    BOOST_CHECK_EQUAL(0, ignite::bitWidth(uint32_t(0)));
    BOOST_CHECK_EQUAL(1, ignite::bitWidth(uint32_t(1)));
    BOOST_CHECK_EQUAL(2, ignite::bitWidth(uint32_t(2)));
    BOOST_CHECK_EQUAL(2, ignite::bitWidth(uint32_t(3)));
    BOOST_CHECK_EQUAL(3, ignite::bitWidth(uint32_t(4)));
    BOOST_CHECK_EQUAL(8, ignite::bitWidth(uint32_t(128)));
    BOOST_CHECK_EQUAL(8, ignite::bitWidth(uint32_t(255)));
    BOOST_CHECK_EQUAL(12, ignite::bitWidth(uint32_t(0x0800)));
    BOOST_CHECK_EQUAL(12, ignite::bitWidth(uint32_t(0x0fff)));
    BOOST_CHECK_EQUAL(16, ignite::bitWidth(uint32_t(0x8000)));
    BOOST_CHECK_EQUAL(16, ignite::bitWidth(uint32_t(0xffff)));
    BOOST_CHECK_EQUAL(24, ignite::bitWidth(uint32_t(0x00800000)));
    BOOST_CHECK_EQUAL(24, ignite::bitWidth(uint32_t(0x00ffffff)));
    BOOST_CHECK_EQUAL(32, ignite::bitWidth(uint32_t(0x80000000)));
    BOOST_CHECK_EQUAL(32, ignite::bitWidth(uint32_t(0xffffffff)));

    BOOST_CHECK_EQUAL(0, ignite::bitWidth(uint64_t(0)));
    BOOST_CHECK_EQUAL(1, ignite::bitWidth(uint64_t(1)));
    BOOST_CHECK_EQUAL(2, ignite::bitWidth(uint64_t(2)));
    BOOST_CHECK_EQUAL(2, ignite::bitWidth(uint64_t(3)));
    BOOST_CHECK_EQUAL(3, ignite::bitWidth(uint64_t(4)));
    BOOST_CHECK_EQUAL(8, ignite::bitWidth(uint64_t(128)));
    BOOST_CHECK_EQUAL(8, ignite::bitWidth(uint64_t(255)));
    BOOST_CHECK_EQUAL(12, ignite::bitWidth(uint64_t(0x0800)));
    BOOST_CHECK_EQUAL(12, ignite::bitWidth(uint64_t(0x0fff)));
    BOOST_CHECK_EQUAL(16, ignite::bitWidth(uint64_t(0x8000)));
    BOOST_CHECK_EQUAL(16, ignite::bitWidth(uint64_t(0xffff)));
    BOOST_CHECK_EQUAL(24, ignite::bitWidth(uint64_t(0x00800000)));
    BOOST_CHECK_EQUAL(24, ignite::bitWidth(uint64_t(0x00ffffff)));
    BOOST_CHECK_EQUAL(32, ignite::bitWidth(uint64_t(0x80000000)));
    BOOST_CHECK_EQUAL(32, ignite::bitWidth(uint64_t(0xffffffff)));
    BOOST_CHECK_EQUAL(48, ignite::bitWidth(uint64_t(0x0000800000000000)));
    BOOST_CHECK_EQUAL(48, ignite::bitWidth(uint64_t(0x0000ffffffffffff)));
    BOOST_CHECK_EQUAL(64, ignite::bitWidth(uint64_t(0x8000000000000000)));
    BOOST_CHECK_EQUAL(64, ignite::bitWidth(uint64_t(0xffffffffffffffff)));
}

BOOST_AUTO_TEST_CASE(bitFloor) {
    BOOST_CHECK_EQUAL(0, ignite::bitFloor(uint8_t(0)));
    BOOST_CHECK_EQUAL(1, ignite::bitFloor(uint8_t(1)));
    BOOST_CHECK_EQUAL(2, ignite::bitFloor(uint8_t(2)));
    BOOST_CHECK_EQUAL(2, ignite::bitFloor(uint8_t(3)));
    BOOST_CHECK_EQUAL(4, ignite::bitFloor(uint8_t(4)));
    BOOST_CHECK_EQUAL(4, ignite::bitFloor(uint8_t(5)));
    BOOST_CHECK_EQUAL(4, ignite::bitFloor(uint8_t(6)));
    BOOST_CHECK_EQUAL(4, ignite::bitFloor(uint8_t(7)));
    BOOST_CHECK_EQUAL(8, ignite::bitFloor(uint8_t(8)));
    BOOST_CHECK_EQUAL(8, ignite::bitFloor(uint8_t(9)));
    BOOST_CHECK_EQUAL(8, ignite::bitFloor(uint8_t(10)));
    BOOST_CHECK_EQUAL(8, ignite::bitFloor(uint8_t(15)));
    BOOST_CHECK_EQUAL(16, ignite::bitFloor(uint8_t(16)));
    BOOST_CHECK_EQUAL(128, ignite::bitFloor(uint8_t(128)));
    BOOST_CHECK_EQUAL(128, ignite::bitFloor(uint8_t(255)));

    BOOST_CHECK_EQUAL(0, ignite::bitFloor(uint16_t(0)));
    BOOST_CHECK_EQUAL(1, ignite::bitFloor(uint16_t(1)));
    BOOST_CHECK_EQUAL(2, ignite::bitFloor(uint16_t(2)));
    BOOST_CHECK_EQUAL(2, ignite::bitFloor(uint16_t(3)));
    BOOST_CHECK_EQUAL(4, ignite::bitFloor(uint16_t(4)));
    BOOST_CHECK_EQUAL(4, ignite::bitFloor(uint16_t(5)));
    BOOST_CHECK_EQUAL(4, ignite::bitFloor(uint16_t(6)));
    BOOST_CHECK_EQUAL(4, ignite::bitFloor(uint16_t(7)));
    BOOST_CHECK_EQUAL(8, ignite::bitFloor(uint16_t(8)));
    BOOST_CHECK_EQUAL(8, ignite::bitFloor(uint16_t(9)));
    BOOST_CHECK_EQUAL(8, ignite::bitFloor(uint16_t(10)));
    BOOST_CHECK_EQUAL(8, ignite::bitFloor(uint16_t(15)));
    BOOST_CHECK_EQUAL(16, ignite::bitFloor(uint16_t(16)));
    BOOST_CHECK_EQUAL(128, ignite::bitFloor(uint16_t(128)));
    BOOST_CHECK_EQUAL(128, ignite::bitFloor(uint16_t(255)));
    BOOST_CHECK_EQUAL(0x8000, ignite::bitFloor(uint16_t(0x8000)));
    BOOST_CHECK_EQUAL(0x8000, ignite::bitFloor(uint16_t(0xff00)));

    BOOST_CHECK_EQUAL(0, ignite::bitFloor(uint32_t(0)));
    BOOST_CHECK_EQUAL(1, ignite::bitFloor(uint32_t(1)));
    BOOST_CHECK_EQUAL(2, ignite::bitFloor(uint32_t(2)));
    BOOST_CHECK_EQUAL(2, ignite::bitFloor(uint32_t(3)));
    BOOST_CHECK_EQUAL(4, ignite::bitFloor(uint32_t(4)));
    BOOST_CHECK_EQUAL(4, ignite::bitFloor(uint32_t(5)));
    BOOST_CHECK_EQUAL(4, ignite::bitFloor(uint32_t(6)));
    BOOST_CHECK_EQUAL(4, ignite::bitFloor(uint32_t(7)));
    BOOST_CHECK_EQUAL(8, ignite::bitFloor(uint32_t(8)));
    BOOST_CHECK_EQUAL(8, ignite::bitFloor(uint32_t(9)));
    BOOST_CHECK_EQUAL(8, ignite::bitFloor(uint32_t(10)));
    BOOST_CHECK_EQUAL(8, ignite::bitFloor(uint32_t(15)));
    BOOST_CHECK_EQUAL(16, ignite::bitFloor(uint32_t(16)));
    BOOST_CHECK_EQUAL(128, ignite::bitFloor(uint32_t(128)));
    BOOST_CHECK_EQUAL(128, ignite::bitFloor(uint32_t(255)));
    BOOST_CHECK_EQUAL(0x8000, ignite::bitFloor(uint32_t(0x8000)));
    BOOST_CHECK_EQUAL(0x8000, ignite::bitFloor(uint32_t(0xff00)));
    BOOST_CHECK_EQUAL(0x80000000, ignite::bitFloor(uint32_t(0x80000000)));
    BOOST_CHECK_EQUAL(0x80000000, ignite::bitFloor(uint32_t(0xff000000)));

    BOOST_CHECK_EQUAL(0, ignite::bitFloor(uint64_t(0)));
    BOOST_CHECK_EQUAL(1, ignite::bitFloor(uint64_t(1)));
    BOOST_CHECK_EQUAL(2, ignite::bitFloor(uint64_t(2)));
    BOOST_CHECK_EQUAL(2, ignite::bitFloor(uint64_t(3)));
    BOOST_CHECK_EQUAL(4, ignite::bitFloor(uint64_t(4)));
    BOOST_CHECK_EQUAL(4, ignite::bitFloor(uint64_t(5)));
    BOOST_CHECK_EQUAL(4, ignite::bitFloor(uint64_t(6)));
    BOOST_CHECK_EQUAL(4, ignite::bitFloor(uint64_t(7)));
    BOOST_CHECK_EQUAL(8, ignite::bitFloor(uint64_t(8)));
    BOOST_CHECK_EQUAL(8, ignite::bitFloor(uint64_t(9)));
    BOOST_CHECK_EQUAL(8, ignite::bitFloor(uint64_t(10)));
    BOOST_CHECK_EQUAL(8, ignite::bitFloor(uint64_t(15)));
    BOOST_CHECK_EQUAL(16, ignite::bitFloor(uint64_t(16)));
    BOOST_CHECK_EQUAL(128, ignite::bitFloor(uint64_t(128)));
    BOOST_CHECK_EQUAL(128, ignite::bitFloor(uint64_t(255)));
    BOOST_CHECK_EQUAL(0x8000, ignite::bitFloor(uint64_t(0x8000)));
    BOOST_CHECK_EQUAL(0x8000, ignite::bitFloor(uint64_t(0xff00)));
    BOOST_CHECK_EQUAL(0x80000000, ignite::bitFloor(uint64_t(0x80000000)));
    BOOST_CHECK_EQUAL(0x80000000, ignite::bitFloor(uint64_t(0xff000000)));
    BOOST_CHECK_EQUAL(0x8000000000000000, ignite::bitFloor(uint64_t(0x8000000000000000)));
    BOOST_CHECK_EQUAL(0x8000000000000000, ignite::bitFloor(uint64_t(0xff00000000000000)));
}

BOOST_AUTO_TEST_CASE(bitCeil) {
    BOOST_CHECK_EQUAL(1, ignite::bitCeil(uint8_t(0)));
    BOOST_CHECK_EQUAL(1, ignite::bitCeil(uint8_t(1)));
    BOOST_CHECK_EQUAL(2, ignite::bitCeil(uint8_t(2)));
    BOOST_CHECK_EQUAL(4, ignite::bitCeil(uint8_t(3)));
    BOOST_CHECK_EQUAL(4, ignite::bitCeil(uint8_t(4)));
    BOOST_CHECK_EQUAL(8, ignite::bitCeil(uint8_t(5)));
    BOOST_CHECK_EQUAL(8, ignite::bitCeil(uint8_t(6)));
    BOOST_CHECK_EQUAL(8, ignite::bitCeil(uint8_t(7)));
    BOOST_CHECK_EQUAL(8, ignite::bitCeil(uint8_t(8)));
    BOOST_CHECK_EQUAL(16, ignite::bitCeil(uint8_t(9)));
    BOOST_CHECK_EQUAL(16, ignite::bitCeil(uint8_t(10)));
    BOOST_CHECK_EQUAL(16, ignite::bitCeil(uint8_t(15)));
    BOOST_CHECK_EQUAL(16, ignite::bitCeil(uint8_t(16)));
    BOOST_CHECK_EQUAL(128, ignite::bitCeil(uint8_t(65)));
    BOOST_CHECK_EQUAL(128, ignite::bitCeil(uint8_t(128)));

    BOOST_CHECK_EQUAL(1, ignite::bitCeil(uint16_t(0)));
    BOOST_CHECK_EQUAL(1, ignite::bitCeil(uint16_t(1)));
    BOOST_CHECK_EQUAL(2, ignite::bitCeil(uint16_t(2)));
    BOOST_CHECK_EQUAL(4, ignite::bitCeil(uint16_t(3)));
    BOOST_CHECK_EQUAL(4, ignite::bitCeil(uint16_t(4)));
    BOOST_CHECK_EQUAL(8, ignite::bitCeil(uint16_t(5)));
    BOOST_CHECK_EQUAL(8, ignite::bitCeil(uint16_t(6)));
    BOOST_CHECK_EQUAL(8, ignite::bitCeil(uint16_t(7)));
    BOOST_CHECK_EQUAL(8, ignite::bitCeil(uint16_t(8)));
    BOOST_CHECK_EQUAL(16, ignite::bitCeil(uint16_t(9)));
    BOOST_CHECK_EQUAL(16, ignite::bitCeil(uint16_t(10)));
    BOOST_CHECK_EQUAL(16, ignite::bitCeil(uint16_t(15)));
    BOOST_CHECK_EQUAL(16, ignite::bitCeil(uint16_t(16)));
    BOOST_CHECK_EQUAL(128, ignite::bitCeil(uint16_t(65)));
    BOOST_CHECK_EQUAL(128, ignite::bitCeil(uint16_t(128)));
    BOOST_CHECK_EQUAL(0x8000, ignite::bitCeil(uint16_t(0x4001)));
    BOOST_CHECK_EQUAL(0x8000, ignite::bitCeil(uint16_t(0x8000)));

    BOOST_CHECK_EQUAL(1, ignite::bitCeil(uint32_t(0)));
    BOOST_CHECK_EQUAL(1, ignite::bitCeil(uint32_t(1)));
    BOOST_CHECK_EQUAL(2, ignite::bitCeil(uint32_t(2)));
    BOOST_CHECK_EQUAL(4, ignite::bitCeil(uint32_t(3)));
    BOOST_CHECK_EQUAL(4, ignite::bitCeil(uint32_t(4)));
    BOOST_CHECK_EQUAL(8, ignite::bitCeil(uint32_t(5)));
    BOOST_CHECK_EQUAL(8, ignite::bitCeil(uint32_t(6)));
    BOOST_CHECK_EQUAL(8, ignite::bitCeil(uint32_t(7)));
    BOOST_CHECK_EQUAL(8, ignite::bitCeil(uint32_t(8)));
    BOOST_CHECK_EQUAL(16, ignite::bitCeil(uint32_t(9)));
    BOOST_CHECK_EQUAL(16, ignite::bitCeil(uint32_t(10)));
    BOOST_CHECK_EQUAL(16, ignite::bitCeil(uint32_t(15)));
    BOOST_CHECK_EQUAL(16, ignite::bitCeil(uint32_t(16)));
    BOOST_CHECK_EQUAL(128, ignite::bitCeil(uint32_t(65)));
    BOOST_CHECK_EQUAL(128, ignite::bitCeil(uint32_t(128)));
    BOOST_CHECK_EQUAL(0x8000, ignite::bitCeil(uint32_t(0x4001)));
    BOOST_CHECK_EQUAL(0x8000, ignite::bitCeil(uint32_t(0x8000)));
    BOOST_CHECK_EQUAL(0x80000000, ignite::bitCeil(uint32_t(0x40000001)));
    BOOST_CHECK_EQUAL(0x80000000, ignite::bitCeil(uint32_t(0x80000000)));

    BOOST_CHECK_EQUAL(1, ignite::bitCeil(uint64_t(0)));
    BOOST_CHECK_EQUAL(1, ignite::bitCeil(uint64_t(1)));
    BOOST_CHECK_EQUAL(2, ignite::bitCeil(uint64_t(2)));
    BOOST_CHECK_EQUAL(4, ignite::bitCeil(uint64_t(3)));
    BOOST_CHECK_EQUAL(4, ignite::bitCeil(uint64_t(4)));
    BOOST_CHECK_EQUAL(8, ignite::bitCeil(uint64_t(5)));
    BOOST_CHECK_EQUAL(8, ignite::bitCeil(uint64_t(6)));
    BOOST_CHECK_EQUAL(8, ignite::bitCeil(uint64_t(7)));
    BOOST_CHECK_EQUAL(8, ignite::bitCeil(uint64_t(8)));
    BOOST_CHECK_EQUAL(16, ignite::bitCeil(uint64_t(9)));
    BOOST_CHECK_EQUAL(16, ignite::bitCeil(uint64_t(10)));
    BOOST_CHECK_EQUAL(16, ignite::bitCeil(uint64_t(15)));
    BOOST_CHECK_EQUAL(16, ignite::bitCeil(uint64_t(16)));
    BOOST_CHECK_EQUAL(128, ignite::bitCeil(uint64_t(65)));
    BOOST_CHECK_EQUAL(128, ignite::bitCeil(uint64_t(128)));
    BOOST_CHECK_EQUAL(0x8000, ignite::bitCeil(uint64_t(0x4001)));
    BOOST_CHECK_EQUAL(0x8000, ignite::bitCeil(uint64_t(0x8000)));
    BOOST_CHECK_EQUAL(0x80000000, ignite::bitCeil(uint64_t(0x40000001)));
    BOOST_CHECK_EQUAL(0x80000000, ignite::bitCeil(uint64_t(0x80000000)));
    BOOST_CHECK_EQUAL(0x8000000000000000, ignite::bitCeil(uint64_t(0x4000000000000001)));
    BOOST_CHECK_EQUAL(0x8000000000000000, ignite::bitCeil(uint64_t(0x8000000000000000)));
}
