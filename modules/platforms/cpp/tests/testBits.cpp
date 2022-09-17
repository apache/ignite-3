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

BOOST_AUTO_TEST_CASE(countRZero)
{
    BOOST_CHECK_EQUAL(8, ignite::countRZero(uint8_t(0)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint8_t(1)));
    BOOST_CHECK_EQUAL(1, ignite::countRZero(uint8_t(2)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint8_t(3)));
    BOOST_CHECK_EQUAL(2, ignite::countRZero(uint8_t(4)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint8_t(5)));
    BOOST_CHECK_EQUAL(1, ignite::countRZero(uint8_t(6)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint8_t(7)));
    BOOST_CHECK_EQUAL(3, ignite::countRZero(uint8_t(8)));
    BOOST_CHECK_EQUAL(2, ignite::countRZero(uint8_t(12)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint8_t(15)));
    BOOST_CHECK_EQUAL(4, ignite::countRZero(uint8_t(16)));
    BOOST_CHECK_EQUAL(7, ignite::countRZero(uint8_t(128)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint8_t(255)));

    BOOST_CHECK_EQUAL(16, ignite::countRZero(uint16_t(0)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint16_t(1)));
    BOOST_CHECK_EQUAL(1, ignite::countRZero(uint16_t(2)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint16_t(3)));
    BOOST_CHECK_EQUAL(2, ignite::countRZero(uint16_t(4)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint16_t(5)));
    BOOST_CHECK_EQUAL(1, ignite::countRZero(uint16_t(6)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint16_t(7)));
    BOOST_CHECK_EQUAL(3, ignite::countRZero(uint16_t(8)));
    BOOST_CHECK_EQUAL(2, ignite::countRZero(uint16_t(12)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint16_t(15)));
    BOOST_CHECK_EQUAL(4, ignite::countRZero(uint16_t(16)));
    BOOST_CHECK_EQUAL(7, ignite::countRZero(uint16_t(128)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint16_t(255)));
    BOOST_CHECK_EQUAL(11, ignite::countRZero(uint16_t(0x0800)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint16_t(0x0fff)));
    BOOST_CHECK_EQUAL(15, ignite::countRZero(uint16_t(0x8000)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint16_t(0xffff)));

    BOOST_CHECK_EQUAL(32, ignite::countRZero(uint32_t(0)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint32_t(1)));
    BOOST_CHECK_EQUAL(1, ignite::countRZero(uint32_t(2)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint32_t(3)));
    BOOST_CHECK_EQUAL(2, ignite::countRZero(uint32_t(4)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint32_t(5)));
    BOOST_CHECK_EQUAL(1, ignite::countRZero(uint32_t(6)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint32_t(7)));
    BOOST_CHECK_EQUAL(3, ignite::countRZero(uint32_t(8)));
    BOOST_CHECK_EQUAL(2, ignite::countRZero(uint32_t(12)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint32_t(15)));
    BOOST_CHECK_EQUAL(4, ignite::countRZero(uint32_t(16)));
    BOOST_CHECK_EQUAL(7, ignite::countRZero(uint32_t(128)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint32_t(255)));
    BOOST_CHECK_EQUAL(11, ignite::countRZero(uint32_t(0x0800)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint32_t(0x0fff)));
    BOOST_CHECK_EQUAL(15, ignite::countRZero(uint32_t(0x8000)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint32_t(0xffff)));
    BOOST_CHECK_EQUAL(23, ignite::countRZero(uint32_t(0x00800000)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint32_t(0x00ffffff)));
    BOOST_CHECK_EQUAL(31, ignite::countRZero(uint32_t(0x80000000)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint32_t(0xffffffff)));

    BOOST_CHECK_EQUAL(64, ignite::countRZero(uint64_t(0)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint64_t(1)));
    BOOST_CHECK_EQUAL(1, ignite::countRZero(uint64_t(2)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint64_t(3)));
    BOOST_CHECK_EQUAL(2, ignite::countRZero(uint64_t(4)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint64_t(5)));
    BOOST_CHECK_EQUAL(1, ignite::countRZero(uint64_t(6)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint64_t(7)));
    BOOST_CHECK_EQUAL(3, ignite::countRZero(uint64_t(8)));
    BOOST_CHECK_EQUAL(2, ignite::countRZero(uint64_t(12)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint64_t(15)));
    BOOST_CHECK_EQUAL(4, ignite::countRZero(uint64_t(16)));
    BOOST_CHECK_EQUAL(7, ignite::countRZero(uint64_t(128)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint64_t(255)));
    BOOST_CHECK_EQUAL(11, ignite::countRZero(uint64_t(0x0800)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint64_t(0x0fff)));
    BOOST_CHECK_EQUAL(15, ignite::countRZero(uint64_t(0x8000)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint64_t(0xffff)));
    BOOST_CHECK_EQUAL(31, ignite::countRZero(uint64_t(0x80000000)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint64_t(0xffffffff)));
    BOOST_CHECK_EQUAL(47, ignite::countRZero(uint64_t(0x0000800000000000)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint64_t(0x0000ffffffffffff)));
    BOOST_CHECK_EQUAL(63, ignite::countRZero(uint64_t(0x8000000000000000)));
    BOOST_CHECK_EQUAL(0, ignite::countRZero(uint64_t(0xffffffffffffffff)));
}

BOOST_AUTO_TEST_CASE(countLZero)
{
    BOOST_CHECK_EQUAL(8, ignite::countLZero(uint8_t(0)));
    BOOST_CHECK_EQUAL(7, ignite::countLZero(uint8_t(1)));
    BOOST_CHECK_EQUAL(6, ignite::countLZero(uint8_t(2)));
    BOOST_CHECK_EQUAL(6, ignite::countLZero(uint8_t(3)));
    BOOST_CHECK_EQUAL(5, ignite::countLZero(uint8_t(4)));
    BOOST_CHECK_EQUAL(5, ignite::countLZero(uint8_t(5)));
    BOOST_CHECK_EQUAL(5, ignite::countLZero(uint8_t(6)));
    BOOST_CHECK_EQUAL(5, ignite::countLZero(uint8_t(7)));
    BOOST_CHECK_EQUAL(4, ignite::countLZero(uint8_t(8)));
    BOOST_CHECK_EQUAL(4, ignite::countLZero(uint8_t(12)));
    BOOST_CHECK_EQUAL(4, ignite::countLZero(uint8_t(15)));
    BOOST_CHECK_EQUAL(3, ignite::countLZero(uint8_t(16)));
    BOOST_CHECK_EQUAL(0, ignite::countLZero(uint8_t(128)));
    BOOST_CHECK_EQUAL(0, ignite::countLZero(uint8_t(255)));

    BOOST_CHECK_EQUAL(16, ignite::countLZero(uint16_t(0)));
    BOOST_CHECK_EQUAL(15, ignite::countLZero(uint16_t(1)));
    BOOST_CHECK_EQUAL(14, ignite::countLZero(uint16_t(2)));
    BOOST_CHECK_EQUAL(14, ignite::countLZero(uint16_t(3)));
    BOOST_CHECK_EQUAL(13, ignite::countLZero(uint16_t(4)));
    BOOST_CHECK_EQUAL(13, ignite::countLZero(uint16_t(5)));
    BOOST_CHECK_EQUAL(13, ignite::countLZero(uint16_t(6)));
    BOOST_CHECK_EQUAL(13, ignite::countLZero(uint16_t(7)));
    BOOST_CHECK_EQUAL(12, ignite::countLZero(uint16_t(8)));
    BOOST_CHECK_EQUAL(12, ignite::countLZero(uint16_t(12)));
    BOOST_CHECK_EQUAL(12, ignite::countLZero(uint16_t(15)));
    BOOST_CHECK_EQUAL(11, ignite::countLZero(uint16_t(16)));
    BOOST_CHECK_EQUAL(8, ignite::countLZero(uint16_t(128)));
    BOOST_CHECK_EQUAL(8, ignite::countLZero(uint16_t(255)));
    BOOST_CHECK_EQUAL(4, ignite::countLZero(uint16_t(0x0800)));
    BOOST_CHECK_EQUAL(4, ignite::countLZero(uint16_t(0x0fff)));
    BOOST_CHECK_EQUAL(0, ignite::countLZero(uint16_t(0x8000)));
    BOOST_CHECK_EQUAL(0, ignite::countLZero(uint16_t(0xffff)));

    BOOST_CHECK_EQUAL(32, ignite::countLZero(uint32_t(0)));
    BOOST_CHECK_EQUAL(31, ignite::countLZero(uint32_t(1)));
    BOOST_CHECK_EQUAL(30, ignite::countLZero(uint32_t(2)));
    BOOST_CHECK_EQUAL(30, ignite::countLZero(uint32_t(3)));
    BOOST_CHECK_EQUAL(29, ignite::countLZero(uint32_t(4)));
    BOOST_CHECK_EQUAL(29, ignite::countLZero(uint32_t(5)));
    BOOST_CHECK_EQUAL(29, ignite::countLZero(uint32_t(6)));
    BOOST_CHECK_EQUAL(29, ignite::countLZero(uint32_t(7)));
    BOOST_CHECK_EQUAL(28, ignite::countLZero(uint32_t(8)));
    BOOST_CHECK_EQUAL(28, ignite::countLZero(uint32_t(12)));
    BOOST_CHECK_EQUAL(28, ignite::countLZero(uint32_t(15)));
    BOOST_CHECK_EQUAL(27, ignite::countLZero(uint32_t(16)));
    BOOST_CHECK_EQUAL(24, ignite::countLZero(uint32_t(128)));
    BOOST_CHECK_EQUAL(24, ignite::countLZero(uint32_t(255)));
    BOOST_CHECK_EQUAL(20, ignite::countLZero(uint32_t(0x0800)));
    BOOST_CHECK_EQUAL(20, ignite::countLZero(uint32_t(0x0fff)));
    BOOST_CHECK_EQUAL(16, ignite::countLZero(uint32_t(0x8000)));
    BOOST_CHECK_EQUAL(16, ignite::countLZero(uint32_t(0xffff)));
    BOOST_CHECK_EQUAL(8, ignite::countLZero(uint32_t(0x00800000)));
    BOOST_CHECK_EQUAL(8, ignite::countLZero(uint32_t(0x00ffffff)));
    BOOST_CHECK_EQUAL(0, ignite::countLZero(uint32_t(0x80000000)));
    BOOST_CHECK_EQUAL(0, ignite::countLZero(uint32_t(0xffffffff)));

    BOOST_CHECK_EQUAL(64, ignite::countLZero(uint64_t(0)));
    BOOST_CHECK_EQUAL(63, ignite::countLZero(uint64_t(1)));
    BOOST_CHECK_EQUAL(62, ignite::countLZero(uint64_t(2)));
    BOOST_CHECK_EQUAL(62, ignite::countLZero(uint64_t(3)));
    BOOST_CHECK_EQUAL(61, ignite::countLZero(uint64_t(4)));
    BOOST_CHECK_EQUAL(61, ignite::countLZero(uint64_t(5)));
    BOOST_CHECK_EQUAL(61, ignite::countLZero(uint64_t(6)));
    BOOST_CHECK_EQUAL(61, ignite::countLZero(uint64_t(7)));
    BOOST_CHECK_EQUAL(60, ignite::countLZero(uint64_t(8)));
    BOOST_CHECK_EQUAL(60, ignite::countLZero(uint64_t(12)));
    BOOST_CHECK_EQUAL(60, ignite::countLZero(uint64_t(15)));
    BOOST_CHECK_EQUAL(59, ignite::countLZero(uint64_t(16)));
    BOOST_CHECK_EQUAL(56, ignite::countLZero(uint64_t(128)));
    BOOST_CHECK_EQUAL(56, ignite::countLZero(uint64_t(255)));
    BOOST_CHECK_EQUAL(52, ignite::countLZero(uint64_t(0x0800)));
    BOOST_CHECK_EQUAL(52, ignite::countLZero(uint64_t(0x0fff)));
    BOOST_CHECK_EQUAL(48, ignite::countLZero(uint64_t(0x8000)));
    BOOST_CHECK_EQUAL(48, ignite::countLZero(uint64_t(0xffff)));
    BOOST_CHECK_EQUAL(40, ignite::countLZero(uint64_t(0x00800000)));
    BOOST_CHECK_EQUAL(40, ignite::countLZero(uint64_t(0x00ffffff)));
    BOOST_CHECK_EQUAL(32, ignite::countLZero(uint64_t(0x80000000)));
    BOOST_CHECK_EQUAL(32, ignite::countLZero(uint64_t(0xffffffff)));
    BOOST_CHECK_EQUAL(16, ignite::countLZero(uint64_t(0x0000800000000000)));
    BOOST_CHECK_EQUAL(16, ignite::countLZero(uint64_t(0x0000ffffffffffff)));
    BOOST_CHECK_EQUAL(0, ignite::countLZero(uint64_t(0x8000000000000000)));
    BOOST_CHECK_EQUAL(0, ignite::countLZero(uint64_t(0xffffffffffffffff)));
}

BOOST_AUTO_TEST_CASE(bitWidth) {
    BOOST_CHECK_EQUAL(0, ignite::bitWidth(uint8_t(0)));
    BOOST_CHECK_EQUAL(1, ignite::bitWidth(uint8_t(1)));
    BOOST_CHECK_EQUAL(2, ignite::bitWidth(uint8_t(2)));
    BOOST_CHECK_EQUAL(2, ignite::bitWidth(uint8_t(3)));
    BOOST_CHECK_EQUAL(3, ignite::bitWidth(uint8_t(4)));
    BOOST_CHECK_EQUAL(3, ignite::bitWidth(uint8_t(5)));
    BOOST_CHECK_EQUAL(3, ignite::bitWidth(uint8_t(6)));
    BOOST_CHECK_EQUAL(3, ignite::bitWidth(uint8_t(7)));
    BOOST_CHECK_EQUAL(4, ignite::bitWidth(uint8_t(8)));
    BOOST_CHECK_EQUAL(4, ignite::bitWidth(uint8_t(12)));
    BOOST_CHECK_EQUAL(4, ignite::bitWidth(uint8_t(15)));
    BOOST_CHECK_EQUAL(5, ignite::bitWidth(uint8_t(16)));
    BOOST_CHECK_EQUAL(8, ignite::bitWidth(uint8_t(128)));
    BOOST_CHECK_EQUAL(8, ignite::bitWidth(uint8_t(255)));

    BOOST_CHECK_EQUAL(0, ignite::bitWidth(uint16_t(0)));
    BOOST_CHECK_EQUAL(1, ignite::bitWidth(uint16_t(1)));
    BOOST_CHECK_EQUAL(2, ignite::bitWidth(uint16_t(2)));
    BOOST_CHECK_EQUAL(2, ignite::bitWidth(uint16_t(3)));
    BOOST_CHECK_EQUAL(3, ignite::bitWidth(uint16_t(4)));
    BOOST_CHECK_EQUAL(3, ignite::bitWidth(uint16_t(5)));
    BOOST_CHECK_EQUAL(3, ignite::bitWidth(uint16_t(6)));
    BOOST_CHECK_EQUAL(3, ignite::bitWidth(uint16_t(7)));
    BOOST_CHECK_EQUAL(4, ignite::bitWidth(uint16_t(8)));
    BOOST_CHECK_EQUAL(4, ignite::bitWidth(uint16_t(12)));
    BOOST_CHECK_EQUAL(4, ignite::bitWidth(uint16_t(15)));
    BOOST_CHECK_EQUAL(5, ignite::bitWidth(uint16_t(16)));
    BOOST_CHECK_EQUAL(8, ignite::bitWidth(uint16_t(128)));
    BOOST_CHECK_EQUAL(8, ignite::bitWidth(uint16_t(255)));
    BOOST_CHECK_EQUAL(12, ignite::bitWidth(uint16_t(0x0800)));
    BOOST_CHECK_EQUAL(12, ignite::bitWidth(uint16_t(0x0fff)));
    BOOST_CHECK_EQUAL(16, ignite::bitWidth(uint16_t(0x8000)));
    BOOST_CHECK_EQUAL(16, ignite::bitWidth(uint16_t(0xffff)));

    BOOST_CHECK_EQUAL(0, ignite::bitWidth(uint32_t(0)));
    BOOST_CHECK_EQUAL(1, ignite::bitWidth(uint32_t(1)));
    BOOST_CHECK_EQUAL(2, ignite::bitWidth(uint32_t(2)));
    BOOST_CHECK_EQUAL(2, ignite::bitWidth(uint32_t(3)));
    BOOST_CHECK_EQUAL(3, ignite::bitWidth(uint32_t(4)));
    BOOST_CHECK_EQUAL(3, ignite::bitWidth(uint32_t(5)));
    BOOST_CHECK_EQUAL(3, ignite::bitWidth(uint32_t(6)));
    BOOST_CHECK_EQUAL(3, ignite::bitWidth(uint32_t(7)));
    BOOST_CHECK_EQUAL(4, ignite::bitWidth(uint32_t(8)));
    BOOST_CHECK_EQUAL(4, ignite::bitWidth(uint32_t(12)));
    BOOST_CHECK_EQUAL(4, ignite::bitWidth(uint32_t(15)));
    BOOST_CHECK_EQUAL(5, ignite::bitWidth(uint32_t(16)));
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
    BOOST_CHECK_EQUAL(3, ignite::bitWidth(uint64_t(5)));
    BOOST_CHECK_EQUAL(3, ignite::bitWidth(uint64_t(6)));
    BOOST_CHECK_EQUAL(3, ignite::bitWidth(uint64_t(7)));
    BOOST_CHECK_EQUAL(4, ignite::bitWidth(uint64_t(8)));
    BOOST_CHECK_EQUAL(4, ignite::bitWidth(uint64_t(12)));
    BOOST_CHECK_EQUAL(4, ignite::bitWidth(uint64_t(15)));
    BOOST_CHECK_EQUAL(5, ignite::bitWidth(uint64_t(16)));
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
    BOOST_CHECK_EQUAL(0x8000, ignite::bitFloor(uint16_t(0xffff)));

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
    BOOST_CHECK_EQUAL(0x8000, ignite::bitFloor(uint32_t(0xffff)));
    BOOST_CHECK_EQUAL(0x80000000, ignite::bitFloor(uint32_t(0x80000000)));
    BOOST_CHECK_EQUAL(0x80000000, ignite::bitFloor(uint32_t(0xffffffff)));

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
    BOOST_CHECK_EQUAL(0x80000000, ignite::bitFloor(uint64_t(0xffffffff)));
    BOOST_CHECK_EQUAL(0x8000000000000000, ignite::bitFloor(uint64_t(0x8000000000000000)));
    BOOST_CHECK_EQUAL(0x8000000000000000, ignite::bitFloor(uint64_t(0xffffffffffffffff)));
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
