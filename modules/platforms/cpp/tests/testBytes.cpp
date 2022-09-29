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

#include "common/Bytes.h"

#include <array>

BOOST_AUTO_TEST_CASE(swapXX) {
    BOOST_CHECK_EQUAL(0x0201, ignite::bytes::swap16(0x0102));
    BOOST_CHECK_EQUAL(0x04030201, ignite::bytes::swap32(0x01020304));
    BOOST_CHECK_EQUAL(0x0807060504030201, ignite::bytes::swap64(0x0102030405060708));
}

BOOST_AUTO_TEST_CASE(reverse) {
    {
        std::int8_t x = 0x01;
        BOOST_CHECK_EQUAL(0x01, ignite::bytes::reverse(x));
    }
    {
        std::uint8_t x = 0x01;
        BOOST_CHECK_EQUAL(0x01, ignite::bytes::reverse(x));
    }
    {
        std::int16_t x = 0x0102;
        BOOST_CHECK_EQUAL(0x0201, ignite::bytes::reverse(x));
    }
    {
        std::uint16_t x = 0x0102;
        BOOST_CHECK_EQUAL(0x0201, ignite::bytes::reverse(x));
    }
    {
        std::int32_t x = 0x01020304;
        BOOST_CHECK_EQUAL(0x04030201, ignite::bytes::reverse(x));
    }
    {
        std::uint32_t x = 0x01020304;
        BOOST_CHECK_EQUAL(0x04030201, ignite::bytes::reverse(x));
    }
    {
        std::int64_t x = 0x0102030405060708;
        BOOST_CHECK_EQUAL(0x0807060504030201, ignite::bytes::reverse(x));
    }
    {
        std::uint64_t x = 0x0102030405060708;
        BOOST_CHECK_EQUAL(0x0807060504030201, ignite::bytes::reverse(x));
    }
}

BOOST_AUTO_TEST_CASE(adjustOrder) {
    using namespace ignite::bytes;

    {
        std::int8_t x = 0x01;
        BOOST_CHECK_EQUAL(x, ltob(x));
        BOOST_CHECK_EQUAL(x, btol(x));
        BOOST_CHECK_EQUAL(x, ltoh(x));
        BOOST_CHECK_EQUAL(x, htol(x));
        BOOST_CHECK_EQUAL(x, btoh(x));
        BOOST_CHECK_EQUAL(x, htob(x));
    }
    {
        std::int16_t x = 0x0102;
        std::int16_t y = 0x0201;
        BOOST_CHECK_EQUAL(y, ltob(x));
        BOOST_CHECK_EQUAL(y, btol(x));
        if (ignite::isLittleEndianPlatform()) {
            BOOST_CHECK_EQUAL(x, ltoh(x));
            BOOST_CHECK_EQUAL(x, htol(x));
            BOOST_CHECK_EQUAL(y, btoh(x));
            BOOST_CHECK_EQUAL(y, htob(x));
        } else if (ignite::isBigEndianPlatform()) {
            BOOST_CHECK_EQUAL(y, ltoh(x));
            BOOST_CHECK_EQUAL(y, htol(x));
            BOOST_CHECK_EQUAL(x, btoh(x));
            BOOST_CHECK_EQUAL(x, htob(x));
        }
    }
    {
        std::int32_t x = 0x01020304;
        std::int32_t y = 0x04030201;
        BOOST_CHECK_EQUAL(y, ltob(x));
        BOOST_CHECK_EQUAL(y, btol(x));
        if (ignite::isLittleEndianPlatform()) {
            BOOST_CHECK_EQUAL(x, ltoh(x));
            BOOST_CHECK_EQUAL(x, htol(x));
            BOOST_CHECK_EQUAL(y, btoh(x));
            BOOST_CHECK_EQUAL(y, htob(x));
        } else if (ignite::isBigEndianPlatform()) {
            BOOST_CHECK_EQUAL(y, ltoh(x));
            BOOST_CHECK_EQUAL(y, htol(x));
            BOOST_CHECK_EQUAL(x, btoh(x));
            BOOST_CHECK_EQUAL(x, htob(x));
        }
    }
    {
        std::int64_t x = 0x0102030405060708;
        std::int64_t y = 0x0807060504030201;
        BOOST_CHECK_EQUAL(y, ltob(x));
        BOOST_CHECK_EQUAL(y, btol(x));
        if (ignite::isLittleEndianPlatform()) {
            BOOST_CHECK_EQUAL(x, ltoh(x));
            BOOST_CHECK_EQUAL(x, htol(x));
            BOOST_CHECK_EQUAL(y, btoh(x));
            BOOST_CHECK_EQUAL(y, htob(x));
        } else if (ignite::isBigEndianPlatform()) {
            BOOST_CHECK_EQUAL(y, ltoh(x));
            BOOST_CHECK_EQUAL(y, htol(x));
            BOOST_CHECK_EQUAL(x, btoh(x));
            BOOST_CHECK_EQUAL(x, htob(x));
        }
    }
}

BOOST_AUTO_TEST_CASE(rawAccess) {
    using namespace ignite::bytes;

    {
        std::int8_t x = 0x01;
        std::array<std::byte, sizeof(x)> buf{std::byte{0}};

        storeRaw(buf.data(), x);
        BOOST_CHECK_EQUAL(0x01, std::to_integer<char>(buf[0]));

        auto y = loadRaw<decltype(x)>(buf.data());
        BOOST_CHECK_EQUAL(x, y);
    }
    {
        std::int16_t x = 0x0102;
        std::array<std::byte, sizeof(x)> buf{std::byte{0}};

        storeRaw(buf.data(), x);
        if (ignite::isLittleEndianPlatform()) {
            BOOST_CHECK_EQUAL(0x02, std::to_integer<char>(buf[0]));
            BOOST_CHECK_EQUAL(0x01, std::to_integer<char>(buf[1]));
        } else if (ignite::isBigEndianPlatform()) {
            BOOST_CHECK_EQUAL(0x01, std::to_integer<char>(buf[0]));
            BOOST_CHECK_EQUAL(0x02, std::to_integer<char>(buf[1]));
        }

        auto y = loadRaw<decltype(x)>(buf.data());
        BOOST_CHECK_EQUAL(x, y);
    }
    {
        std::int32_t x = 0x01020304;
        std::array<std::byte, sizeof(x)> buf{std::byte{0}};

        storeRaw(buf.data(), x);
        if (ignite::isLittleEndianPlatform()) {
            BOOST_CHECK_EQUAL(0x04, std::to_integer<char>(buf[0]));
            BOOST_CHECK_EQUAL(0x03, std::to_integer<char>(buf[1]));
            BOOST_CHECK_EQUAL(0x02, std::to_integer<char>(buf[2]));
            BOOST_CHECK_EQUAL(0x01, std::to_integer<char>(buf[3]));
        } else if (ignite::isBigEndianPlatform()) {
            BOOST_CHECK_EQUAL(0x01, std::to_integer<char>(buf[0]));
            BOOST_CHECK_EQUAL(0x02, std::to_integer<char>(buf[1]));
            BOOST_CHECK_EQUAL(0x03, std::to_integer<char>(buf[2]));
            BOOST_CHECK_EQUAL(0x04, std::to_integer<char>(buf[3]));
        }

        auto y = loadRaw<decltype(x)>(buf.data());
        BOOST_CHECK_EQUAL(x, y);
    }
    {
        std::int64_t x = 0x0102030405060708;
        std::array<std::byte, sizeof(x)> buf{std::byte{0}};

        storeRaw(buf.data(), x);
        if (ignite::isLittleEndianPlatform()) {
            BOOST_CHECK_EQUAL(0x08, std::to_integer<char>(buf[0]));
            BOOST_CHECK_EQUAL(0x07, std::to_integer<char>(buf[1]));
            BOOST_CHECK_EQUAL(0x06, std::to_integer<char>(buf[2]));
            BOOST_CHECK_EQUAL(0x05, std::to_integer<char>(buf[3]));
            BOOST_CHECK_EQUAL(0x04, std::to_integer<char>(buf[4]));
            BOOST_CHECK_EQUAL(0x03, std::to_integer<char>(buf[5]));
            BOOST_CHECK_EQUAL(0x02, std::to_integer<char>(buf[6]));
            BOOST_CHECK_EQUAL(0x01, std::to_integer<char>(buf[7]));
        } else if (ignite::isBigEndianPlatform()) {
            BOOST_CHECK_EQUAL(0x01, std::to_integer<char>(buf[0]));
            BOOST_CHECK_EQUAL(0x02, std::to_integer<char>(buf[1]));
            BOOST_CHECK_EQUAL(0x03, std::to_integer<char>(buf[2]));
            BOOST_CHECK_EQUAL(0x04, std::to_integer<char>(buf[3]));
            BOOST_CHECK_EQUAL(0x05, std::to_integer<char>(buf[4]));
            BOOST_CHECK_EQUAL(0x06, std::to_integer<char>(buf[5]));
            BOOST_CHECK_EQUAL(0x07, std::to_integer<char>(buf[6]));
            BOOST_CHECK_EQUAL(0x08, std::to_integer<char>(buf[7]));
        }

        auto y = loadRaw<decltype(x)>(buf.data());
        BOOST_CHECK_EQUAL(x, y);
    }
}

BOOST_AUTO_TEST_CASE(genericAccess) {
    using namespace ignite::bytes;

    {
        std::int8_t x = 0x01;
        std::array<std::byte, sizeof(x)> buf{std::byte{0}};
        constexpr auto E = ignite::Endian::LITTLE;

        store<E>(buf.data(), x);
        BOOST_CHECK_EQUAL(0x01, std::to_integer<char>(buf[0]));

        auto y = load<E, decltype(x)>(buf.data());
        BOOST_CHECK_EQUAL(x, y);
    }
    {
        std::int8_t x = 0x01;
        std::array<std::byte, sizeof(x)> buf{std::byte{0}};
        constexpr auto E = ignite::Endian::BIG;

        store<E>(buf.data(), x);
        BOOST_CHECK_EQUAL(0x01, std::to_integer<char>(buf[0]));

        auto y = load<E, decltype(x)>(buf.data());
        BOOST_CHECK_EQUAL(x, y);
    }
    {
        std::int16_t x = 0x0102;
        std::array<std::byte, sizeof(x)> buf{std::byte{0}};
        constexpr auto E = ignite::Endian::LITTLE;

        store<E>(buf.data(), x);
        BOOST_CHECK_EQUAL(0x02, std::to_integer<char>(buf[0]));
        BOOST_CHECK_EQUAL(0x01, std::to_integer<char>(buf[1]));

        auto y = load<E, decltype(x)>(buf.data());
        BOOST_CHECK_EQUAL(x, y);
    }
    {
        std::int16_t x = 0x0102;
        std::array<std::byte, sizeof(x)> buf{std::byte{0}};
        constexpr auto E = ignite::Endian::BIG;

        store<E>(buf.data(), x);
        BOOST_CHECK_EQUAL(0x01, std::to_integer<char>(buf[0]));
        BOOST_CHECK_EQUAL(0x02, std::to_integer<char>(buf[1]));

        auto y = load<E, decltype(x)>(buf.data());
        BOOST_CHECK_EQUAL(x, y);
    }
    {
        std::int32_t x = 0x01020304;
        std::array<std::byte, sizeof(x)> buf{std::byte{0}};
        constexpr auto E = ignite::Endian::LITTLE;

        store<E>(buf.data(), x);
        BOOST_CHECK_EQUAL(0x04, std::to_integer<char>(buf[0]));
        BOOST_CHECK_EQUAL(0x03, std::to_integer<char>(buf[1]));
        BOOST_CHECK_EQUAL(0x02, std::to_integer<char>(buf[2]));
        BOOST_CHECK_EQUAL(0x01, std::to_integer<char>(buf[3]));

        auto y = load<E, decltype(x)>(buf.data());
        BOOST_CHECK_EQUAL(x, y);
    }
    {
        std::int32_t x = 0x01020304;
        std::array<std::byte, sizeof(x)> buf{std::byte{0}};
        constexpr auto E = ignite::Endian::BIG;

        store<E>(buf.data(), x);
        BOOST_CHECK_EQUAL(0x01, std::to_integer<char>(buf[0]));
        BOOST_CHECK_EQUAL(0x02, std::to_integer<char>(buf[1]));
        BOOST_CHECK_EQUAL(0x03, std::to_integer<char>(buf[2]));
        BOOST_CHECK_EQUAL(0x04, std::to_integer<char>(buf[3]));

        auto y = load<E, decltype(x)>(buf.data());
        BOOST_CHECK_EQUAL(x, y);
    }
    {
        std::int64_t x = 0x0102030405060708;
        std::array<std::byte, sizeof(x)> buf{std::byte{0}};
        constexpr auto E = ignite::Endian::LITTLE;

        store<E>(buf.data(), x);
        BOOST_CHECK_EQUAL(0x08, std::to_integer<char>(buf[0]));
        BOOST_CHECK_EQUAL(0x07, std::to_integer<char>(buf[1]));
        BOOST_CHECK_EQUAL(0x06, std::to_integer<char>(buf[2]));
        BOOST_CHECK_EQUAL(0x05, std::to_integer<char>(buf[3]));
        BOOST_CHECK_EQUAL(0x04, std::to_integer<char>(buf[4]));
        BOOST_CHECK_EQUAL(0x03, std::to_integer<char>(buf[5]));
        BOOST_CHECK_EQUAL(0x02, std::to_integer<char>(buf[6]));
        BOOST_CHECK_EQUAL(0x01, std::to_integer<char>(buf[7]));

        auto y = load<E, decltype(x)>(buf.data());
        BOOST_CHECK_EQUAL(x, y);
    }
    {
        std::int64_t x = 0x0102030405060708;
        std::array<std::byte, sizeof(x)> buf{std::byte{0}};
        constexpr auto E = ignite::Endian::BIG;

        store<E>(buf.data(), x);
        BOOST_CHECK_EQUAL(0x01, std::to_integer<char>(buf[0]));
        BOOST_CHECK_EQUAL(0x02, std::to_integer<char>(buf[1]));
        BOOST_CHECK_EQUAL(0x03, std::to_integer<char>(buf[2]));
        BOOST_CHECK_EQUAL(0x04, std::to_integer<char>(buf[3]));
        BOOST_CHECK_EQUAL(0x05, std::to_integer<char>(buf[4]));
        BOOST_CHECK_EQUAL(0x06, std::to_integer<char>(buf[5]));
        BOOST_CHECK_EQUAL(0x07, std::to_integer<char>(buf[6]));
        BOOST_CHECK_EQUAL(0x08, std::to_integer<char>(buf[7]));

        auto y = load<E, decltype(x)>(buf.data());
        BOOST_CHECK_EQUAL(x, y);
    }
}

BOOST_AUTO_TEST_CASE(genericAccessFloat) {
    using namespace ignite::bytes;

    {
        float x = 0.1234f;
        std::array<std::byte, sizeof(x)> buf1{std::byte{0}};
        std::array<std::byte, sizeof(x)> buf2{std::byte{0}};
        constexpr auto E1 = ignite::Endian::LITTLE;
        constexpr auto E2 = ignite::Endian::BIG;

        store<E1>(buf1.data(), x);
        store<E2>(buf2.data(), x);
        for (int i = 0; i < sizeof(x); i++) {
            BOOST_CHECK_EQUAL(std::to_integer<char>(buf1[i]), std::to_integer<char>(buf2[sizeof(x) - i - 1]));
        }

        auto y1 = load<E1, decltype(x)>(buf1.data());
        auto y2 = load<E2, decltype(x)>(buf2.data());
        BOOST_CHECK_EQUAL(x, y1);
        BOOST_CHECK_EQUAL(x, y2);
    }
    {
        double x = 0.1234;
        std::array<std::byte, sizeof(x)> buf1{std::byte{0}};
        std::array<std::byte, sizeof(x)> buf2{std::byte{0}};
        constexpr auto E1 = ignite::Endian::LITTLE;
        constexpr auto E2 = ignite::Endian::BIG;

        store<E1>(buf1.data(), x);
        store<E2>(buf2.data(), x);
        for (int i = 0; i < sizeof(x); i++) {
            BOOST_CHECK_EQUAL(std::to_integer<char>(buf1[i]), std::to_integer<char>(buf2[sizeof(x) - i - 1]));
        }

        auto y1 = load<E1, decltype(x)>(buf1.data());
        auto y2 = load<E2, decltype(x)>(buf2.data());
        BOOST_CHECK_EQUAL(x, y1);
        BOOST_CHECK_EQUAL(x, y2);
    }
}
