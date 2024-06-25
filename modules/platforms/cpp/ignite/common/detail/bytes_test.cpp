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

#include "bytes.h"

#include <gtest/gtest.h>

#include <array>

using namespace ignite::detail;

TEST(bytes, swapXX) {
    EXPECT_EQ(0x0201, bytes::swap16(0x0102));
    EXPECT_EQ(0x04030201, bytes::swap32(0x01020304));
    EXPECT_EQ(0x0807060504030201, bytes::swap64(0x0102030405060708));
}

TEST(bytes, everse) {
    {
        std::int8_t x = 0x01;
        EXPECT_EQ(0x01, bytes::reverse(x));
    }
    {
        std::uint8_t x = 0x01;
        EXPECT_EQ(0x01, bytes::reverse(x));
    }
    {
        std::int16_t x = 0x0102;
        EXPECT_EQ(0x0201, bytes::reverse(x));
    }
    {
        std::uint16_t x = 0x0102;
        EXPECT_EQ(0x0201, bytes::reverse(x));
    }
    {
        std::int32_t x = 0x01020304;
        EXPECT_EQ(0x04030201, bytes::reverse(x));
    }
    {
        std::uint32_t x = 0x01020304;
        EXPECT_EQ(0x04030201, bytes::reverse(x));
    }
    {
        std::int64_t x = 0x0102030405060708;
        EXPECT_EQ(0x0807060504030201, bytes::reverse(x));
    }
    {
        std::uint64_t x = 0x0102030405060708;
        EXPECT_EQ(0x0807060504030201, bytes::reverse(x));
    }
}

TEST(bytes, adjustOrder) {
    using namespace bytes;

    {
        std::int8_t x = 0x01;
        EXPECT_EQ(x, ltob(x));
        EXPECT_EQ(x, btol(x));
        EXPECT_EQ(x, ltoh(x));
        EXPECT_EQ(x, htol(x));
        EXPECT_EQ(x, btoh(x));
        EXPECT_EQ(x, htob(x));
    }
    {
        std::int16_t x = 0x0102;
        std::int16_t y = 0x0201;
        EXPECT_EQ(y, ltob(x));
        EXPECT_EQ(y, btol(x));
        if (is_little_endian_platform()) {
            EXPECT_EQ(x, ltoh(x));
            EXPECT_EQ(x, htol(x));
            EXPECT_EQ(y, btoh(x));
            EXPECT_EQ(y, htob(x));
        } else if (is_big_endian_platform()) {
            EXPECT_EQ(y, ltoh(x));
            EXPECT_EQ(y, htol(x));
            EXPECT_EQ(x, btoh(x));
            EXPECT_EQ(x, htob(x));
        }
    }
    {
        std::int32_t x = 0x01020304;
        std::int32_t y = 0x04030201;
        EXPECT_EQ(y, ltob(x));
        EXPECT_EQ(y, btol(x));
        if (is_little_endian_platform()) {
            EXPECT_EQ(x, ltoh(x));
            EXPECT_EQ(x, htol(x));
            EXPECT_EQ(y, btoh(x));
            EXPECT_EQ(y, htob(x));
        } else if (is_big_endian_platform()) {
            EXPECT_EQ(y, ltoh(x));
            EXPECT_EQ(y, htol(x));
            EXPECT_EQ(x, btoh(x));
            EXPECT_EQ(x, htob(x));
        }
    }
    {
        std::int64_t x = 0x0102030405060708;
        std::int64_t y = 0x0807060504030201;
        EXPECT_EQ(y, ltob(x));
        EXPECT_EQ(y, btol(x));
        if (is_little_endian_platform()) {
            EXPECT_EQ(x, ltoh(x));
            EXPECT_EQ(x, htol(x));
            EXPECT_EQ(y, btoh(x));
            EXPECT_EQ(y, htob(x));
        } else if (is_big_endian_platform()) {
            EXPECT_EQ(y, ltoh(x));
            EXPECT_EQ(y, htol(x));
            EXPECT_EQ(x, btoh(x));
            EXPECT_EQ(x, htob(x));
        }
    }
}

TEST(bytes, awAccess) {
    using namespace bytes;

    {
        std::int8_t x = 0x01;
        std::array<std::byte, sizeof(x)> buf{std::byte{0}};

        store_raw(buf.data(), x);
        EXPECT_EQ(0x01, std::to_integer<char>(buf[0]));

        auto y = load_raw<decltype(x)>(buf.data());
        EXPECT_EQ(x, y);
    }
    {
        std::int16_t x = 0x0102;
        std::array<std::byte, sizeof(x)> buf{std::byte{0}};

        store_raw(buf.data(), x);
        if (is_little_endian_platform()) {
            EXPECT_EQ(0x02, std::to_integer<char>(buf[0]));
            EXPECT_EQ(0x01, std::to_integer<char>(buf[1]));
        } else if (is_big_endian_platform()) {
            EXPECT_EQ(0x01, std::to_integer<char>(buf[0]));
            EXPECT_EQ(0x02, std::to_integer<char>(buf[1]));
        }

        auto y = load_raw<decltype(x)>(buf.data());
        EXPECT_EQ(x, y);
    }
    {
        std::int32_t x = 0x01020304;
        std::array<std::byte, sizeof(x)> buf{std::byte{0}};

        store_raw(buf.data(), x);
        if (is_little_endian_platform()) {
            EXPECT_EQ(0x04, std::to_integer<char>(buf[0]));
            EXPECT_EQ(0x03, std::to_integer<char>(buf[1]));
            EXPECT_EQ(0x02, std::to_integer<char>(buf[2]));
            EXPECT_EQ(0x01, std::to_integer<char>(buf[3]));
        } else if (is_big_endian_platform()) {
            EXPECT_EQ(0x01, std::to_integer<char>(buf[0]));
            EXPECT_EQ(0x02, std::to_integer<char>(buf[1]));
            EXPECT_EQ(0x03, std::to_integer<char>(buf[2]));
            EXPECT_EQ(0x04, std::to_integer<char>(buf[3]));
        }

        auto y = load_raw<decltype(x)>(buf.data());
        EXPECT_EQ(x, y);
    }
    {
        std::int64_t x = 0x0102030405060708;
        std::array<std::byte, sizeof(x)> buf{std::byte{0}};

        store_raw(buf.data(), x);
        if (is_little_endian_platform()) {
            EXPECT_EQ(0x08, std::to_integer<char>(buf[0]));
            EXPECT_EQ(0x07, std::to_integer<char>(buf[1]));
            EXPECT_EQ(0x06, std::to_integer<char>(buf[2]));
            EXPECT_EQ(0x05, std::to_integer<char>(buf[3]));
            EXPECT_EQ(0x04, std::to_integer<char>(buf[4]));
            EXPECT_EQ(0x03, std::to_integer<char>(buf[5]));
            EXPECT_EQ(0x02, std::to_integer<char>(buf[6]));
            EXPECT_EQ(0x01, std::to_integer<char>(buf[7]));
        } else if (is_big_endian_platform()) {
            EXPECT_EQ(0x01, std::to_integer<char>(buf[0]));
            EXPECT_EQ(0x02, std::to_integer<char>(buf[1]));
            EXPECT_EQ(0x03, std::to_integer<char>(buf[2]));
            EXPECT_EQ(0x04, std::to_integer<char>(buf[3]));
            EXPECT_EQ(0x05, std::to_integer<char>(buf[4]));
            EXPECT_EQ(0x06, std::to_integer<char>(buf[5]));
            EXPECT_EQ(0x07, std::to_integer<char>(buf[6]));
            EXPECT_EQ(0x08, std::to_integer<char>(buf[7]));
        }

        auto y = load_raw<decltype(x)>(buf.data());
        EXPECT_EQ(x, y);
    }
}

TEST(bytes, genericAccess) {
    using namespace bytes;

    {
        std::int8_t x = 0x01;
        std::array<std::byte, sizeof(x)> buf{std::byte{0}};
        constexpr auto E = endian::LITTLE;

        store<E>(buf.data(), x);
        EXPECT_EQ(0x01, std::to_integer<char>(buf[0]));

        auto y = load<E, decltype(x)>(buf.data());
        EXPECT_EQ(x, y);
    }
    {
        std::int8_t x = 0x01;
        std::array<std::byte, sizeof(x)> buf{std::byte{0}};
        constexpr auto E = endian::BIG;

        store<E>(buf.data(), x);
        EXPECT_EQ(0x01, std::to_integer<char>(buf[0]));

        auto y = load<E, decltype(x)>(buf.data());
        EXPECT_EQ(x, y);
    }
    {
        std::int16_t x = 0x0102;
        std::array<std::byte, sizeof(x)> buf{std::byte{0}};
        constexpr auto E = endian::LITTLE;

        store<E>(buf.data(), x);
        EXPECT_EQ(0x02, std::to_integer<char>(buf[0]));
        EXPECT_EQ(0x01, std::to_integer<char>(buf[1]));

        auto y = load<E, decltype(x)>(buf.data());
        EXPECT_EQ(x, y);
    }
    {
        std::int16_t x = 0x0102;
        std::array<std::byte, sizeof(x)> buf{std::byte{0}};
        constexpr auto E = endian::BIG;

        store<E>(buf.data(), x);
        EXPECT_EQ(0x01, std::to_integer<char>(buf[0]));
        EXPECT_EQ(0x02, std::to_integer<char>(buf[1]));

        auto y = load<E, decltype(x)>(buf.data());
        EXPECT_EQ(x, y);
    }
    {
        std::int32_t x = 0x01020304;
        std::array<std::byte, sizeof(x)> buf{std::byte{0}};
        constexpr auto E = endian::LITTLE;

        store<E>(buf.data(), x);
        EXPECT_EQ(0x04, std::to_integer<char>(buf[0]));
        EXPECT_EQ(0x03, std::to_integer<char>(buf[1]));
        EXPECT_EQ(0x02, std::to_integer<char>(buf[2]));
        EXPECT_EQ(0x01, std::to_integer<char>(buf[3]));

        auto y = load<E, decltype(x)>(buf.data());
        EXPECT_EQ(x, y);
    }
    {
        std::int32_t x = 0x01020304;
        std::array<std::byte, sizeof(x)> buf{std::byte{0}};
        constexpr auto E = endian::BIG;

        store<E>(buf.data(), x);
        EXPECT_EQ(0x01, std::to_integer<char>(buf[0]));
        EXPECT_EQ(0x02, std::to_integer<char>(buf[1]));
        EXPECT_EQ(0x03, std::to_integer<char>(buf[2]));
        EXPECT_EQ(0x04, std::to_integer<char>(buf[3]));

        auto y = load<E, decltype(x)>(buf.data());
        EXPECT_EQ(x, y);
    }
    {
        std::int64_t x = 0x0102030405060708;
        std::array<std::byte, sizeof(x)> buf{std::byte{0}};
        constexpr auto E = endian::LITTLE;

        store<E>(buf.data(), x);
        EXPECT_EQ(0x08, std::to_integer<char>(buf[0]));
        EXPECT_EQ(0x07, std::to_integer<char>(buf[1]));
        EXPECT_EQ(0x06, std::to_integer<char>(buf[2]));
        EXPECT_EQ(0x05, std::to_integer<char>(buf[3]));
        EXPECT_EQ(0x04, std::to_integer<char>(buf[4]));
        EXPECT_EQ(0x03, std::to_integer<char>(buf[5]));
        EXPECT_EQ(0x02, std::to_integer<char>(buf[6]));
        EXPECT_EQ(0x01, std::to_integer<char>(buf[7]));

        auto y = load<E, decltype(x)>(buf.data());
        EXPECT_EQ(x, y);
    }
    {
        std::int64_t x = 0x0102030405060708;
        std::array<std::byte, sizeof(x)> buf{std::byte{0}};
        constexpr auto E = endian::BIG;

        store<E>(buf.data(), x);
        EXPECT_EQ(0x01, std::to_integer<char>(buf[0]));
        EXPECT_EQ(0x02, std::to_integer<char>(buf[1]));
        EXPECT_EQ(0x03, std::to_integer<char>(buf[2]));
        EXPECT_EQ(0x04, std::to_integer<char>(buf[3]));
        EXPECT_EQ(0x05, std::to_integer<char>(buf[4]));
        EXPECT_EQ(0x06, std::to_integer<char>(buf[5]));
        EXPECT_EQ(0x07, std::to_integer<char>(buf[6]));
        EXPECT_EQ(0x08, std::to_integer<char>(buf[7]));

        auto y = load<E, decltype(x)>(buf.data());
        EXPECT_EQ(x, y);
    }
}

TEST(bytes, genericAccessFloat) {
    using namespace bytes;

    {
        float x = 0.1234f;
        std::array<std::byte, sizeof(x)> buf1{std::byte{0}};
        std::array<std::byte, sizeof(x)> buf2{std::byte{0}};
        constexpr auto E1 = endian::LITTLE;
        constexpr auto E2 = endian::BIG;

        store<E1>(buf1.data(), x);
        store<E2>(buf2.data(), x);
        for (std::size_t i = 0; i < sizeof(x); i++) {
            EXPECT_EQ(std::to_integer<char>(buf1[i]), std::to_integer<char>(buf2[sizeof(x) - i - 1]));
        }

        auto y1 = load<E1, decltype(x)>(buf1.data());
        auto y2 = load<E2, decltype(x)>(buf2.data());
        EXPECT_EQ(x, y1);
        EXPECT_EQ(x, y2);
    }
    {
        double x = 0.1234;
        std::array<std::byte, sizeof(x)> buf1{std::byte{0}};
        std::array<std::byte, sizeof(x)> buf2{std::byte{0}};
        constexpr auto E1 = endian::LITTLE;
        constexpr auto E2 = endian::BIG;

        store<E1>(buf1.data(), x);
        store<E2>(buf2.data(), x);
        for (std::size_t i = 0; i < sizeof(x); i++) {
            EXPECT_EQ(std::to_integer<char>(buf1[i]), std::to_integer<char>(buf2[sizeof(x) - i - 1]));
        }

        auto y1 = load<E1, decltype(x)>(buf1.data());
        auto y2 = load<E2, decltype(x)>(buf2.data());
        EXPECT_EQ(x, y1);
        EXPECT_EQ(x, y2);
    }
}
