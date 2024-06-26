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

#include "big_integer.h"

#include "detail/bytes.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <memory>
#include <sstream>

namespace ignite {

void big_integer::from_big_endian(const std::byte *data, std::size_t size) {
    while (size > 0 && data[0] == std::byte{0}) {
        size--;
        data++;
    }

    if (size == 0) {
        assign_uint64(0);
        return;
    }

    m_mpi.grow((size + 3) / 4);

    for (std::size_t i = 0; size >= 4; i++) {
        size -= 4;
        m_mpi.magnitude()[i] = detail::bytes::load<detail::endian::BIG, std::uint32_t>(data + size);
    }

    if (size > 0) {
        std::uint32_t last = 0;
        switch (size) {
            case 3:
                last |= std::to_integer<std::uint32_t>(data[size - 3]) << 16;
                [[fallthrough]];
            case 2:
                last |= std::to_integer<std::uint32_t>(data[size - 2]) << 8;
                [[fallthrough]];
            case 1:
                last |= std::to_integer<std::uint32_t>(data[size - 1]);
                break;
            default:
                assert(false);
        }
        m_mpi.magnitude()[m_mpi.length() - 1] = last;
    }

    m_mpi.make_positive();
}

void big_integer::from_negative_big_endian(const std::byte *data, std::size_t size) {
    assert(size > 0);
    assert(data[0] != std::byte{0});

    while (size > 0 && data[0] == std::byte{0xff}) {
        size--;
        data++;
    }

    // Take one step back if only zeroes remain.
    if (std::all_of(data, data + size, [](std::byte x) { return x == std::byte{0}; })) {
        size++;
        data--;
    }

    m_mpi.grow((size + 3) / 4);

    for (std::size_t i = 0; size >= 4; i++) {
        size -= 4;
        m_mpi.magnitude()[i] = ~detail::bytes::load<detail::endian::BIG, std::uint32_t>(data + size);
    }

    if (size > 0) {
        std::uint32_t last = 0;
        switch (size) {
            case 3:
                last |= std::to_integer<std::uint32_t>(~data[size - 3]) << 16;
                [[fallthrough]];
            case 2:
                last |= std::to_integer<std::uint32_t>(~data[size - 2]) << 8;
                [[fallthrough]];
            case 1:
                last |= std::to_integer<std::uint32_t>(~data[size - 1]);
                break;
            default:
                assert(false);
        }
        m_mpi.magnitude()[m_mpi.length() - 1] = last;
    }

    for (std::size_t i = 0; i < m_mpi.length(); i++) {
        ++m_mpi.magnitude()[i];
        if (m_mpi.magnitude()[i] != 0) {
            break;
        }
    }

    m_mpi.make_negative();
}

big_integer::big_integer(const int8_t *val, int32_t len, int8_t sign, bool big_endian) {
    assert(val != nullptr);
    assert(len >= 0);
    assert(sign == detail::mpi_sign::POSITIVE || sign == 0 || sign == detail::mpi_sign::NEGATIVE);

    // Normalize sign.
    sign = sign >= 0 ? detail::mpi_sign::POSITIVE : detail::mpi_sign::NEGATIVE;

    std::size_t size = len;
    const auto *data = (const std::byte *) (val);

    if (big_endian) {
        from_big_endian(reinterpret_cast<const std::byte *>(val), len);
    } else {
        while (size > 0 && data[size - 1] != std::byte{0}) {
            --size;
        }

        if (size == 0) {
            assign_int64(0);
            return;
        }

        m_mpi.grow((size + 3) / 4);

        for (std::size_t i = 0; size >= 4; i++) {
            m_mpi.magnitude()[i] = detail::bytes::load<detail::endian::LITTLE, std::uint32_t>(data);
            size -= 4;
            data += 4;
        }

        if (size > 0) {
            std::uint32_t last = 0;
            switch (size) {
                case 3:
                    last |= std::to_integer<std::uint32_t>(data[2]) << 16;
                    [[fallthrough]];
                case 2:
                    last |= std::to_integer<std::uint32_t>(data[1]) << 8;
                    [[fallthrough]];
                case 1:
                    last |= std::to_integer<std::uint32_t>(data[0]);
                    break;
                default:
                    assert(false);
            }
            m_mpi.magnitude()[m_mpi.length() - 1] = last;
        }
    }

    m_mpi.set_sign(detail::mpi_sign(sign));
}

big_integer::big_integer(const std::byte *data, std::size_t size) {
    if (size == 0) {
        return;
    }

    if (std::to_integer<std::int8_t>(data[0]) >= 0) {
        from_big_endian(data, size);
    } else {
        from_negative_big_endian(data, size);
    }
}

void big_integer::assign_int64(int64_t val) {
    if (val < 0) {
        assign_uint64(val > INT64_MIN ? static_cast<uint64_t>(-val) : static_cast<uint64_t>(val));
        m_mpi.make_negative();
    } else {
        assign_uint64(static_cast<uint64_t>(val));
    }
}

void big_integer::assign_uint64(uint64_t x) {
    m_mpi.reinit();

    word_t val[2];

    val[0] = static_cast<word_t>(x);
    val[1] = static_cast<word_t>(x >> 32);

    if (val[1] == 0) {
        m_mpi.grow(1);
        m_mpi.magnitude()[0] = val[0];
    } else {
        m_mpi.grow(2);
        m_mpi.magnitude()[0] = val[0];
        m_mpi.magnitude()[1] = val[1];
    }
}

void big_integer::assign_string(const std::string &val) {
    m_mpi.assign_from_string(val.c_str());
}

void big_integer::assign_string(const char *val, std::size_t len) {
    assign_string({val, len});
}

std::uint32_t big_integer::magnitude_bit_length() const noexcept {
    return m_mpi.magnitude_bit_length();
}

std::uint32_t big_integer::bit_length() const noexcept {
    auto res = magnitude_bit_length();
    if (res == 0)
        return 1;

    auto view = get_magnitude();

    if (is_negative()) {
        // Check if the magnitude is a power of 2.
        auto last = view.back();
        if ((last & (last - 1)) == 0 && std::all_of(view.rbegin() + 1, view.rend(), [](auto x) { return x == 0; })) {
            res--;
        }
    }
    return res;
}

std::size_t big_integer::byte_size() const noexcept {
    // This includes a sign bit.
    return bit_length() / 8u + 1u;
}

void big_integer::store_bytes(std::byte *data) const {
    if (m_mpi.length() == 0) {
        data[0] = std::byte{0};
        return;
    }

    m_mpi.shrink();

    std::size_t size = byte_size();

    if (is_positive()) {
        for (std::size_t i = 0; size >= 4; i++) {
            size -= 4;
            detail::bytes::store<detail::endian::BIG, std::uint32_t>(data + size, m_mpi.magnitude()[i]);
        }

        if (size > 0) {
            std::uint32_t last = m_mpi.length() == 0 ? 0u : m_mpi.magnitude()[m_mpi.length() - 1];
            if (std::int8_t(last) < 0 && size == 1) {
                last = 0;
            }

            switch (size) {
                case 3:
                    data[size - 3] = std::byte(last >> 16);
                    [[fallthrough]];
                case 2:
                    data[size - 2] = std::byte(last >> 8);
                    [[fallthrough]];
                case 1:
                    data[size - 1] = std::byte(last);
                    break;
                default:
                    assert(false);
            }
        }
    } else {
        std::uint32_t carry = 1;

        for (std::size_t i = 0; size >= 4; i++) {
            std::uint32_t value = ~m_mpi.magnitude()[i] + carry;
            if (value != 0) {
                carry = 0;
            }

            size -= 4;
            detail::bytes::store<detail::endian::BIG, std::uint32_t>(data + size, value);
        }

        if (size > 0) {
            std::uint32_t last = ~m_mpi.magnitude()[m_mpi.length() - 1] + carry;
            if (std::int8_t(last) > 0 && size == 1) {
                last = -1;
            }

            switch (size) {
                case 3:
                    data[size - 3] = std::byte(last >> 16);
                    [[fallthrough]];
                case 2:
                    data[size - 2] = std::byte(last >> 8);
                    [[fallthrough]];
                case 1:
                    data[size - 1] = std::byte(last);
                    break;
                default:
                    assert(false);
                    break;
            }
        }
    }
}

int32_t big_integer::get_precision() const noexcept {
    // See http://graphics.stanford.edu/~seander/bithacks.html
    // for the details on the algorithm.

    if (m_mpi.is_zero())
        return 1;

    auto r = int32_t(uint32_t(((static_cast<uint64_t>(magnitude_bit_length()) + 1) * 646456993) >> 31));

    big_integer prec;
    big_integer::get_power_of_ten(r, prec);

    auto cmp = compare(prec, true);

    return cmp < 0 ? r : r + 1;
}

void big_integer::pow(int32_t exp) {
    mpi_t result(1);

    while (exp > 0) {
        if (exp & 1) {
            result.multiply(m_mpi);
        }
        m_mpi.multiply(m_mpi);
        exp >>= 1;
    }

    m_mpi = result;
}

void big_integer::multiply(const big_integer &other, big_integer &res) const {
    res = m_mpi * other.m_mpi;
}

void big_integer::divide(const big_integer &divisor, big_integer &res) const {
    divide(divisor, res, nullptr);
}

void big_integer::divide(const big_integer &divisor, big_integer &res, big_integer &rem) const {
    divide(divisor, res, &rem);
}

void big_integer::add(const big_integer &other, big_integer &res) const {
    res = m_mpi + other.m_mpi;
}

void big_integer::subtract(const big_integer &other, big_integer &res) const {
    res = m_mpi - other.m_mpi;
}

void big_integer::add(uint64_t x) {
    if (x == 0)
        return;

    if (is_zero()) {
        assign_uint64(x);
        return;
    }

    std::uint32_t val[2];

    val[0] = static_cast<word_t>(x);
    val[1] = static_cast<word_t>(x >> 32);

    if (val[1] == 0) {
        m_mpi.grow(1);
        m_mpi.magnitude()[0] = val[0];
    } else {
        m_mpi.grow(2);
        m_mpi.magnitude()[0] = val[0];
        m_mpi.magnitude()[1] = val[1];
    }
}

int big_integer::compare(const big_integer &other, bool ignore_sign) const {
    return m_mpi.compare(other.m_mpi, ignore_sign);
}

int64_t big_integer::to_int64() const {
    auto mag = m_mpi.magnitude();
    std::uint64_t high = mag.size() > 1 ? mag[1] : 0;
    std::uint64_t low = mag.size() > 0 ? mag[0] : 0;
    return m_mpi.sign() * int64_t(high << 32 | low);
}

void big_integer::get_power_of_ten(std::int32_t pow, big_integer &res) {
    assert(pow >= 0);

    res.assign_uint64(10);
    res.pow(pow);
}

void big_integer::divide(const big_integer &divisor, big_integer &res, big_integer *rem) const {
    if (rem) {
        res = m_mpi.div_and_mod(divisor.m_mpi, rem->m_mpi);
    } else {
        res = m_mpi / divisor.m_mpi;
    }
}

std::string big_integer::to_string() const {
    return m_mpi.to_string();
}

std::ostream &operator<<(std::ostream &os, const big_integer &val) {
    return os << val.to_string();
}

std::istream &operator>>(std::istream &is, big_integer &val) {
    std::istream::sentry sentry(is);

    // Return zero if input failed.
    val.assign_int64(0);

    // Current char.
    int c = is.peek();

    if (!is) {
        return is;
    }

    std::string str;
    while (is && (isdigit(c) || c == '-' || c == '+')) {
        str.push_back(c);
        is.ignore();
        c = is.peek();
    }

    val.m_mpi.assign_from_string(str.c_str());

    return is;
}

void swap(big_integer &lhs, big_integer &rhs) {
    using std::swap;

    std::swap(lhs.m_mpi, rhs.m_mpi);
}

} // namespace ignite
