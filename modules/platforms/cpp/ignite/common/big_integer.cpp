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

#include <array>
#include <cassert>
#include <memory>
#include <sstream>

namespace ignite {

big_integer::big_integer(const int8_t *val, int32_t len, int8_t sign, bool big_endian) {
    assert(val != nullptr);
    assert(len >= 0);
    assert(sign == detail::mpi_sign::POSITIVE || sign == 0 || sign == detail::mpi_sign::NEGATIVE);

    m_mpi.read(reinterpret_cast<const std::uint8_t *>(val), len, big_endian);

    m_mpi.set_sign(sign >= 0 ? detail::mpi_sign::POSITIVE : detail::mpi_sign::NEGATIVE);

    m_mpi.shrink();
}

big_integer::big_integer(const std::byte *data, std::size_t size) {
    auto ptr = reinterpret_cast<const std::uint8_t *>(data);

    m_mpi.read(ptr, size);

    if (ptr[0] & 0x80) {
        mpi_t::word carry = 1;
        for (auto &word : m_mpi.magnitude()) {
            // Invert word and add carry if number is negative because it should be in two's complement form.
            word = ~word + carry;
            if (word != 0) {
                carry = 0;
            }
        }

        std::size_t extra_bytes = size % 4;
        if(extra_bytes) {
            mpi_t::word mask = 0xFFFFFFFF >> 8 * (4 - extra_bytes);
            m_mpi.magnitude().back() &= mask;
        }
        m_mpi.make_negative();
    }

    m_mpi.shrink();
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
        if ((last & (last - 1)) == 0) {
            bool all_zero = true;
            for (auto i = std::int64_t(view.size() - 2); i > 0; i--) {
                if (view[i] != 0) {
                    all_zero = false;
                    break;
                }
            }
            if (all_zero) {
                res--;
            }
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

    auto size = byte_size();
    m_mpi.write(reinterpret_cast<std::uint8_t *>(data), size);

    if (is_negative()) {
        mpi_t::word carry = 1;
        for (std::size_t i = size; i > 0; i--) {
            data[i - 1] = std::byte(std::uint8_t(~data[i - 1]) + carry);
            if (data[i - 1] != std::byte(0)) {
                carry = 0;
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
