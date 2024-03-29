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

#include "bits.h"
#include "bytes.h"
#include "ignite_error.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <sstream>

namespace ignite {

namespace {

/**
 * Makes single 64-bit integer number out of two 32-bit numbers.
 *
 * @param higher Higher bits part.
 * @param lower Lower bits part.
 * @return New 64-bit integer.
 */
inline uint64_t make_u64(uint32_t higher, uint32_t lower) {
    return (uint64_t{higher} << 32) | lower;
}

/**
 * Shift magnitude left by the specified number of bits.
 *
 * @param in Input magnitude.
 * @param len Magnitude length.
 * @param out Output magnitude. Should be not shorter than the input
 *     magnitude.
 * @param n Number of bits to shift to.
 */
void shift_left(const uint32_t *in, int32_t len, uint32_t *out, unsigned n) {
    assert(n < 32);

    if (n == 0) {
        std::copy(in, in + len, out);

        return;
    }

    for (int32_t i = len - 1; i > 0; --i)
        out[i] = (in[i] << n) | (in[i - 1] >> (32 - n));

    out[0] = in[0] << n;
}

/**
 * Shift magnitude right by the specified number of bits.
 *
 * @param in Input magnitude.
 * @param len Magnitude length.
 * @param out Output magnitude. Should be not shorter than the input
 *     magnitude.
 * @param n Number of bits to shift to.
 */
void shift_right(const uint32_t *in, int32_t len, uint32_t *out, unsigned n) {
    assert(n < 32);

    if (n == 0) {
        std::copy(in, in + len, out);

        return;
    }

    for (int32_t i = 0; i < len - 1; ++i)
        out[i] = (in[i] >> n) | (in[i + 1] << (32 - n));

    out[len - 1] = in[len - 1] >> n;
}

/**
 * Part of the division algorithm. Computes q - (a * x).
 *
 * @param q Minuend.
 * @param a Multiplier of the subtrahend.
 * @param alen Length of the a.
 * @param x Multiplicand of the subtrahend.
 * @return Carry.
 */
uint32_t multiply_and_subtract(uint32_t *q, const uint32_t *a, int32_t alen, uint32_t x) {
    uint64_t carry = 0;

    for (int32_t i = 0; i < alen; ++i) {
        uint64_t product = a[i] * static_cast<uint64_t>(x);
        auto difference = int64_t(q[i] - carry - (product & 0xFFFFFFFF));

        q[i] = static_cast<uint32_t>(difference);

        // This will add one if difference is negative.
        carry = (product >> 32) - (difference >> 32);
    }

    return static_cast<uint32_t>(carry);
}

/**
 * Add two magnitude arrays and return carry.
 *
 * @param res First addend. Result is placed here. Length of this addend
 *     should be equal or greater than len.
 * @param addend Second addend.
 * @param len Length of the second addend.
 * @return Carry.
 */
uint32_t add(uint32_t *res, const uint32_t *addend, int32_t len) {
    uint64_t carry = 0;

    for (int32_t i = 0; i < len; ++i) {
        uint64_t sum = static_cast<uint64_t>(res[i]) + addend[i] + carry;
        res[i] = static_cast<uint32_t>(sum);
        carry = sum >> 32;
    }

    return static_cast<uint32_t>(carry);
}

/**
 * Add single number to a magnitude array and return carry.
 *
 * @param res First addend. Result is placed here. Length of this addend
 *     should be equal or greater than len.
 * @param len Length of the First addend.
 * @param addend Second addend.
 * @return Carry.
 */
uint32_t add(uint32_t *res, int32_t len, uint32_t addend) {
    uint64_t carry = addend;

    for (int32_t i = 0; (i < len) && carry; ++i) {
        uint64_t sum = static_cast<uint64_t>(res[i]) + carry;
        res[i] = static_cast<uint32_t>(sum);
        carry = sum >> 32;
    }

    return static_cast<uint32_t>(carry);
}

} // namespace

void big_integer::from_big_endian(const std::byte *data, std::size_t size) {
    while (size > 0 && data[0] == std::byte{0}) {
        size--;
        data++;
    }

    if (size == 0) {
        assign_uint64(0);
        return;
    }

    mag.resize((size + 3) / 4);

    for (std::size_t i = 0; size >= 4; i++) {
        size -= 4;
        mag[i] = bytes::load<endian::BIG, std::uint32_t>(data + size);
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
                break;
        }
        mag.back() = last;
    }
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

    mag.resize((size + 3) / 4);

    for (std::size_t i = 0; size >= 4; i++) {
        size -= 4;
        mag[i] = ~bytes::load<endian::BIG, std::uint32_t>(data + size);
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
                break;
        }
        mag.back() = last;
    }

    for (auto &val : mag) {
        ++val;
        if (val != 0) {
            break;
        }
    }
}

big_integer::big_integer(const int8_t *val, int32_t len, int8_t sign, bool bigEndian)
    : sign(sign) {
    assert(val != nullptr);
    assert(len >= 0);
    assert(sign == 1 || sign == 0 || sign == -1);

    std::size_t size = len;
    const auto *data = (const std::byte *) (val);

    if (bigEndian) {
        from_big_endian(data, size);
    } else {
        while (size > 0 && data[size - 1] != std::byte{0}) {
            --size;
        }

        if (size == 0) {
            assign_int64(0);
            return;
        }

        mag.resize((size + 3) / 4);

        for (std::size_t i = 0; size >= 4; i++) {
            mag[i] = bytes::load<endian::LITTLE, std::uint32_t>(data);
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
                    break;
            }
            mag.back() = last;
        }
    }
}

big_integer::big_integer(const std::byte *data, std::size_t size) {
    if (size == 0) {
        return;
    }

    if (std::to_integer<std::int8_t>(data[0]) >= 0) {
        from_big_endian(data, size);
    } else {
        from_negative_big_endian(data, size);
        sign = -1;
    }
}

void big_integer::assign_int64(int64_t val) {
    if (val < 0) {
        assign_uint64(val > INT64_MIN ? static_cast<uint64_t>(-val) : static_cast<uint64_t>(val));
        sign = -1;
    } else {
        assign_uint64(static_cast<uint64_t>(val));
    }
}

void big_integer::assign_uint64(uint64_t val) {
    sign = 1;

    if (val == 0) {
        mag.clear();
        return;
    }

    auto highWord = uint32_t(val >> 32);

    if (highWord == 0)
        mag.resize(1);
    else {
        mag.resize(2);
        mag[1] = highWord;
    }

    mag[0] = static_cast<uint32_t>(val);
}

void big_integer::assign_string(const char *val, std::size_t len) {
    std::stringstream converter;

    converter.write(val, std::streamsize(len));

    converter >> *this;
}

void big_integer::swap(big_integer &other) {
    using std::swap;

    swap(sign, other.sign);
    mag.swap(other.mag);
}

std::uint32_t big_integer::magnitude_bit_length() const noexcept {
    if (mag.empty())
        return 0;

    std::uint32_t res = bit_width(mag.back());
    if (mag.size() > 1) {
        res += std::uint32_t(mag.size() - 1) * 32;
    }

    return res;
}

std::uint32_t big_integer::bit_length() const noexcept {
    auto res = magnitude_bit_length();
    if (res == 0)
        return 1;

    if (is_negative()) {
        // Check if the magnitude is a power of 2.
        auto last = mag.back();
        if ((last & (last - 1)) == 0 && std::all_of(mag.rbegin() + 1, mag.rend(), [](auto x) { return x == 0; })) {
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
    if (mag.empty()) {
        data[0] = std::byte{0};
        return;
    }

    std::size_t size = byte_size();

    if (!is_negative()) {
        for (std::size_t i = 0; size >= 4; i++) {
            size -= 4;
            bytes::store<endian::BIG, std::uint32_t>(data + size, mag[i]);
        }

        if (size > 0) {
            std::uint32_t last = mag.empty() ? 0u : mag.back();
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
                    break;
            }
        }
    } else {
        std::uint32_t carry = 1;

        for (std::size_t i = 0; size >= 4; i++) {
            std::uint32_t value = ~mag[i] + carry;
            if (value != 0) {
                carry = 0;
            }

            size -= 4;
            bytes::store<endian::BIG, std::uint32_t>(data + size, value);
        }

        if (size > 0) {
            std::uint32_t last = ~mag.back() + carry;
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

    if (mag.empty())
        return 1;

    auto r = int32_t(uint32_t(((static_cast<uint64_t>(magnitude_bit_length()) + 1) * 646456993) >> 31));

    big_integer prec;
    big_integer::get_power_of_ten(r, prec);

    return compare(prec, true) < 0 ? r : r + 1;
}

void big_integer::pow(int32_t exp) {
    if (exp < 0) {
        assign_int64(0);
        return;
    }

    uint32_t bitsLen = magnitude_bit_length();

    if (!bitsLen)
        return;

    if (bitsLen == 1) {
        if ((exp % 2 == 0) && sign < 0)
            sign = int8_t(-sign);
        return;
    }

    big_integer multiplicand(*this);
    assign_int64(1);

    int32_t mutExp = exp;
    while (mutExp) {
        if (mutExp & 1) {
            multiply(multiplicand, *this);
        }

        mutExp >>= 1;

        if (mutExp) {
            multiplicand.multiply(multiplicand, multiplicand);
        }
    }
}

void big_integer::multiply(const big_integer &other, big_integer &res) const {
    mag_array resMag(mag.size() + other.mag.size());

    resMag.resize(mag.size() + other.mag.size());

    for (std::size_t i = 0; i < other.mag.size(); ++i) {
        uint32_t carry = 0;

        for (std::size_t j = 0; j < mag.size(); ++j) {
            uint64_t product = static_cast<uint64_t>(mag[j]) * other.mag[i] + +resMag[i + j] + carry;

            resMag[i + j] = static_cast<uint32_t>(product);
            carry = static_cast<uint32_t>(product >> 32);
        }

        resMag[i + mag.size()] = carry;
    }

    res.mag.swap(resMag);
    res.sign = int8_t(sign * other.sign);

    res.normalize();
}

void big_integer::divide(const big_integer &divisor, big_integer &res) const {
    divide(divisor, res, nullptr);
}

void big_integer::divide(const big_integer &divisor, big_integer &res, big_integer &rem) const {
    divide(divisor, res, &rem);
}

void big_integer::add(const uint32_t *addend, int32_t len) {
    if (mag.size() < size_t(len)) {
        mag.reserve(len + 1);
        mag.resize(len);
    } else {
        mag.reserve(mag.size() + 1);
    }

    if (uint32_t carry = ignite::add(mag.data(), addend, len)) {
        carry = ignite::add(mag.data() + len, int32_t(mag.size()) - len, carry);
        if (carry) {
            mag.push_back(carry);
        }
    }
}

void big_integer::add(uint64_t x) {
    if (x == 0)
        return;

    if (is_zero()) {
        assign_uint64(x);
        return;
    }

    uint32_t val[2];

    val[0] = static_cast<uint32_t>(x);
    val[1] = static_cast<uint32_t>(x >> 32);

    add(val, val[1] ? 2 : 1);
}

int big_integer::compare(const big_integer &other, bool ignoreSign) const {
    // What we should return if magnitude is greater.
    int32_t mgt = 1;

    if (!ignoreSign) {
        if (sign != other.sign)
            return sign > other.sign ? 1 : -1;
        mgt = uint8_t(sign);
    }

    if (mag.size() != other.mag.size())
        return mag.size() > other.mag.size() ? mgt : -mgt;

    for (int32_t i = int32_t(mag.size()) - 1; i >= 0; --i) {
        if (mag[i] != other.mag[i]) {
            return mag[i] > other.mag[i] ? mgt : -mgt;
        }
    }

    return 0;
}

int64_t big_integer::to_int64() const {
    return int64_t((uint64_t(get_mag_int(1)) << 32) | get_mag_int(0));
}

void big_integer::get_power_of_ten(int32_t pow, big_integer &res) {
    assert(pow >= 0);

    static constexpr auto n64 = std::numeric_limits<uint64_t>::digits10 + 1;
    if (pow < n64) {
        static const auto power10 = [] {
            std::array<std::uint64_t, n64> a{};
            uint64_t power = 1;
            for (int i = 0; i < n64; i++) {
                a[i] = power;
                power *= 10;
            }
            return a;
        }();
        res.assign_uint64(power10[pow]);
    } else {
        res.assign_int64(10);
        res.pow(pow);
    }
}

uint32_t big_integer::get_mag_int(int32_t n) const {
    assert(n >= 0);

    if (size_t(n) >= mag.size())
        return sign > 0 ? 0 : -1;

    return sign * mag[n];
}

void big_integer::divide(const big_integer &divisor, big_integer &res, big_integer *rem) const {
    // Can't divide by zero.
    if (divisor.mag.empty())
        throw ignite_error(error::code::GENERIC, "Division by zero.");

    int32_t compRes = compare(divisor, true);
    auto resSign = int8_t(sign * divisor.sign);

    // The same magnitude. Result is [-]1 and remainder is zero.
    if (compRes == 0) {
        res.assign_int64(resSign);

        if (rem) {
            rem->assign_int64(0);
        }

        return;
    }

    // Divisor is greater than this. Result is 0 and remainder is this.
    if (compRes == -1) {
        // Order is important here! Copy to rem first to handle the case
        // when &res == this.
        if (rem) {
            *rem = *this;
        }

        res.assign_int64(0);

        return;
    }

    // If divisor is [-]1 result is [-]this and remainder is zero.
    if (divisor.magnitude_bit_length() == 1) {
        // Once again: order is important.
        res = *this;
        res.sign = int8_t(sign * divisor.sign);

        if (rem) {
            rem->assign_int64(0);
        }

        return;
    }

    // Trivial case.
    if (mag.size() <= 2) {
        uint64_t u = mag[0];
        uint64_t v = divisor.mag[0];

        if (mag.size() == 2) {
            u |= static_cast<uint64_t>(mag[1]) << 32;
        }
        if (divisor.mag.size() == 2) {
            v |= static_cast<uint64_t>(divisor.mag[1]) << 32;
        }

        // Divisor can not be 1, or 0.
        assert(v > 1);

        // It should also be less than dividend.
        assert(v < u);

        // (u / v) is always fits into int64_t because abs(v) >= 2.
        res.assign_int64(resSign * static_cast<int64_t>(u / v));

        // (u % v) is always fits into int64_t because (u > v) ->
        // (u % v) < (u / 2).
        if (rem) {
            rem->assign_int64(resSign * static_cast<int64_t>(u % v));
        }

        return;
    }

    // Using Knuth division algorithm D for common case.

    // Short aliases.
    const mag_array &u = mag;
    const mag_array &v = divisor.mag;
    mag_array &q = res.mag;
    auto ulen = int32_t(u.size());
    auto vlen = int32_t(v.size());

    // First we need to normalize divisor.
    mag_array nv;
    nv.resize(v.size());

    int32_t shift = countl_zero(v.back());
    shift_left(v.data(), vlen, nv.data(), shift);

    // Divisor is normalized. Now we need to normalize dividend.
    mag_array nu;

    // First find out what is the size of it.
    if (countl_zero(u.back()) >= shift) {
        // Everything is the same as with divisor. Just add leading zero.
        nu.resize(ulen + 1);

        shift_left(u.data(), ulen, nu.data(), shift);

        assert((static_cast<uint64_t>(u.back()) >> (32 - shift)) == 0);
    } else {
        // We need one more byte here. Also adding leading zero.
        nu.resize(ulen + 2);

        shift_left(u.data(), ulen, nu.data(), shift);

        nu[ulen] = u[ulen - 1] >> (32 - shift);

        assert(nu[ulen] != 0);
    }

    assert(nu.back() == 0);

    // Resizing resulting array.
    q.resize(ulen - vlen + 1);

    // Main loop
    for (int32_t i = ulen - vlen; i >= 0; --i) {
        uint64_t base = make_u64(nu[i + vlen], nu[i + vlen - 1]);

        uint64_t qhat = base / nv[vlen - 1]; // Estimated quotient.
        uint64_t rhat = base % nv[vlen - 1]; // A remainder.

        // Adjusting result if needed.
        while (qhat > UINT32_MAX
            || ((qhat * nv[vlen - 2]) > ((UINT32_MAX + static_cast<uint64_t>(1)) * rhat + nu[i + vlen - 2]))) {
            --qhat;
            rhat += nv[vlen - 1];

            if (rhat > UINT32_MAX)
                break;
        }

        auto qhat32 = static_cast<uint32_t>(qhat);

        // Multiply and subtract.
        uint32_t carry = multiply_and_subtract(nu.data() + i, nv.data(), vlen, qhat32);

        int64_t difference = nu[i + vlen] - carry;

        nu[i + vlen] = static_cast<uint32_t>(difference);

        if (difference < 0) {
            --qhat32;
            carry = ignite::add(nu.data() + i, nv.data(), vlen);

            assert(carry == 0);
        }

        q[i] = qhat32;
    }

    res.sign = resSign;
    res.normalize();

    // If remainder is needed denormalize it.
    if (rem) {
        rem->sign = resSign;
        rem->mag.resize(vlen);

        shift_right(nu.data(), int32_t(rem->mag.size()), rem->mag.data(), shift);

        rem->normalize();
    }
}

void big_integer::normalize() {
    int32_t lastNonZero = int32_t(mag.size()) - 1;
    while (lastNonZero >= 0 && mag[lastNonZero] == 0)
        --lastNonZero;

    mag.resize(lastNonZero + 1);
}

std::ostream &operator<<(std::ostream &os, const big_integer &val) {
    if (val.is_zero())
        return os << '0';

    if (val.sign < 0)
        os << '-';

    const int32_t maxResultDigits = 19;
    big_integer maxUintTenPower;
    big_integer res;
    big_integer left;

    maxUintTenPower.assign_uint64(10000000000000000000U);

    std::vector<uint64_t> vals;

    val.divide(maxUintTenPower, left, res);

    if (res.sign < 0)
        res.sign = int8_t(-res.sign);

    if (left.sign < 0)
        left.sign = int8_t(-left.sign);

    vals.push_back(static_cast<uint64_t>(res.to_int64()));

    while (!left.is_zero()) {
        left.divide(maxUintTenPower, left, res);

        vals.push_back(static_cast<uint64_t>(res.to_int64()));
    }

    os << vals.back();

    for (int32_t i = static_cast<int32_t>(vals.size()) - 2; i >= 0; --i) {
        os.fill('0');
        os.width(maxResultDigits);

        os << vals[i];
    }

    return os;
}

std::istream &operator>>(std::istream &is, big_integer &val) {
    std::istream::sentry sentry(is);

    // Return zero if input failed.
    val.assign_int64(0);

    if (!is)
        return is;

    // Current value parts.
    uint64_t part = 0;
    int32_t partDigits = 0;
    int32_t sign = 1;

    big_integer pow;
    big_integer bigPart;

    // Current char.
    int c = is.peek();

    if (!is)
        return is;

    // Checking sign.
    if (c == '-' || c == '+') {
        if (c == '-')
            sign = -1;

        is.ignore();
        c = is.peek();
    }

    // Reading number itself.
    while (is && isdigit(c)) {
        part = part * 10 + (c - '0');
        ++partDigits;

        if (part >= 1000000000000000000U) {
            big_integer::get_power_of_ten(partDigits, pow);
            val.multiply(pow, val);

            val.add(part);

            part = 0;
            partDigits = 0;
        }

        is.ignore();
        c = is.peek();
    }

    // Adding last part of the number.
    if (partDigits) {
        big_integer::get_power_of_ten(partDigits, pow);

        val.multiply(pow, val);

        val.add(part);
    }

    if (sign < 0)
        val.negate();

    return is;
}

} // namespace ignite
