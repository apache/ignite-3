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

#include "BigInteger.h"

#include "common/bits.h"
#include "common/bytes.h"
#include "common/ignite_error.h"

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
inline uint64_t makeU64(uint32_t higher, uint32_t lower) {
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
void ShiftLeft(const uint32_t *in, int32_t len, uint32_t *out, unsigned n) {
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
void ShiftRight(const uint32_t *in, int32_t len, uint32_t *out, unsigned n) {
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
 * @param a Multipliplier of the subtrahend.
 * @param alen Length of the a.
 * @param x Multipliplicand of the subtrahend.
 * @return Carry.
 */
uint32_t MultiplyAndSubstruct(uint32_t *q, const uint32_t *a, int32_t alen, uint32_t x) {
    uint64_t carry = 0;

    for (int32_t i = 0; i < alen; ++i) {
        uint64_t product = a[i] * static_cast<uint64_t>(x);
        int64_t difference = q[i] - carry - (product & 0xFFFFFFFF);

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
uint32_t Add(uint32_t *res, const uint32_t *addend, int32_t len) {
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
uint32_t Add(uint32_t *res, int32_t len, uint32_t addend) {
    uint64_t carry = addend;

    for (int32_t i = 0; (i < len) && carry; ++i) {
        uint64_t sum = static_cast<uint64_t>(res[i]) + carry;
        res[i] = static_cast<uint32_t>(sum);
        carry = sum >> 32;
    }

    return static_cast<uint32_t>(carry);
}

} // namespace

void BigInteger::initializeBigEndian(const std::byte *data, std::size_t size) {
    while (size > 0 && data[0] == std::byte{0}) {
        size--;
        data++;
    }

    if (size == 0) {
        AssignUint64(0);
        return;
    }

    mag.resize((size + 3) / 4);

    for (std::size_t i = 0; size >= 4; i++) {
        size -= 4;
        mag[i] = bytes::load<Endian::BIG, std::uint32_t>(data + size);
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
        }
        mag.back() = last;
    }
}

void BigInteger::initializeNegativeBigEndian(const std::byte *data, std::size_t size) {
    assert(size > 0);
    assert(data[0] != std::byte{0});

    while (size > 0 && data[0] == std::byte{255}) {
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
        mag[i] = ~bytes::load<Endian::BIG, std::uint32_t>(data + size);
    }

    if (size > 0) {
        std::uint32_t last = 0xffffff;
        switch (size) {
            case 3:
                last ^= std::to_integer<std::uint32_t>(data[size - 3]) << 16;
                [[fallthrough]];
            case 2:
                last ^= std::to_integer<std::uint32_t>(data[size - 2]) << 8;
                [[fallthrough]];
            case 1:
                last ^= std::to_integer<std::uint32_t>(data[size - 1]);
                break;
        }
        mag.back() = last;
    }

    for (std::size_t i = 0; i < mag.size(); i++) {
        mag[i]++;
        if (mag[i] != 0) {
            break;
        }
    }
}

BigInteger::BigInteger(const int8_t *val, int32_t len, int32_t sign, bool bigEndian)
    : sign(sign) {
    assert(val != 0);
    assert(len >= 0);
    assert(sign == 1 || sign == 0 || sign == -1);

    std::size_t size = len;
    const std::byte *data = (const std::byte *)(val);

    if (bigEndian) {
        initializeBigEndian(data, size);
    } else {
        while (size > 0 && data[size - 1] != std::byte{0}) {
            --size;
        }

        if (size == 0) {
            AssignInt64(0);
            return;
        }

        mag.resize((size + 3) / 4);

        for (std::size_t i = 0; size >= 4; i++) {
            mag[i] = bytes::load<Endian::LITTLE, std::uint32_t>(data);
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
            }
            mag.back() = last;
        }
    }
}

BigInteger::BigInteger(const std::byte *data, std::size_t size) {
    if (size == 0) {
        return;
    }

    if (std::to_integer<std::int8_t>(data[0]) >= 0) {
        initializeBigEndian(data, size);
    } else {
        initializeNegativeBigEndian(data, size);
    }
}

void BigInteger::AssignInt64(int64_t val) {
    if (val < 0) {
        AssignUint64(val > INT64_MIN ? static_cast<uint64_t>(-val) : static_cast<uint64_t>(val));
        sign = -1;
    } else {
        AssignUint64(static_cast<uint64_t>(val));
    }
}

void BigInteger::AssignUint64(uint64_t val) {
    sign = 1;

    if (val == 0) {
        mag.clear();
        return;
    }

    uint32_t highWord = static_cast<uint32_t>(val >> 32);

    if (highWord == 0)
        mag.resize(1);
    else {
        mag.resize(2);
        mag[1] = highWord;
    }

    mag[0] = static_cast<uint32_t>(val);
}

void BigInteger::AssignString(const char *val, int32_t len) {
    std::stringstream converter;

    converter.write(val, len);

    converter >> *this;
}

void BigInteger::Swap(BigInteger &other) {
    using std::swap;

    swap(sign, other.sign);
    mag.swap(other.mag);
}

uint32_t BigInteger::GetBitLength() const {
    if (mag.empty())
        return 0;

    uint32_t res = bit_width(mag.back());
    if (mag.size() > 1)
        res += (mag.size() - 1) * 32;

    return res;
}

#if 0
std::size_t BigInteger::GetByteSize() const {
    return (GetBitLength() + 7u) / 8u;
}
#endif

int32_t BigInteger::GetPrecision() const {
    // See http://graphics.stanford.edu/~seander/bithacks.html
    // for the details on the algorithm.

    if (mag.size() == 0)
        return 1;

    int32_t r = static_cast<uint32_t>(((static_cast<uint64_t>(GetBitLength()) + 1) * 646456993) >> 31);

    BigInteger prec;
    BigInteger::GetPowerOfTen(r, prec);

    return compare(prec, true) < 0 ? r : r + 1;
}

void BigInteger::Pow(int32_t exp) {
    if (exp < 0) {
        AssignInt64(0);
        return;
    }

    uint32_t bitsLen = GetBitLength();

    if (!bitsLen)
        return;

    if (bitsLen == 1) {
        if ((exp % 2 == 0) && sign < 0)
            sign = -sign;
        return;
    }

    BigInteger multiplicant(*this);
    AssignInt64(1);

    int32_t mutExp = exp;
    while (mutExp) {
        if (mutExp & 1) {
            Multiply(multiplicant, *this);
        }

        mutExp >>= 1;

        if (mutExp) {
            multiplicant.Multiply(multiplicant, multiplicant);
        }
    }
}

void BigInteger::Multiply(const BigInteger &other, BigInteger &res) const {
    MagArray resMag(mag.size() + other.mag.size());

    resMag.resize(mag.size() + other.mag.size());

    for (int32_t i = 0; i < other.mag.size(); ++i) {
        uint32_t carry = 0;

        for (int32_t j = 0; j < mag.size(); ++j) {
            uint64_t product = static_cast<uint64_t>(mag[j]) * other.mag[i] + +resMag[i + j] + carry;

            resMag[i + j] = static_cast<uint32_t>(product);
            carry = static_cast<uint32_t>(product >> 32);
        }

        resMag[i + mag.size()] = carry;
    }

    res.mag.swap(resMag);
    res.sign = sign * other.sign;

    res.Normalize();
}

void BigInteger::Divide(const BigInteger &divisor, BigInteger &res) const {
    Divide(divisor, res, 0);
}

void BigInteger::Divide(const BigInteger &divisor, BigInteger &res, BigInteger &rem) const {
    Divide(divisor, res, &rem);
}

void BigInteger::Add(const uint32_t *addend, int32_t len) {
    if (mag.size() < len) {
        mag.reserve(len + 1);
        mag.resize(len);
    } else {
        mag.reserve(mag.size() + 1);
    }

    if (uint32_t carry = ignite::Add(mag.data(), addend, len)) {
        carry = ignite::Add(mag.data() + len, mag.size() - len, carry);
        if (carry) {
            mag.push_back(carry);
        }
    }
}

void BigInteger::Add(uint64_t x) {
    if (x == 0)
        return;

    if (IsZero()) {
        AssignUint64(x);
        return;
    }

    uint32_t val[2];

    val[0] = static_cast<uint32_t>(x);
    val[1] = static_cast<uint32_t>(x >> 32);

    Add(val, val[1] ? 2 : 1);
}

int BigInteger::compare(const BigInteger &other, bool ignoreSign) const {
    // What we should return if magnitude is greater.
    int32_t mgt = 1;

    if (!ignoreSign) {
        if (sign != other.sign)
            return sign > other.sign ? 1 : -1;
        mgt = sign;
    }

    if (mag.size() != other.mag.size())
        return mag.size() > other.mag.size() ? mgt : -mgt;

    for (int32_t i = mag.size() - 1; i >= 0; --i) {
        if (mag[i] != other.mag[i]) {
            return mag[i] > other.mag[i] ? mgt : -mgt;
        }
    }

    return 0;
}

int64_t BigInteger::ToInt64() const {
    return (static_cast<uint64_t>(GetMagInt(1)) << 32) | GetMagInt(0);
}

void BigInteger::GetPowerOfTen(int32_t pow, BigInteger &res) {
    assert(pow >= 0);

    constexpr auto n64 = std::numeric_limits<uint64_t>::digits10 + 1;
    if (pow < n64) {
        static const auto power10 = [] {
            std::array<std::uint64_t, n64> a;
            uint64_t power = 1;
            for (int i = 0; i < n64; i++) {
                a[i] = power;
                power *= 10;
            }
            return a;
        }();
        res.AssignUint64(power10[pow]);
    } else {
        res.AssignInt64(10);
        res.Pow(pow);
    }
}

uint32_t BigInteger::GetMagInt(int32_t n) const {
    assert(n >= 0);

    if (n >= mag.size())
        return sign > 0 ? 0 : -1;

    return sign * mag[n];
}

void BigInteger::Divide(const BigInteger &divisor, BigInteger &res, BigInteger *rem) const {
    // Can't divide by zero.
    if (divisor.mag.empty())
        throw ignite_error(status_code::GENERIC, "Division by zero.");

    int32_t compRes = compare(divisor, true);
    int8_t resSign = sign * divisor.sign;

    // The same magnitude. Result is [-]1 and remainder is zero.
    if (compRes == 0) {
        res.AssignInt64(resSign);

        if (rem) {
            rem->AssignInt64(0);
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

        res.AssignInt64(0);

        return;
    }

    // If divisor is [-]1 result is [-]this and remainder is zero.
    if (divisor.GetBitLength() == 1) {
        // Once again: order is important.
        res = *this;
        res.sign = sign * divisor.sign;

        if (rem) {
            rem->AssignInt64(0);
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
        res.AssignInt64(resSign * static_cast<int64_t>(u / v));

        // (u % v) is always fits into int64_t because (u > v) ->
        // (u % v) < (u / 2).
        if (rem) {
            rem->AssignInt64(resSign * static_cast<int64_t>(u % v));
        }

        return;
    }

    // Using Knuth division algorithm D for common case.

    // Short aliases.
    const MagArray &u = mag;
    const MagArray &v = divisor.mag;
    MagArray &q = res.mag;
    int32_t ulen = u.size();
    int32_t vlen = v.size();

    // First we need to normilize divisor.
    MagArray nv;
    nv.resize(v.size());

    uint32_t shift = countl_zero(v.back());
    ShiftLeft(v.data(), vlen, nv.data(), shift);

    // Divisor is normilized. Now we need to normilize divident.
    MagArray nu;

    // First find out what is the size of it.
    if (countl_zero(u.back()) >= shift) {
        // Everything is the same as with divisor. Just add leading zero.
        nu.resize(ulen + 1);

        ShiftLeft(u.data(), ulen, nu.data(), shift);

        assert((static_cast<uint64_t>(u.back()) >> (32 - shift)) == 0);
    } else {
        // We need one more byte here. Also adding leading zero.
        nu.resize(ulen + 2);

        ShiftLeft(u.data(), ulen, nu.data(), shift);

        nu[ulen] = u[ulen - 1] >> (32 - shift);

        assert(nu[ulen] != 0);
    }

    assert(nu.back() == 0);

    // Resizing resulting array.
    q.resize(ulen - vlen + 1);

    // Main loop
    for (int32_t i = ulen - vlen; i >= 0; --i) {
        uint64_t base = makeU64(nu[i + vlen], nu[i + vlen - 1]);

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

        uint32_t qhat32 = static_cast<uint32_t>(qhat);

        // Multiply and subtract.
        uint32_t carry = MultiplyAndSubstruct(nu.data() + i, nv.data(), vlen, qhat32);

        int64_t difference = nu[i + vlen] - carry;

        nu[i + vlen] = static_cast<uint32_t>(difference);

        if (difference < 0) {
            --qhat32;
            carry = ignite::Add(nu.data() + i, nv.data(), vlen);

            assert(carry == 0);
        }

        q[i] = qhat32;
    }

    res.sign = resSign;
    res.Normalize();

    // If remainder is needed unnormolize it.
    if (rem) {
        rem->sign = resSign;
        rem->mag.resize(vlen);

        ShiftRight(nu.data(), rem->mag.size(), rem->mag.data(), shift);

        rem->Normalize();
    }
}

void BigInteger::Normalize() {
    int32_t lastNonZero = mag.size() - 1;
    while (lastNonZero >= 0 && mag[lastNonZero] == 0)
        --lastNonZero;

    mag.resize(lastNonZero + 1);
}

} // namespace ignite
