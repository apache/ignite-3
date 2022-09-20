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

#pragma once

#include "common/config.h"

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <vector>

namespace ignite {

/**
 * Big integer number implementation.
 *
 * TODO: Modernize this code to C++17 and update coding style
 */
class IGNITE_API BigInteger {
    friend class BigDecimal;

public:
    // Magnitude array type.
    using MagArray = std::vector<std::uint32_t>;

    /**
     * Default constructor. Constructs zero-value big integer.
     */
    BigInteger() = default;

    /**
     * Copy constructor.
     *
     * @param other Other value.
     */
    BigInteger(const BigInteger &other) = default;

    /**
     * Move constructor.
     *
     * @param other Other value.
     */
    BigInteger(BigInteger &&other) = default;

    /**
     * Constructs big integer with the specified magnitude.
     * @warning Magnitude is moved. This mean mag left empty after the call.
     *
     * @param mag Magnitude. Moved.
     * @param sign Sign. Can be 1 or -1.
     */
    BigInteger(MagArray &&mag, int8_t sign)
        : sign(sign)
        , mag(std::move(mag)) { }

    /**
     * Constructs big integer with the specified integer value.
     *
     * @param val Value.
     */
    explicit BigInteger(std::int64_t val) { AssignInt64(val); }

    /**
     * String constructor.
     *
     * @param val String to assign.
     */
    explicit BigInteger(const std::string &val) { AssignString(val); }

    /**
     * String constructor.
     *
     * @param val String to assign.
     * @param len String length.
     */
    BigInteger(const char *val, int32_t len) { AssignString(val, len); }

    /**
     * Constructs big integer from the byte array.
     *
     * @param val Bytes of the integer. Byte order is big-endian.
     * @param len Array length.
     * @param sign Signum. Can be -1 (negative) or 1 (positive or zero).
     * @param bigEndian If true then magnitude is in big-endian. Otherwise
     *     the byte order of the magnitude considered to be little-endian.
     */
    BigInteger(const int8_t *val, int32_t len, int32_t sign, bool bigEndian = true);

    /**
     * Constructs a big integer from the byte array.
     *
     * @param data Bytes of the integer. Byte order is big-endian. The representation is two's-complement.
     * @param size The number of bytes.
     */
    BigInteger(const std::byte *data, std::size_t size);

    /**
     * Copy-assigment operator.
     *
     * @param other Other value.
     * @return *this.
     */
    BigInteger &operator=(const BigInteger &other) = default;

    /**
     * Move-assigment operator.
     *
     * @param other Other value.
     * @return *this.
     */
    BigInteger &operator=(BigInteger &&other) = default;

    /**
     * Assign specified value to this BigInteger.
     *
     * @param val Value to assign.
     */
    void AssignInt64(int64_t val);

    /**
     * Assign specified value to this BigInteger.
     *
     * @param val Value to assign.
     */
    void AssignUint64(uint64_t val);

    /**
     * Assign specified value to this Decimal.
     *
     * @param val String to assign.
     */
    void AssignString(const std::string &val) { AssignString(val.data(), static_cast<int32_t>(val.size())); }

    /**
     * Assign specified value to this Decimal.
     *
     * @param val String to assign.
     * @param len String length.
     */
    void AssignString(const char *val, int32_t len);

    /**
     * Get number sign. Returns -1 if negative and 1 otherwise.
     *
     * @return Sign of the number.
     */
    std::int8_t GetSign() const noexcept { return sign; }

    /**
     * Get magnitude array.
     *
     * @return magnitude array.
     */
    const MagArray &GetMagnitude() const noexcept { return mag; }

    /**
     * Swap function for the BigInteger type.
     *
     * @param other Other instance.
     */
    void Swap(BigInteger &other);

    /**
     * Get this number length in bits as if it was positive.
     *
     * @return Number length in bits.
     */
    uint32_t GetBitLength() const;

    /**
     * Get precision of the BigInteger.
     *
     * @return Number of the decimal digits in the decimal representation
     *     of the value.
     */
    int32_t GetPrecision() const;

    /**
     * Mutates this BigInteger so its value becomes exp power of this.
     *
     * @param exp Exponent.
     */
    void Pow(int32_t exp);

    /**
     * Muitiply this to another big integer.
     *
     * @param other Another instance. Can be *this.
     * @param res Result placed there. Can be *this.
     */
    void Multiply(const BigInteger &other, BigInteger &res) const;

    /**
     * Divide this to another big integer.
     *
     * @param divisor Divisor. Can be *this.
     * @param res Result placed there. Can be *this.
     */
    void Divide(const BigInteger &divisor, BigInteger &res) const;

    /**
     * Divide this to another big integer.
     *
     * @param divisor Divisor. Can be *this.
     * @param res Result placed there. Can be *this.
     * @param rem Remainder placed there. Can be *this.
     */
    void Divide(const BigInteger &divisor, BigInteger &res, BigInteger &rem) const;

    /**
     * Add unsigned integer number to this BigInteger.
     *
     * @param x Number to add.
     */
    void Add(uint64_t x);

    /**
     * Compare this instance to another.
     *
     * @param other Another instance.
     * @param ignoreSign If set to true than only magnitudes are compared.
     * @return Comparasion result - 0 if equal, 1 if this is greater, -1 if
     *     this is less.
     */
    int compare(const BigInteger &other, bool ignoreSign = false) const;

    /**
     * Convert to int64_t.
     *
     * @return int64_t value.
     */
    int64_t ToInt64() const;

    /**
     * Check whether this value is negative.
     *
     * @return True if this value is negative and false otherwise.
     */
    bool IsNegative() const { return sign < 0; }

    /**
     * Check whether this value is zero.
     *
     * @return True if this value is negative and false otherwise.
     */
    bool IsZero() const { return mag.empty(); }

    /**
     * Check whether this value is positive.
     *
     * @return True if this value is positive and false otherwise.
     */
    bool IsPositive() const { return sign > 0 && !IsZero(); }

    /**
     * Rverses sign of this value.
     */
    void Negate() {
        if (!IsZero()) {
            sign = -sign;
        }
    }

    /**
     * Output operator.
     *
     * @param os Output stream.
     * @param val Value to output.
     * @return Reference to the first param.
     */
    friend std::ostream &operator<<(std::ostream &os, const BigInteger &val) {
        if (val.IsZero())
            return os << '0';

        if (val.sign < 0)
            os << '-';

        const int32_t maxResultDigits = 19;
        BigInteger maxUintTenPower;
        BigInteger res;
        BigInteger left;

        maxUintTenPower.AssignUint64(10000000000000000000U);

        std::vector<uint64_t> vals;

        val.Divide(maxUintTenPower, left, res);

        if (res.sign < 0)
            res.sign = -res.sign;

        if (left.sign < 0)
            left.sign = -left.sign;

        vals.push_back(static_cast<uint64_t>(res.ToInt64()));

        while (!left.IsZero()) {
            left.Divide(maxUintTenPower, left, res);

            vals.push_back(static_cast<uint64_t>(res.ToInt64()));
        }

        os << vals.back();

        for (int32_t i = static_cast<int32_t>(vals.size()) - 2; i >= 0; --i) {
            os.fill('0');
            os.width(maxResultDigits);

            os << vals[i];
        }

        return os;
    }

    /**
     * Input operator.
     *
     * @param is Input stream.
     * @param val Value to input.
     * @return Reference to the first param.
     */
    friend std::istream &operator>>(std::istream &is, BigInteger &val) {
        std::istream::sentry sentry(is);

        // Return zero if input failed.
        val.AssignInt64(0);

        if (!is)
            return is;

        // Current value parts.
        uint64_t part = 0;
        int32_t partDigits = 0;
        int32_t sign = 1;

        BigInteger pow;
        BigInteger bigPart;

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
                BigInteger::GetPowerOfTen(partDigits, pow);
                val.Multiply(pow, val);

                val.Add(part);

                part = 0;
                partDigits = 0;
            }

            is.ignore();
            c = is.peek();
        }

        // Adding last part of the number.
        if (partDigits) {
            BigInteger::GetPowerOfTen(partDigits, pow);

            val.Multiply(pow, val);

            val.Add(part);
        }

        if (sign < 0)
            val.Negate();

        return is;
    }

    /**
     * Get BigInteger which value is the ten of the specified power.
     *
     * @param pow Tenth power.
     * @param res Result is placed here.
     */
    static void GetPowerOfTen(int32_t pow, BigInteger &res);

private:
    /**
     * Initializes a big integer from a byte array with big-endian byte order.
     *
     * @param data Byte array.
     * @param size Byte array size.
     */
    void initializeBigEndian(const std::byte *data, std::size_t size);

    /**
     * Initializes a big integer from a byte array with big-endian byte order and a negative value
     * represenetd as two's-complement.
     *
     * @param data Byte array.
     * @param size Byte array size.
     */
    void initializeNegativeBigEndian(const std::byte *data, std::size_t size);

    /**
     * Add magnitude array to current.
     *
     * @param addend Addend.
     * @param len Length of the addend.
     */
    void Add(const uint32_t *addend, int32_t len);

    /**
     * Get n-th integer of the magnitude.
     *
     * @param n Index.
     * @return Value of the n-th int of the magnitude.
     */
    uint32_t GetMagInt(int32_t n) const;

    /**
     * Divide this to another big integer.
     *
     * @param divisor Divisor. Can be *this.
     * @param res Result placed there. Can be *this.
     * @param rem Remainder placed there if requested. Can be *this.
     *     Can be null if the remainder is not needed.
     */
    void Divide(const BigInteger &divisor, BigInteger &res, BigInteger *rem) const;

    /**
     * Normalizes current value removing trailing zeroes from the magnitude.
     */
    void Normalize();

    /** The sign of this BigInteger: -1 for negative and 1 for non-negative. */
    std::int8_t sign = 1;

    /** The magnitude of this BigInteger. Byte order is little-endian. */
    MagArray mag;
};

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is equal to the second.
 */
inline bool operator==(const BigInteger &lhs, const BigInteger &rhs) noexcept {
    return lhs.compare(rhs) == 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is not equal to the second.
 */
inline bool operator!=(const BigInteger &lhs, const BigInteger &rhs) noexcept {
    return lhs.compare(rhs) != 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is less than the second.
 */
inline bool operator<(const BigInteger &lhs, const BigInteger &rhs) noexcept {
    return lhs.compare(rhs) < 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is less than or equal to the second.
 */
inline bool operator<=(const BigInteger &lhs, const BigInteger &rhs) noexcept {
    return lhs.compare(rhs) <= 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is greater than the second.
 */
inline bool operator>(const BigInteger &lhs, const BigInteger &rhs) noexcept {
    return lhs.compare(rhs) > 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is greater than or equal to the second.
 */
inline bool operator>=(const BigInteger &lhs, const BigInteger &rhs) noexcept {
    return lhs.compare(rhs) >= 0;
}

} // namespace ignite
