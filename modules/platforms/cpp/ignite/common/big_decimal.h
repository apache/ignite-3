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

#include "big_integer.h"

#include <cassert>
#include <cctype>
#include <cstdint>
#include <iostream>
#include <sstream>

namespace ignite {

/**
 * @brief Big decimal number implementation.
 *
 * TODO: Modernize this code to C++17 and update coding style
 */
class big_decimal {
public:
    // Default
    big_decimal() = default;

    /**
     * Constructor.
     *
     * @param mag Bytes of the magnitude. Should be positive, sign is
     *     passed using separate argument.
     * @param len Magnitude length in bytes.
     * @param scale Scale.
     * @param sign Sign of the decimal. Should be -1 for negative numbers
     *     and 1 otherwise.
     * @param bigEndian If true then magnitude is in big-endian. Otherwise
     *     the byte order of the magnitude considered to be little-endian.
     */
    big_decimal(const std::int8_t *mag, std::int32_t len, std::int16_t scale, std::int8_t sign, bool big_endian = true)
        : m_scale(std::int16_t(scale & 0x7FFF))
        , m_magnitude(mag, len, sign, big_endian) {}

    /**
     * Constructs a big decimal from the byte array.
     *
     * @param data Bytes of the decimal. Scale in little byte order, magnitude as a @ref big_integer.
     * @param size The number of bytes.
     */
    big_decimal(const std::byte *data, std::size_t size);

    /**
     * Integer constructor.
     *
     * @param val Integer value.
     */
    explicit big_decimal(int64_t val)
        : m_magnitude(val) {}

    /**
     * Integer constructor with scale.
     *
     * @param val Integer value.
     * @param scale Scale.
     */
    big_decimal(int64_t val, int16_t scale)
        : m_scale(scale)
        , m_magnitude(val) {}

    /**
     * big_integer constructor with scale.
     *
     * @param val big_integer value.
     * @param scale Scale.
     */
    big_decimal(const big_integer &val, int16_t scale)
        : m_scale(scale)
        , m_magnitude(val) {}

    /**
     * big_integer constructor with scale.
     *
     * @param val big_integer value.
     * @param scale Scale.
     */
    big_decimal(big_integer &&val, int16_t scale)
        : m_scale(scale)
        , m_magnitude(std::forward<big_integer>(val)) {}

    /**
     * String constructor.
     *
     * @param val String to assign.
     * @param len String length.
     */
    explicit big_decimal(const char *val, int32_t len)
        : m_magnitude(0) {
        assign_string(val, len);
    }

    /**
     * String constructor.
     *
     * @param val String to assign.
     */
    explicit big_decimal(const std::string &val)
        : m_magnitude(0) {
        assign_string(val);
    }

    /**
     * From double.
     *
     * @param val Double value.
     * @return An instance of big_decimal from double.
     */
    static big_decimal from_double(double val) {
        big_decimal res;
        res.assign_double(val);
        return res;
    }

    /**
     * Get number of bytes required to store this decimal as byte array.
     *
     * @return Number of bytes required to store this decimal as byte array.
     */
    [[nodiscard]] std::size_t byte_size() const noexcept;

    /**
     * Store this decimal as a byte array.
     *
     * @param data Destination byte array. Its size must be at least as large as the value returned by @ref
     * byte_size();
     */
    void store_bytes(std::byte *data) const;

    /**
     * Convert value to bytes.
     *
     * @return Vector of bytes.
     */
    [[nodiscard]] std::vector<std::byte> to_bytes() const {
        std::vector<std::byte> bytes(byte_size());
        store_bytes(bytes.data());
        return bytes;
    }

    /**
     * Convert to double.
     */
    explicit operator double() const { return to_double(); }

    /**
     * Convert to int64_t.
     */
    explicit operator int64_t() const { return to_int64(); }

    /**
     * Convert to double.
     *
     * @return Double value.
     */
    [[nodiscard]] double to_double() const {
        std::stringstream stream;
        stream << *this;

        double result;
        stream >> result;
        return result;
    }

    /**
     * Convert to int64_t.
     *
     * @return int64_t value.
     */
    [[nodiscard]] int64_t to_int64() const {
        if (m_scale == 0) {
            return m_magnitude.to_int64();
        }

        big_decimal zero_scaled;

        set_scale(0, zero_scaled);

        return zero_scaled.m_magnitude.to_int64();
    }

    /**
     * Get scale.
     *
     * @return Scale.
     */
    [[nodiscard]] std::int16_t get_scale() const noexcept { return m_scale; }

    /**
     * Set scale.
     *
     * @param scale Scale to set.
     * @param res Result is placed here. Can be *this.
     */
    void set_scale(std::int16_t new_scale, big_decimal &res) const;

    /**
     * Get precision of the Decimal.
     *
     * @return Number of the decimal digits in the decimal representation
     *     of the value.
     */
    [[nodiscard]] std::int32_t get_precision() const noexcept { return m_magnitude.get_precision(); }

    /**
     * Get unscaled value.
     *
     * @return Unscaled value.
     */
    [[nodiscard]] const big_integer &get_unscaled_value() const noexcept { return m_magnitude; }

    /**
     * Swap function for the Decimal type.
     *
     * @param other Other instance.
     */
    friend void swap(big_decimal &lhs, big_decimal &rhs) {
        using std::swap;

        swap(lhs.m_scale, rhs.m_scale);
        swap(lhs.m_magnitude, rhs.m_magnitude);
    }

    /**
     * Assign specified value to this Decimal.
     *
     * @param val String to assign.
     */
    void assign_string(const std::string &val) { assign_string(val.data(), static_cast<int32_t>(val.size())); }

    /**
     * Assign specified value to this Decimal.
     *
     * @param val String to assign.
     * @param len String length.
     */
    void assign_string(const char *val, int32_t len) {
        std::stringstream converter;

        converter.write(val, len);

        converter >> *this;
    }

    /**
     * Assign specified value to this Decimal.
     *
     * @param val Value to assign.
     */
    void assign_int64(int64_t val) {
        m_magnitude.assign_int64(val);

        m_scale = 0;
    }

    /**
     * Assign specified value to this Decimal.
     *
     * @param val Value to assign.
     */
    void assign_double(double val) {
        std::stringstream converter;

        converter.precision(16);

        converter << val;
        converter >> *this;
    }

    /**
     * Assign specified value to this Decimal.
     *
     * @param val Value to assign.
     */
    void assign_uint64(uint64_t val) {
        m_magnitude.assign_uint64(val);

        m_scale = 0;
    }

    /**
     * Add another big decimal to this.
     *
     * @param other Addendum. Can be *this.
     * @param res Result placed there. Can be *this.
     */
    void add(const big_decimal &other, big_decimal &res) const;

    /**
     * Subtract another big decimal from this.
     *
     * @param other Subtrahend. Can be *this.
     * @param res  Result placed there. Can be *this.
     */
    void subtract(const big_decimal &other, big_decimal &res) const;

    /**
     * Muitiply this to another big decimal.
     *
     * @param other Another instance. Can be *this.
     * @param res Result placed there. Can be *this.
     */
    void multiply(const big_decimal &other, big_decimal &res) const;

    /**
     * Divide this to another big decimal.
     *
     * @param divisor Divisor. Can be *this.
     * @param res Result placed there. Can be *this.
     */
    void divide(const big_decimal &other, big_decimal &res) const;

    /**
     * Reverses sign of this value.
     */
    void negate() { m_magnitude.negate(); }

    /**
     * compare this instance to another.
     *
     * @param other Another instance.
     * @return Comparasion result - 0 if equal, 1 if this is greater, -1 if
     *     this is less.
     */
    [[nodiscard]] int compare(const big_decimal &other) const;

    /**
     * Check whether this value is negative.
     *
     * @return True if this value is negative and false otherwise.
     */
    [[nodiscard]] bool is_negative() const noexcept { return m_magnitude.is_negative(); }

    /**
     * Check whether this value is zero.
     *
     * @return True if this value is negative and false otherwise.
     */
    [[nodiscard]] bool is_zero() const noexcept { return m_magnitude.is_zero(); }

    /**
     * Check whether this value is positive.
     *
     * @return True if this value is positive and false otherwise.
     */
    [[nodiscard]] bool is_positive() const noexcept { return m_magnitude.is_positive(); }

    /**
     * Output operator.
     *
     * @param os Output stream.
     * @param val Value to output.
     * @return Reference to the first param.
     */
    friend std::ostream &operator<<(std::ostream &os, const big_decimal &val);

    /**
     * Input operator.
     *
     * @param is Input stream.
     * @param val Value to input.
     * @return Reference to the first param.
     */
    friend std::istream &operator>>(std::istream &is, big_decimal &val);

private:
    /** Scale. */
    std::int16_t m_scale = 0;

    /** Magnitude. */
    big_integer m_magnitude;
};

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is equal to the second.
 */
inline bool operator==(const big_decimal &lhs, const big_decimal &rhs) noexcept {
    return lhs.compare(rhs) == 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is not equal to the second.
 */
inline bool operator!=(const big_decimal &lhs, const big_decimal &rhs) noexcept {
    return lhs.compare(rhs) != 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is less than the second.
 */
inline bool operator<(const big_decimal &lhs, const big_decimal &rhs) noexcept {
    return lhs.compare(rhs) < 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is less than or equal to the second.
 */
inline bool operator<=(const big_decimal &lhs, const big_decimal &rhs) noexcept {
    return lhs.compare(rhs) <= 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is greater than the second.
 */
inline bool operator>(const big_decimal &lhs, const big_decimal &rhs) noexcept {
    return lhs.compare(rhs) > 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is greater than or equal to the second.
 */
inline bool operator>=(const big_decimal &lhs, const big_decimal &rhs) noexcept {
    return lhs.compare(rhs) >= 0;
}

/**
 * @brief Sum operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return New value that holds result of lhs + rhs.
 */
inline big_decimal operator+(const big_decimal &lhs, const big_decimal &rhs) noexcept {
    big_decimal res;
    lhs.add(rhs, res);
    return res;
}

/**
 * @brief Subtract operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return New value that holds result of lhs - rhs.
 */
inline big_decimal operator-(const big_decimal &lhs, const big_decimal &rhs) noexcept {
    big_decimal res;
    lhs.subtract(rhs, res);
    return res;
}

/**
 * @brief Multiply operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return New value that holds result of lhs * rhs.
 */
inline big_decimal operator*(const big_decimal &lhs, const big_decimal &rhs) noexcept {
    big_decimal res;
    lhs.multiply(rhs, res);
    return res;
}

/**
 * @brief Divide operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return New value that holds result of lhs / rhs.
 */
inline big_decimal operator/(const big_decimal &lhs, const big_decimal &rhs) noexcept {
    big_decimal res;
    lhs.divide(rhs, res);
    return res;
}

} // namespace ignite
