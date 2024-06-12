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

#include "bytes_view.h"
#include "config.h"

#include "mpi.h"

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
class big_integer {
    friend class big_decimal;

    using mpi_t = mpi;
    using word_t = mpi_t::word;

public:
    // Magnitude array type.
    using mag_array_view = mpi::mag_view;

    /**
     * Default constructor. Constructs zero-value big integer.
     */
    big_integer() = default;

    /**
     * Copy constructor.
     *
     * @param other Other value.
     */
    big_integer(const big_integer &other) = default;

    /**
     * Move constructor.
     *
     * @param other Other value.
     */
    big_integer(big_integer &&other) noexcept = default;

    /**
     * Constructs big integer with the specified integer value.
     *
     * @param val Value.
     */
    explicit big_integer(std::int64_t val) { assign_int64(val); }

    /**
     * String constructor.
     *
     * @param val String to assign.
     */
    explicit big_integer(const std::string &val) { assign_string(val); }

    /**
     * String constructor.
     *
     * @param val String to assign.
     * @param len String length.
     */
    big_integer(const char *val, std::int32_t len) { assign_string(val, len); }

    /**
     * Constructs big integer from the byte array.
     *
     * @param val Bytes of the integer. Byte order is big-endian.
     * @param len Array length.
     * @param sign Signum. Can be -1 (negative) or 1 (positive or zero).
     * @param bigEndian If true then magnitude is in big-endian. Otherwise
     *     the byte order of the magnitude considered to be little-endian.
     */
    big_integer(const std::int8_t *val, std::int32_t len, std::int8_t sign, bool big_endian = true);

    /**
     * Constructs a big integer from the byte array.
     *
     * @param data Bytes of the integer. Byte order is big-endian. The representation is two's-complement.
     * @param size The number of bytes.
     */
    big_integer(const std::byte *data, std::size_t size);

    /**
     * Copy-assigment operator.
     *
     * @param other Other value.
     * @return *this.
     */
    big_integer &operator=(const big_integer &other) = default;

    /**
     * Move-assigment operator.
     *
     * @param other Other value.
     * @return *this.
     */
    big_integer &operator=(big_integer &&other) noexcept = default;

    /**
     * Copy-assigment operator.
     *
     * @param other Other value.
     * @return *this.
     */
    big_integer &operator=(const mpi &other) {
        if (&other == &this->m_mpi) {
            return *this;
        }

        m_mpi = other;

        return *this;
    }

    /**
     * Move-assigment operator.
     *
     * @param other Other value.
     * @return *this.
     */
    big_integer &operator=(mpi &&other) noexcept {
        using std::swap;

        if (&other == &this->m_mpi) {
            return *this;
        }

        swap(m_mpi, other);

        return *this;
    }

    /**
     * Assign specified value to this big_integer.
     *
     * @param val Value to assign.
     */
    void assign_int64(std::int64_t val);

    /**
     * Assign specified value to this big_integer.
     *
     * @param val Value to assign.
     */
    void assign_uint64(std::uint64_t val);

    /**
     * Assign specified value to this Decimal.
     *
     * @param val String to assign.
     */
    void assign_string(const std::string &val);

    /**
     * Assign specified value to this Decimal.
     *
     * @param val String to assign.
     * @param len String length.
     */
    void assign_string(const char *val, std::size_t len);

    /**
     * Get number sign. Returns -1 if negative and 1 otherwise.
     *
     * @return Sign of the number.
     */
    [[nodiscard]] std::int8_t get_sign() const noexcept { return std::int8_t(m_mpi.sign()); }

    /**
     * Get magnitude array.
     *
     * @return magnitude array.
     */
    [[nodiscard]] mag_array_view get_magnitude() const noexcept { return m_mpi.magnitude(); }

    /**
     * Get length in bits of the two's-complement representation of this number, excluding a sign bit.
     *
     * @return Length in bits of the two's-complement representation of this number, excluding a sign bit.
     */
    [[nodiscard]] std::uint32_t bit_length() const noexcept;

    /**
     * Get number of bytes required to store this number as byte array.
     *
     * @return Number of bytes required to store this number as byte array.
     */
    [[nodiscard]] std::size_t byte_size() const noexcept;

    /**
     * Store this number as a byte array.
     *
     * @param data Destination byte array. Its size must be at least as large as the value returned by @ref
     * bytes_size();
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
     * Get precision of the BigInteger.
     *
     * @return Number of the decimal digits in the decimal representation
     *     of the value.
     */
    [[nodiscard]] std::int32_t get_precision() const noexcept;

    /**
     * Mutates this BigInteger so its value becomes exp power of this.
     *
     * @param exp Exponent.
     */
    void pow(std::int32_t exp);

    /**
     * Muitiply this to another big integer.
     *
     * @param other Another instance. Can be *this.
     * @param res Result placed there. Can be *this.
     */
    void multiply(const big_integer &other, big_integer &res) const;

    /**
     * Divide this to another big integer.
     *
     * @param divisor Divisor. Can be *this.
     * @param res Result placed there. Can be *this.
     */
    void divide(const big_integer &divisor, big_integer &res) const;

    /**
     * Divide this to another big integer.
     *
     * @param divisor Divisor. Can be *this.
     * @param res Result placed there. Can be *this.
     * @param rem Remainder placed there. Can be *this.
     */
    void divide(const big_integer &divisor, big_integer &res, big_integer &rem) const;

    /**
     * Add another big integer to this.
     *
     * @param other Addendum. Can be *this.
     * @param res Result placed there. Can be *this.
     */
    void add(const big_integer &other, big_integer &res) const;

    /**
     * Subtract another big integer from this.
     *
     * @param other Subtrahend. Can be *this.
     * @param res  Result placed there. Can be *this.
     */
    void subtract(const big_integer &other, big_integer &res) const;

    /**
     * Add unsigned integer number to this big_integer.
     *
     * @param x Number to add.
     */
    void add(std::uint64_t x);

    /**
     * compare this instance to another.
     *
     * @param other Another instance.
     * @param ignore_sign If set to true than only magnitudes are compared.
     * @return Comparasion result - 0 if equal, 1 if this is greater, -1 if
     *     this is less.
     */
    [[nodiscard]] int compare(const big_integer &other, bool ignore_sign = false) const;

    /**
     * Convert to int64_t.
     *
     * @return int64_t value.
     */
    [[nodiscard]] std::int64_t to_int64() const;

    /**
     * Check whether this value is negative.
     *
     * @return True if this value is negative and false otherwise.
     */
    [[nodiscard]] bool is_negative() const noexcept { return m_mpi.is_negative(); }

    /**
     * Check whether this value is zero.
     *
     * @return True if this value is negative and false otherwise.
     */
    [[nodiscard]] bool is_zero() const noexcept { return m_mpi.is_zero(); }

    /**
     * Check whether this value is positive.
     *
     * @return True if this value is positive and false otherwise.
     */
    [[nodiscard]] bool is_positive() const noexcept { return m_mpi.is_positive(); }

    /**
     * Reverses sign of this value.
     */
    void negate() { m_mpi.negate(); }

    /**
     * Converts value to string.
     */
    [[nodiscard]] std::string to_string() const;

    /**
     * Output operator.
     *
     * @param os Output stream.
     * @param val Value to output.
     * @return Reference to the first param.
     */
    friend std::ostream &operator<<(std::ostream &os, const big_integer &val);

    /**
     * Input operator.
     *
     * @param is Input stream.
     * @param val Value to input.
     * @return Reference to the first param.
     */
    friend std::istream &operator>>(std::istream &is, big_integer &val);

    /**
     * Get big_integer which value is the ten of the specified power.
     *
     * @param pow Tenth power.
     * @param res Result is placed here.
     */
    static void get_power_of_ten(std::int32_t pow, big_integer &res);

    /**
     * Swap function for the big_integer type.
     *
     * @param other Other instance.
     */
    friend void swap(big_integer &lhs, big_integer &rhs);

private:
    /**
     * Get this number's length in bits as if it was positive.
     *
     * @return Number's length in bits.
     */
    [[nodiscard]] std::uint32_t magnitude_bit_length() const noexcept;

    /**
     * Initializes a big integer from a byte array with big-endian byte order.
     *
     * @param data Byte array.
     * @param size Byte array size.
     */
    void from_big_endian(const std::byte *data, std::size_t size);

    /**
     * Initializes a big integer from a byte array with big-endian byte order and a negative value
     * represenetd as two's-complement.
     *
     * @param data Byte array.
     * @param size Byte array size.
     */
    void from_negative_big_endian(const std::byte *data, std::size_t size);

    /**
     * Add magnitude array to current.
     *
     * @param addend Addend.
     * @param len Length of the addend.
     */
    void add(const std::uint32_t *addend, int32_t len);

    /**
     * Divide this to another big integer.
     *
     * @param divisor Divisor. Can be *this.
     * @param res Result placed there. Can be *this.
     * @param rem Remainder placed there if requested. Can be *this.
     *     Can be null if the remainder is not needed.
     */
    void divide(const big_integer &divisor, big_integer &res, big_integer *rem) const;

    /**
     * Internal multiprecision integer structure.
     */
    mpi_t m_mpi;
};

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is equal to the second.
 */
inline bool operator==(const big_integer &lhs, const big_integer &rhs) noexcept {
    return lhs.compare(rhs) == 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is not equal to the second.
 */
inline bool operator!=(const big_integer &lhs, const big_integer &rhs) noexcept {
    return lhs.compare(rhs) != 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is less than the second.
 */
inline bool operator<(const big_integer &lhs, const big_integer &rhs) noexcept {
    return lhs.compare(rhs) < 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is less than or equal to the second.
 */
inline bool operator<=(const big_integer &lhs, const big_integer &rhs) noexcept {
    return lhs.compare(rhs) <= 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is greater than the second.
 */
inline bool operator>(const big_integer &lhs, const big_integer &rhs) noexcept {
    return lhs.compare(rhs) > 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is greater than or equal to the second.
 */
inline bool operator>=(const big_integer &lhs, const big_integer &rhs) noexcept {
    return lhs.compare(rhs) >= 0;
}

/**
 * @brief Sum operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return New value that holds result of lhs + rhs.
 */
inline big_integer operator+(const big_integer &lhs, const big_integer &rhs) noexcept {
    big_integer res;
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
inline big_integer operator-(const big_integer &lhs, const big_integer &rhs) noexcept {
    big_integer res;
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
inline big_integer operator*(const big_integer &lhs, const big_integer &rhs) noexcept {
    big_integer res;
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
inline big_integer operator/(const big_integer &lhs, const big_integer &rhs) noexcept {
    big_integer res;
    lhs.divide(rhs, res);
    return res;
}

/**
 * @brief Modulo operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return New value that holds result of lhs % rhs.
 */
inline big_integer operator%(const big_integer &lhs, const big_integer &rhs) noexcept {
    big_integer res;
    big_integer rem;
    lhs.divide(rhs, res, rem);
    return rem;
}

} // namespace ignite
