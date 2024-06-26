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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

struct mbedtls_mpi;

namespace ignite::detail {

/**
 * Enum with the possible values of sign.
 */
enum mpi_sign : short { POSITIVE = 1, NEGATIVE = -1 };

/**
 * MbedTLS MPI struct wrapper.
 */
struct mpi {
    // Internal type.
    using type = mbedtls_mpi;
    // mpi word type.
    using word = std::uint32_t;

    /**
     * Support class for the mpi magnitude.
     */
    struct mag_view {
        mag_view(mpi::word *ptr, const unsigned short sz)
            : m_ptr{ptr}
            , m_size{sz} {}

        /** Returns size of the magnitude. */
        [[nodiscard]] std::size_t size() const noexcept { return m_size; }

        /** Subscript operator. */
        [[nodiscard]] const mpi::word &operator[](const std::size_t n) const { return m_ptr[n]; }

        /** Subscript operator. */
        [[nodiscard]] mpi::word &operator[](const std::size_t n) { return m_ptr[n]; }

        /** Checks if the magnitude is empty. */
        [[nodiscard]] bool empty() const noexcept { return size() == 0; }

        /** Returns pointer to the magnitude beginning. */
        [[nodiscard]] mpi::word *begin() { return m_ptr; }

        /** Returns pointer to the element past last. */
        [[nodiscard]] mpi::word *end() { return m_ptr + m_size; }

        /** Returns const pointer to the magnitude beginning. */
        [[nodiscard]] const mpi::word *begin() const { return m_ptr; }

        /** Returns const pointer to the element past last. */
        [[nodiscard]] const mpi::word *end() const { return m_ptr + m_size; }

        /** Returns const reference to the last element. */
        [[nodiscard]] const mpi::word &back() const { return m_ptr[m_size - 1]; }

        /** Returns reference to the last element. */
        [[nodiscard]] mpi::word &back() { return m_ptr[m_size - 1]; }

        /** Returns const reference to the last element. */
        [[nodiscard]] const mpi::word &front() const { return m_ptr[0]; }

        /** Returns reference to the last element. */
        [[nodiscard]] mpi::word &front() { return m_ptr[0]; }

    private:
        /** Pointer to the magnitude array. */
        mpi::word *const m_ptr;
        /** Size of the array. */
        unsigned short m_size;
    };

    /** Default constructor. */
    mpi();

    /** Constructor mpi from the int value. */
    mpi(std::int32_t v);
    /** Construct mpi from the string. */
    mpi(const char *string);

    /** Destructor. */
    ~mpi();

    /** Copy constructor. */
    mpi(const mpi &other);
    /** Move constructor. */
    mpi(mpi &&other) noexcept;

    /** Assignment operator. */
    mpi &operator=(const mpi &other);
    /** Move operator. */
    mpi &operator=(mpi &&other) noexcept;

    /** Implicit conversion to the pointer to the mbedtls_mpi. */
    [[nodiscard]] type *get() { return val; }

    /** Implicit conversion to the pointer to the const mbedtls_mpi. */
    [[nodiscard]] const type *get() const { return val; }

    /** Arrow operator. */
    [[nodiscard]] type *operator->() { return val; }

    /** Arrow operator. */
    [[nodiscard]] const type *operator->() const { return val; }

    /** Init internal mpi structure. */
    void init();
    /** Free internal mpi structure. */
    void free();
    /** Reinit internal mpi structure. Calls \c free and \c init. */
    void reinit();

    /** Returns mpi sign. */
    [[nodiscard]] mpi_sign sign() const noexcept;

    /** Returns pointer to the mpi magnitude. */
    [[nodiscard]] word *pointer() const noexcept;

    /** Returns length of the mpi magnitude. */
    [[nodiscard]] unsigned short length() const noexcept;

    /** Returns view of the magnitude. */
    [[nodiscard]] mag_view magnitude() const noexcept;

    /** Returns length of the magnitude in bits. */
    [[nodiscard]] std::size_t magnitude_bit_length() const noexcept;

    /** Returns true if mpi is zero. */
    [[nodiscard]] bool is_zero() const noexcept;
    /** Returns true if mpi is positive. */
    [[nodiscard]] bool is_positive() const noexcept;
    /** Returns true if mpi is negative. */
    [[nodiscard]] bool is_negative() const noexcept;

    /** Sets mpi sign. */
    void set_sign(mpi_sign sign);
    /** Make mpi positive. */
    void make_positive() noexcept;
    /** Make mpi negative. */
    void make_negative() noexcept;
    /** Change mpi sign. */
    void negate() noexcept;

    /** Shrink internal mpi representation downwards to keep at least \c limbs limbs.*/
    void shrink(size_t limbs = 0);
    /** Grow internal mpi representation to the \c limbs limbs.*/
    void grow(size_t limbs);

    /**
     * Compares mpi with another one.
     * @param other Another mpi.
     * @param ignore_sign If true ignores sign in comparison.
     * @return 1 if this mpi bigger than other, 0 if equal, -1 if less.
     */
    [[nodiscard]] int compare(const mpi &other, bool ignore_sign = false) const noexcept;

    /** Adds another mpi to this one. */
    void add(const mpi &addendum);
    /** Subtracts another mpi from this one. */
    void subtract(const mpi &subtrahend);
    /** Multiplies this mpi on another one. */
    void multiply(const mpi &factor);
    /** Divides this mpi on another one. */
    void divide(const mpi &divisor);
    /** Computes modulo with the another mpi. */
    void modulo(const mpi &divisor);

    /** Sum operator. */
    mpi operator+(const mpi &addendum) const;
    /** Subtract operator. */
    mpi operator-(const mpi &subtrahend) const;
    /** Multiply operator. */
    mpi operator*(const mpi &factor) const;
    /** Divide operator. */
    mpi operator/(const mpi &divisor) const;
    /** Modulo operator. */
    mpi operator%(const mpi &divisor) const;
    /** Equality operator. */
    bool operator==(const mpi &other) const;

    /**
     * Calculates quotient and remainder of division in one operation.
     *
     * @param divisor Divisor mpi.
     * @param remainder Reference to the mpi that will hold remainder after division complete.
     *
     * @return Quotient mpi.
     */
    mpi div_and_mod(const mpi &divisor, mpi &remainder) const;

    /** Swaps this mpi with another one. */
    friend void swap(mpi &lhs, mpi &rhs);

    /** Reads mpi value from string. */
    void assign_from_string(const char *string);
    /** Writes mpi to the string. */
    [[nodiscard]] std::string to_string() const;

    /**
     * Writes mpi magnitude in the byte array. Sign and size will not be written.
     *
     * @param data Pointer to the byte array.
     * @param size Size of the byte array.
     * @param big_endian Write as big-endian if true, little-endian otherwise.
     *
     * @return True if write successful, false otherwise.
     * */
    bool write(std::uint8_t *data, std::size_t size, bool big_endian = true);

    /**
     * Reads mpi magnitude from the byte array. Reads only magnitude, sign should be set separately.
     *
     * @param data Pointer to the byte array.
     * @param size Size of the byte array.
     * @param big_endian Read as big-endian if true, little-endian otherwise.
     *
     * @return True if read successful, false otherwise.
     * */
    bool read(const std::uint8_t *data, std::size_t size, bool big_endian = true);

private:
    /** Internal MbedTLS mpi structure. */
    type *val;
};

} // namespace ignite::detail
