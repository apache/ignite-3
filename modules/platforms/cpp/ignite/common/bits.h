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

#include "ignite/common/detail/config.h"

#include <array>
#include <cassert>
#include <climits>
#include <cstdint>
#include <type_traits>

/**
 * Maximum number of digits in std::uint64_t number.
 */
const std::int32_t UINT64_MAX_PRECISION = 20;

#if defined(__cpp_lib_bitops) && __cpp_lib_bitops >= 201907L
# define IGNITE_STD_BITOPS 1
#endif
#if defined(__cpp_lib_int_pow2) && __cpp_lib_int_pow2 >= 202002L
# define IGNITE_STD_INT_POW2 1
#endif

#if IGNITE_STD_BITOPS || IGNITE_STD_INT_POW2
# include <bit>
#endif

#if !IGNITE_STD_BITOPS || !IGNITE_STD_INT_POW2
# include <limits>
#endif

#if !IGNITE_STD_BITOPS && defined(_MSC_VER)
# include <intrin.h>
#endif

namespace ignite {

/**
 * Returns the number of consecutive 0 bits in the value of x, starting from the least significant bit ("right").
 */
template<typename T>
int countr_zero(T value) noexcept {
    static_assert(std::is_unsigned_v<T>, "countr_zero() doesn't support this type");
    static_assert(sizeof(T) <= sizeof(unsigned long long), "countr_zero() doesn't support this type size");

#if IGNITE_STD_BITOPS
    return std::countr_zero(value);
#elif defined(__GNUC__) || defined(__clang__)
    if (value == 0) {
        return std::numeric_limits<T>::digits;
    }

    if constexpr (sizeof(T) <= sizeof(unsigned int)) {
        return __builtin_ctz(value);
    } else if constexpr (sizeof(T) <= sizeof(unsigned long)) {
        return __builtin_ctzl(value);
    } else {
        return __builtin_ctzll(value);
    }
#elif defined(_MSC_VER)
    unsigned long index;
    if constexpr (sizeof(T) <= sizeof(unsigned long)) {
        return _BitScanForward(&index, value) ? index : std::numeric_limits<T>::digits;
    } else {
        return _BitScanForward64(&index, value) ? index : std::numeric_limits<T>::digits;
    }
#else
# error "TODO: implement countr_zero() for other compilers"
#endif
}

/**
 * Returns the number of consecutive 0 bits in the value of x, starting from the most significant bit ("left").
 */
template<typename T>
int countl_zero(T value) noexcept {
    static_assert(std::is_unsigned_v<T>, "countl_zero() doesn't support this type");
    static_assert(sizeof(T) <= sizeof(unsigned long long), "countl_zero() doesn't support this type size");

#if IGNITE_STD_BITOPS
    return std::countl_zero(value);
#elif defined(__GNUC__) || defined(__clang__)
    if (value == 0) {
        return std::numeric_limits<T>::digits;
    }

    if constexpr (sizeof(T) <= sizeof(unsigned int)) {
        constexpr auto extraBits = std::numeric_limits<unsigned int>::digits - std::numeric_limits<T>::digits;
        return __builtin_clz(value) - extraBits;
    } else if constexpr (sizeof(T) <= sizeof(unsigned long)) {
        constexpr auto extraBits = std::numeric_limits<unsigned long>::digits - std::numeric_limits<T>::digits;
        return __builtin_clzl(value) - extraBits;
    } else {
        constexpr auto extraBits = std::numeric_limits<unsigned long long>::digits - std::numeric_limits<T>::digits;
        return __builtin_clzll(value) - extraBits;
    }
#elif defined(_MSC_VER)
    unsigned long index;
    constexpr auto bit_size = std::numeric_limits<T>::digits;
    if constexpr (sizeof(T) <= sizeof(unsigned long)) {
        return _BitScanReverse(&index, value) ? bit_size - index - 1 : bit_size;
    } else {
        return _BitScanReverse64(&index, value) ? bit_size - index - 1 : bit_size;
    }
#else
# error "TODO: implement countl_zero() for other compilers"
#endif
}

/**
 * If x is not zero, calculates the number of bits needed to store the value x, that is, 1 + ⌊log2(x)⌋.
 * If x is zero, returns zero.
 */
template<typename T>
int bit_width(T value) noexcept {
    static_assert(std::is_unsigned_v<T>, "bit_width() doesn't support this type");

#if IGNITE_STD_INT_POW2
    return std::bit_width(value);
#else
    return std::numeric_limits<T>::digits - countl_zero(value);
#endif
}

/**
 * If value is not zero, calculates the largest integral power of two that is not greater than value.
 * If value is zero, returns zero.
 */
template<typename T>
T bit_floor(T value) noexcept {
    static_assert(std::is_unsigned_v<T>, "bit_floor() doesn't support this type");

#if IGNITE_STD_INT_POW2
    return std::bit_floor(value);
#else
    return value == 0 ? 0u : T(1u) << (bit_width(value) - 1);
#endif
}

/**
 * Calculates the smallest integral power of two that is not smaller than a given value.
 */
template<typename T>
T bit_ceil(T value) noexcept {
    static_assert(std::is_unsigned_v<T>, "bit_ceil() doesn't support this type");

#if IGNITE_STD_INT_POW2
    return std::bit_ceil(value);
#else
    // TODO: If bit_width(value - 1) is equal to the number of bits in T then this should
    // be UB e.g. reported by the UB sanitizer. But because of the integer promotion in
    // the expression below this might be hidden. A solution is actually provided in the
    // "Possible implementation" section here:
    // https://en.cppreference.com/w/cpp/numeric/bit_ceil
    // But do we really need this complication in our code? We can use somewhat relaxed
    // rules as compared to the standard library.
    return value <= 1u ? 1u : T(1u) << bit_width(T(value - 1u));
#endif
}

/**
 * Get a number of bytes needed to store a specified number of bits of information.
 *
 * @param bits_num Number of bits to store.
 * @return Required bytes number.
 */
[[nodiscard]] inline std::size_t bytes_for_bits(std::size_t bits_num) {
    return (bits_num + CHAR_BIT - 1) / CHAR_BIT;
}

/**
 * Get n-th power of ten.
 *
 * @param n Power. Should be in range [0, UINT64_MAX_PRECISION]
 * @return 10 pow n, if n is in range [0, UINT64_MAX_PRECISION].
 *     Otherwise, behaviour is undefined.
 */
inline std::uint64_t ten_power_u64(std::int32_t n) {
    static const std::array<uint64_t, UINT64_MAX_PRECISION> TEN_POWERS_TABLE{
        1U, // 0  / 10^0
        10U, // 1  / 10^1
        100U, // 2  / 10^2
        1000U, // 3  / 10^3
        10000U, // 4  / 10^4
        100000U, // 5  / 10^5
        1000000U, // 6  / 10^6
        10000000U, // 7  / 10^7
        100000000U, // 8  / 10^8
        1000000000U, // 9  / 10^9
        10000000000U, // 10 / 10^10
        100000000000U, // 11 / 10^11
        1000000000000U, // 12 / 10^12
        10000000000000U, // 13 / 10^13
        100000000000000U, // 14 / 10^14
        1000000000000000U, // 15 / 10^15
        10000000000000000U, // 16 / 10^16
        100000000000000000U, // 17 / 10^17
        1000000000000000000U, // 18 / 10^18
        10000000000000000000U // 19 / 10^19
    };

    assert(n >= 0 && n < UINT64_MAX_PRECISION);

    return TEN_POWERS_TABLE[n];
}

/**
 * Get the number of decimal digits of the integer value.
 *
 * @param value The value.
 * @return The number of decimal digits of the integer value.
 */
[[nodiscard]] inline std::int32_t digit_length(std::uint64_t value) {
    // See http://graphics.stanford.edu/~seander/bithacks.html
    // for the details on the algorithm.

    if (value < 10)
        return 1;

    std::int32_t r = ((64 - countl_zero(value) + 1) * 1233) >> 12;

    assert(r <= UINT64_MAX_PRECISION);

    return (r == UINT64_MAX_PRECISION || value < ten_power_u64(r)) ? r : r + 1;
}

} // namespace ignite
