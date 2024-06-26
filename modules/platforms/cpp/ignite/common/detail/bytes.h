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

#include "detail/config.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

#if defined(__cpp_lib_endian) && __cpp_lib_endian >= 201907L
# define IGNITE_STD_ENDIAN 1
#endif
#if defined(__cpp_lib_byteswap) && __cpp_lib_byteswap >= 202110L
# define IGNITE_STD_BYTESWAP 1
#endif

#if IGNITE_STD_ENDIAN || IGNITE_STD_BYTESWAP
# include <bit>
#endif

#if !IGNITE_STD_ENDIAN
# if defined(__BYTE_ORDER__) && defined(__ORDER_BIG_ENDIAN__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#  define IGNITE_NATIVE_BYTE_ORDER BIG
# elif defined(__BYTE_ORDER__) && defined(__ORDER_LITTLE_ENDIAN__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#  define IGNITE_NATIVE_BYTE_ORDER LITTLE
# elif defined(_WIN32)
#  define IGNITE_NATIVE_BYTE_ORDER LITTLE
# else
#  error "Unknown endianess"
# endif
#endif

#if !IGNITE_STD_BYTESWAP
# if defined(_MSC_VER)
#  include <cstdlib>
#  define IGNITE_BYTESWAP_16(x) _byteswap_ushort(x)
#  define IGNITE_BYTESWAP_32(x) _byteswap_ulong(x)
#  define IGNITE_BYTESWAP_64(x) _byteswap_uint64(x)
#  define IGNITE_BYTESWAP_SPECIFIER
# elif defined(__GNUC__) || defined(__clang__)
// Some rather old gcc and clang versions do not provide the required builtin functions. But we already
// require C++17 support and we can safely assume that if a compiler is modern enough for this standard
// it is also modern enough for these builtins.
#  define IGNITE_BYTESWAP_16(x) __builtin_bswap16(x)
#  define IGNITE_BYTESWAP_32(x) __builtin_bswap32(x)
#  define IGNITE_BYTESWAP_64(x) __builtin_bswap64(x)
# else
#  define IGNITE_BYTESWAP_16(x) ignite::bytes::swap16(x)
#  define IGNITE_BYTESWAP_32(x) ignite::bytes::swap32(x)
#  define IGNITE_BYTESWAP_64(x) ignite::bytes::swap64(x)
# endif
#endif

#ifndef IGNITE_BYTESWAP_SPECIFIER
# define IGNITE_BYTESWAP_SPECIFIER constexpr
#endif

namespace ignite::detail {

/**
 * Byte order enum.
 */
enum class endian {
#if IGNITE_STD_ENDIAN
    LITTLE = std::endian::little,
    BIG = std::endian::big,
    NATIVE = std::endian::native,
#else
    LITTLE,
    BIG,
    NATIVE = IGNITE_NATIVE_BYTE_ORDER,
#endif
};

/**
 * @brief Returns true if the host platform has big-endian byte order.
 *
 * @return true If the host platform has big-endian byte order.
 * @return false If the host platform has other byte order.
 */
constexpr bool is_big_endian_platform() noexcept {
    return endian::NATIVE == endian::BIG;
}

/**
 * @brief Returns true if the host platform has little-endian byte order.
 *
 * @return true If the host platform has little-endian byte order.
 * @return false If the host platform has other byte order.
 */
constexpr bool is_little_endian_platform() noexcept {
    return endian::NATIVE == endian::LITTLE;
}

// Namespace for byte utilities
namespace bytes {

/**
 * @brief Reverses bytes in a 2-byte unsigned integer.
 *
 * This is a generic function that does not use compiler intrinsics.
 *
 * @param value Original integer value.
 * @return Value with reversed bytes.
 */
constexpr std::uint16_t swap16(std::uint16_t value) noexcept {
    return (value << 8) | (value >> 8);
}

/**
 * @brief Reverses bytes in a 4-byte unsigned integer.
 *
 * This is a generic function that does not use compiler intrinsics.
 *
 * @param value Original integer value.
 * @return Value with reversed bytes.
 */
constexpr std::uint32_t swap32(std::uint32_t value) noexcept {
    return (value << 24) | (value >> 24) | ((value << 8) & 0x00ff0000) | ((value >> 8) & 0x0000ff00);
}

/**
 * @brief Reverses bytes in a 8-byte unsigned integer.
 *
 * This is a generic function that does not use compiler intrinsics.
 *
 * @param value Original integer value.
 * @return Value with reversed bytes.
 */
constexpr std::uint64_t swap64(std::uint64_t value) noexcept {
    return (value << 56) | (value >> 56) | ((value << 40) & 0x00ff000000000000) | ((value >> 40) & 0x000000000000ff00)
        | ((value << 24) & 0x0000ff0000000000) | ((value >> 24) & 0x0000000000ff0000)
        | ((value << 8) & 0x000000ff00000000) | ((value >> 8) & 0x00000000ff000000);
}

/**
 * @brief Reinterpret a value of one type as a value of another type avoiding aliasing problems of reinterpret_cast.
 *
 * This is basically a C++17-compatible version of C++20 std::bit_cast but w/o constexpr that is not that much needed
 * here. Typically the whole operation is optimized out by the compiler and becomes a no-op.
 *
 * @tparam T Required type.
 * @tparam S Original type.
 * @param src Original value.
 * @return Value reinterpreted as having the required type.
 */
template<typename T, typename S>
T cast(const S &src) noexcept {
    static_assert(sizeof(T) == sizeof(S) && std::is_trivially_copyable_v<T> && std::is_trivially_copyable_v<S>,
        "Unsuitable types for casting");

    T dst;
    std::memcpy(&dst, &src, sizeof(T));
    return dst;
}

namespace detail {

// A helper to implement byte swapping.
template<std::size_t Size>
struct swapper;

// Specialization for 1-byte types.
template<>
struct swapper<1> {
    using type = std::uint8_t;

    static IGNITE_BYTESWAP_SPECIFIER type swap(type value) noexcept { return value; }
};

// Specialization for 2-byte types.
template<>
struct swapper<2> {
    using type = std::uint16_t;

    static IGNITE_BYTESWAP_SPECIFIER type swap(type value) noexcept { return IGNITE_BYTESWAP_16(value); }
};

// Specialization for 4-byte types.
template<>
struct swapper<4> {
    using type = std::uint32_t;

    static IGNITE_BYTESWAP_SPECIFIER type swap(type value) noexcept { return IGNITE_BYTESWAP_32(value); }
};

// Specialization for 8-byte types.
template<>
struct swapper<8> {
    using type = std::uint64_t;

    static IGNITE_BYTESWAP_SPECIFIER type swap(type value) noexcept { return IGNITE_BYTESWAP_64(value); }
};

// A type suitable for swapping bytes.
template<typename T>
using swap_type = typename swapper<sizeof(T)>::type;

} // namespace detail

/**
 * @brief Reverses bytes in an integer value.
 *
 * This function uses compiler intrinsics if possible.
 *
 * @tparam T Integer type.
 * @param value Original integer value.
 * @return Value with reversed bytes.
 */
template<typename T>
IGNITE_BYTESWAP_SPECIFIER T reverse(T value) noexcept {
    static_assert(std::is_integral_v<T>, "reverse() is unimplemented for this type");

    // Although std::byteswap() can potentially support other sizes we would like to keep
    // different implementations in sync to have identical diagnostics on all compilers.
    static_assert(sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8,
        "reverse() is unimplemented for types of this size");

#if IGNITE_STD_BYTESWAP
    return std::byteswap(value);
#else
    return detail::swapper<sizeof(T)>::swap(value);
#endif
}

/**
 * @brief Convert a value from one byte order to another.
 *
 * @tparam T Integer type.
 * @tparam X Original byte-order.
 * @tparam Y Required byte-order.
 * @param value Original value.
 * @return Converted value.
 */
template<typename T, endian X, endian Y>
static T adjust_order(T value) noexcept {
    if constexpr (X == Y) {
        return value;
    } else {
        static_assert((X == endian::LITTLE && Y == endian::BIG) || (X == endian::BIG && Y == endian::LITTLE),
            "unimplemented byte order conversion");
        return reverse(value);
    }
}

/**
 * @brief Convert a value from little-endian to big-endian byte order.
 *
 * @tparam T Integer type.
 * @param value Original value.
 * @return Converted value.
 */
template<typename T>
static T ltob(T value) noexcept {
    return adjust_order<T, endian::LITTLE, endian::BIG>(value);
}

/**
 * @brief Convert a value from little-endian to native (host) byte order.
 *
 * @tparam T Integer type.
 * @param value Original value.
 * @return Converted value.
 */
template<typename T>
static T ltoh(T value) noexcept {
    return adjust_order<T, endian::LITTLE, endian::NATIVE>(value);
}

/**
 * @brief Convert a value from native (host) to little-endian byte order.
 *
 * @tparam T Integer type.
 * @param value Original value.
 * @return Converted value.
 */
template<typename T>
static T htol(T value) noexcept {
    return adjust_order<T, endian::NATIVE, endian::LITTLE>(value);
}

/**
 * @brief Convert a value from big-endian to little-endian byte order.
 *
 * @tparam T Integer type.
 * @param value Original value.
 * @return Converted value.
 */
template<typename T>
static T btol(T value) noexcept {
    return adjust_order<T, endian::BIG, endian::LITTLE>(value);
}

/**
 * @brief Convert a value from big-endian to native (host) byte order.
 *
 * @tparam T Integer type.
 * @param value Original value.
 * @return Converted value.
 */
template<typename T>
static T btoh(T value) noexcept {
    return adjust_order<T, endian::BIG, endian::NATIVE>(value);
}

/**
 * @brief Convert a value from native (host) to little-endian byte order.
 *
 * @tparam T Integer type.
 * @param value Original value.
 * @return Converted value.
 */
template<typename T>
static T htob(T value) noexcept {
    return adjust_order<T, endian::NATIVE, endian::BIG>(value);
}

/**
 * @brief Load a value from a byte sequence.
 *
 * @tparam T Value type.
 * @param bytes Pointer to byte storage.
 * @return Loaded value.
 */
template<typename T>
T load_raw(const std::byte *bytes) noexcept {
    static_assert(std::is_trivially_copyable_v<T>, "Unsuitable type for byte copying");

    T value;
    std::memcpy(&value, bytes, sizeof(T));
    return value;
}

/**
 * @brief Store a value as a sequence of bytes.
 *
 * @tparam T Value type.
 * @param bytes Pointer to byte storage.
 * @param value Value to store.
 */
template<typename T>
void store_raw(std::byte *bytes, T value) noexcept {
    static_assert(std::is_trivially_copyable_v<T>, "Unsuitable type for byte copying");

    std::memcpy(bytes, &value, sizeof(T));
}

/**
 * @brief Load a value from a byte sequence with byte-order adjustment.
 *
 * @tparam E Byte order.
 * @tparam T Value type.
 * @param bytes Pointer to byte storage.
 * @return Loaded value.
 */
template<endian E, typename T>
T load(const std::byte *bytes) noexcept {
    if constexpr (E == endian::NATIVE) {
        return load_raw<T>(bytes);
    } else if constexpr (std::is_integral_v<T>) {
        return adjust_order<T, E, endian::NATIVE>(load_raw<T>(bytes));
    } else {
        using S = detail::swap_type<T>;
        return cast<T>(adjust_order<S, E, endian::NATIVE>(load_raw<S>(bytes)));
    }
}

/**
 * @brief Store a value as a sequence of bytes with byte-order adjustment.
 *
 * @tparam E Byte order.
 * @tparam T Value type.
 * @param bytes Pointer to byte storage.
 * @param value Value to store.
 */
template<endian E, typename T>
void store(std::byte *bytes, T value) noexcept {
    if constexpr (E == endian::NATIVE) {
        store_raw(bytes, value);
    } else if constexpr (std::is_integral_v<T>) {
        store_raw(bytes, adjust_order<T, endian::NATIVE, E>(value));
    } else {
        using U = detail::swap_type<T>;
        store_raw(bytes, adjust_order<U, endian::NATIVE, E>(cast<U>(value)));
    }
}

} // namespace bytes
} // namespace ignite::detail
