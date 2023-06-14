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

#include <cstdint>
#include <iomanip>
#include <istream>
#include <ostream>

namespace ignite {

/**
 * @brief Universally unique identifier (UUID).
 *
 * Minimalistic implementation that mimics JDK java.util.UUID.
 */
class uuid {
public:
    /**
     * Default constructor.
     */
    constexpr uuid() noexcept = default;

    /**
     * Constructor.
     *
     * @param most Most significant bits.
     * @param least Least significant bits.
     */
    constexpr uuid(std::int64_t most, std::int64_t least) noexcept
        : most(most)
        , least(least) {}

    /**
     * Returns the most significant 64 bits of this instance.
     *
     * @return The most significant 64 bits of this instance.
     */
    [[nodiscard]] constexpr std::int64_t get_most_significant_bits() const noexcept { return most; }

    /**
     * Returns the least significant 64 bits of this instance.
     *
     * @return The least significant 64 bits of this instance.
     */
    [[nodiscard]] constexpr std::int64_t get_least_significant_bits() const noexcept { return least; }

    /**
     * The version number associated with this instance.  The version
     * number describes how this uuid was generated.
     *
     * The version number has the following meaning:
     * 1    Time-based UUID;
     * 2    DCE security UUID;
     * 3    Name-based UUID;
     * 4    Randomly generated UUID.
     *
     * @return The version number of this instance.
     */
    [[nodiscard]] constexpr std::int32_t version() const noexcept {
        return static_cast<std::int32_t>((most >> 12) & 0x0f);
    }

    /**
     * The variant number associated with this instance. The variant
     * number describes the layout of the uuid.
     *
     * The variant number has the following meaning:
     * 0    Reserved for NCS backward compatibility;
     * 2    IETF RFC 4122 (Leach-Salz), used by this class;
     * 6    Reserved, Microsoft Corporation backward compatibility;
     * 7    Reserved for future definition.
     *
     * @return The variant number of this instance.
     */
    [[nodiscard]] constexpr std::int32_t variant() const noexcept {
        auto least0 = static_cast<uint64_t>(least);
        return static_cast<std::int32_t>((least0 >> (64 - (least0 >> 62))) & (least >> 63));
    }

    /**
     * compare to another value.
     *
     * @param other Instance to compare to.
     * @return Zero if equals, negative number if less, and positive if greater.
     */
    [[nodiscard]] constexpr int compare(const uuid &other) const noexcept {
        if (most != other.most) {
            return most < other.most ? -1 : 1;
        }
        if (least != other.least) {
            return least < other.least ? -1 : 1;
        }
        return 0;
    }

private:
    /** Most significant bits. */
    std::int64_t most = 0;

    /** Least significant bits. */
    std::int64_t least = 0;
};

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is equal to the second.
 */
constexpr bool operator==(const uuid &lhs, const uuid &rhs) noexcept {
    return lhs.compare(rhs) == 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is not equal to the second.
 */
constexpr bool operator!=(const uuid &lhs, const uuid &rhs) noexcept {
    return lhs.compare(rhs) != 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is less than the second.
 */
constexpr bool operator<(const uuid &lhs, const uuid &rhs) noexcept {
    return lhs.compare(rhs) < 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is less than or equal to the second.
 */
constexpr bool operator<=(const uuid &lhs, const uuid &rhs) noexcept {
    return lhs.compare(rhs) <= 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is greater than the second.
 */
constexpr bool operator>(const uuid &lhs, const uuid &rhs) noexcept {
    return lhs.compare(rhs) > 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is greater than or equal to the second.
 */
constexpr bool operator>=(const uuid &lhs, const uuid &rhs) noexcept {
    return lhs.compare(rhs) >= 0;
}

/**
 * @brief Output operator.
 *
 * @param os Output stream.
 * @param uuid Uuid to output.
 * @return Reference to the first param.
 */
template<typename C, typename T>
::std::basic_ostream<C, T> &operator<<(std::basic_ostream<C, T> &os, const uuid &uuid) {
    auto msb = uuid.get_most_significant_bits();
    auto lsb = uuid.get_least_significant_bits();

    auto part1 = static_cast<std::uint32_t>(msb >> 32);
    auto part2 = static_cast<std::uint16_t>(msb >> 16);
    auto part3 = static_cast<std::uint16_t>(msb);
    auto part4 = static_cast<std::uint16_t>(lsb >> 48);
    uint64_t part5 = lsb & 0x0000FFFFFFFFFFFFU;

    std::ios_base::fmtflags saved_flags = os.flags();

    // clang-format off
    os  << std::hex 
        << std::setfill<C>('0') << std::setw(8)  << part1 << '-'
        << std::setfill<C>('0') << std::setw(4)  << part2 << '-'
        << std::setfill<C>('0') << std::setw(4)  << part3 << '-'
        << std::setfill<C>('0') << std::setw(4)  << part4 << '-'
        << std::setfill<C>('0') << std::setw(12) << part5;
    // clang-format on

    os.flags(saved_flags);

    return os;
}

/**
 * Input operator.
 *
 * @param is Input stream.
 * @param uuid Uuid to input.
 * @return Reference to the first param.
 */
template<typename C, typename T>
::std::basic_istream<C, T> &operator>>(std::basic_istream<C, T> &is, uuid &result) {
    std::uint64_t parts[5];

    std::ios_base::fmtflags saved_flags = is.flags();

    is >> std::hex;

    for (int i = 0; i < 4; ++i) {
        C delim;

        is >> parts[i] >> delim;

        if (delim != static_cast<C>('-')) {
            return is;
        }
    }

    is >> parts[4];

    is.flags(saved_flags);

    result =
        uuid(std::int64_t((parts[0] << 32) | (parts[1] << 16) | parts[2]), std::int64_t((parts[3] << 48) | parts[4]));

    return is;
}

} // namespace ignite
