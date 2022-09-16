/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
class Uuid {
public:
    /**
     * Default constructor.
     */
    constexpr Uuid() noexcept = default;

    /**
     * Constructor.
     *
     * @param most Most significant bits.
     * @param least Least significant bits.
     */
    constexpr Uuid(std::int64_t most, std::int64_t least) noexcept
        : most(most)
        , least(least) { }

    /**
     * Returns the most significant 64 bits of this instance.
     *
     * @return The most significant 64 bits of this instance.
     */
    constexpr std::int64_t getMostSignificantBits() const noexcept { return most; }

    /**
     * Returns the least significant 64 bits of this instance.
     *
     * @return The least significant 64 bits of this instance.
     */
    constexpr std::int64_t getLeastSignificantBits() const noexcept { return least; }

    /**
     * The version number associated with this instance.  The version
     * number describes how this Guid was generated.
     *
     * The version number has the following meaning:
     * 1    Time-based UUID;
     * 2    DCE security UUID;
     * 3    Name-based UUID;
     * 4    Randomly generated UUID.
     *
     * @return The version number of this instance.
     */
    constexpr std::int32_t version() const noexcept { return static_cast<std::int32_t>((most >> 12) & 0x0f); }

    /**
     * The variant number associated with this instance. The variant
     * number describes the layout of the Guid.
     *
     * The variant number has the following meaning:
     * 0    Reserved for NCS backward compatibility;
     * 2    IETF RFC 4122 (Leach-Salz), used by this class;
     * 6    Reserved, Microsoft Corporation backward compatibility;
     * 7    Reserved for future definition.
     *
     * @return The variant number of this instance.
     */
    constexpr std::int32_t variant() const noexcept {
        std::uint64_t least0 = static_cast<uint64_t>(least);
        return static_cast<std::int32_t>((least0 >> (64 - (least0 >> 62))) & (least >> 63));
    }

    /**
     * Compare to another value.
     *
     * @param other Instance to compare to.
     * @return Zero if equals, negative number if less, and positive if greater.
     */
    constexpr int compare(const Uuid &other) const noexcept {
        if (most != other.most) {
            return most < other.most ? -1 : 1;
        }
        if (least != other.least) {
            return least < other.least ? -1 : 1;
        }
        return 0;
    }

public:
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
constexpr bool operator==(const Uuid &lhs, const Uuid &rhs) noexcept {
    return lhs.compare(rhs) == 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is not equal to the second.
 */
constexpr bool operator!=(const Uuid &lhs, const Uuid &rhs) noexcept {
    return lhs.compare(rhs) != 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is less than the second.
 */
constexpr bool operator<(const Uuid &lhs, const Uuid &rhs) noexcept {
    return lhs.compare(rhs) < 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is less than or equal to the second.
 */
constexpr bool operator<=(const Uuid &lhs, const Uuid &rhs) noexcept {
    return lhs.compare(rhs) <= 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is greater than the second.
 */
constexpr bool operator>(const Uuid &lhs, const Uuid &rhs) noexcept {
    return lhs.compare(rhs) > 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is greater than or equal to the second.
 */
constexpr bool operator>=(const Uuid &lhs, const Uuid &rhs) noexcept {
    return lhs.compare(rhs) >= 0;
}

/**
 * @brief Output operator.
 *
 * @param os Output stream.
 * @param uuid Uuid to output.
 * @return Reference to the first param.
 */
template <typename C, typename T>
::std::basic_ostream<C, T> &operator<<(std::basic_ostream<C, T> &os, const Uuid &uuid) {
    uint32_t part1 = static_cast<uint32_t>(uuid.getMostSignificantBits() >> 32);
    uint16_t part2 = static_cast<uint16_t>(uuid.getMostSignificantBits() >> 16);
    uint16_t part3 = static_cast<uint16_t>(uuid.getMostSignificantBits());
    uint16_t part4 = static_cast<uint16_t>(uuid.getLeastSignificantBits() >> 48);
    uint64_t part5 = uuid.getLeastSignificantBits() & 0x0000FFFFFFFFFFFFU;

    std::ios_base::fmtflags savedFlags = os.flags();

    // clang-format off
    os  << std::hex 
        << std::setfill<C>('0') << std::setw(8)  << part1 << '-'
        << std::setfill<C>('0') << std::setw(4)  << part2 << '-'
        << std::setfill<C>('0') << std::setw(4)  << part3 << '-'
        << std::setfill<C>('0') << std::setw(4)  << part4 << '-'
        << std::setfill<C>('0') << std::setw(12) << part5;
    // clang-format on

    os.flags(savedFlags);

    return os;
}

/**
 * Input operator.
 *
 * @param is Input stream.
 * @param uuid Uuid to input.
 * @return Reference to the first param.
 */
template <typename C, typename T>
::std::basic_istream<C, T> &operator>>(std::basic_istream<C, T> &is, Uuid &uuid) {
    uint64_t parts[5];

    std::ios_base::fmtflags savedFlags = is.flags();

    is >> std::hex;

    for (int i = 0; i < 4; ++i) {
        C delim;

        is >> parts[i] >> delim;

        if (delim != static_cast<C>('-'))
            return is;
    }

    is >> parts[4];

    is.flags(savedFlags);

    uuid = Uuid((parts[0] << 32) | (parts[1] << 16) | parts[2], (parts[3] << 48) | parts[4]);

    return is;
}

} // namespace ignite
