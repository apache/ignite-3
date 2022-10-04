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

/**
 * @file
 * Declares ignite::Guid class.
 */

#pragma once

#include <cstdint>
#include <iomanip>

namespace ignite {

/**
 * Global universally unique identifier (GUID).
 */
class Guid {
public:
    // Default
    Guid() = default;

    /**
     * Constructor.
     *
     * @param most Most significant bits.
     * @param least Least significant bits.
     */
    Guid(int64_t most, int64_t least)
        : m_most(most)
        , m_least(least) { }

    /**
     * Returns the most significant 64 bits of this instance.
     *
     * @return The most significant 64 bits of this instance.
     */
    [[nodiscard]] std::int64_t getMostSignificantBits() const { return m_most; }

    /**
     * Returns the least significant 64 bits of this instance.
     *
     * @return The least significant 64 bits of this instance.
     */
    [[nodiscard]] std::int64_t getLeastSignificantBits() const { return m_least; }

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
    [[nodiscard]] std::int32_t getVersion() const { return static_cast<int32_t>((m_most >> 12) & 0x0F); }

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
    [[nodiscard]] std::int32_t getVariant() const {
        auto least = static_cast<uint64_t>(m_least);
        return static_cast<std::int32_t>((least >> (64 - (least >> 62))) & (least >> 63));
    }

    /**
     * Compare to another value.
     *
     * @param other Instance to compare to.
     * @return Zero if equals, negative number if less and positive if more.
     */
    [[nodiscard]] std::int64_t compare(const Guid &other) const {
        if (m_most < other.m_most)
            return -1;

        if (m_most > other.m_most)
            return 1;

        if (m_least < other.m_least)
            return -1;

        if (m_least > other.m_least)
            return 1;

        return 0;
    }

private:
    /** Most significant bits. */
    std::int64_t m_most{0};

    /** Least significant bits. */
    std::int64_t m_least{0};
};

/**
 * Comparison operator.
 *
 * @param val1 First value.
 * @param val2 Second value.
 * @return True if equal.
 */
inline bool operator==(const Guid &val1, const Guid &val2) {
    return val1.compare(val2) == 0;
}

/**
 * Comparison operator.
 *
 * @param val1 First value.
 * @param val2 Second value.
 * @return True if not equal.
 */
inline bool operator!=(const Guid &val1, const Guid &val2) {
    return val1.compare(val2) != 0;
}

/**
 * Comparison operator.
 *
 * @param val1 First value.
 * @param val2 Second value.
 * @return True if less.
 */
inline bool operator<(const Guid &val1, const Guid &val2) {
    return val1.compare(val2) < 0;
}

/**
 * Comparison operator.
 *
 * @param val1 First value.
 * @param val2 Second value.
 * @return True if less or equal.
 */
inline bool operator<=(const Guid &val1, const Guid &val2) {
    return val1.compare(val2) <= 0;
}

/**
 * Comparison operator.
 *
 * @param val1 First value.
 * @param val2 Second value.
 * @return True if greater.
 */
inline bool operator>(const Guid &val1, const Guid &val2) {
    return val1.compare(val2) > 0;
}

/**
 * Comparison operator.
 *
 * @param val1 First value.
 * @param val2 Second value.
 * @return True if greater or equal.
 */
inline bool operator>=(const Guid &val1, const Guid &val2) {
    return val1.compare(val2) >= 0;
}

/**
 * Output operator.
 *
 * @param os Output stream.
 * @param guid Guid to output.
 * @return Reference to the first param.
 */
template <typename C>
::std::basic_ostream<C> &operator<<(std::basic_ostream<C> &os, const Guid &guid) {
    auto part1 = uint32_t(guid.getMostSignificantBits() >> 32);
    auto part2 = uint16_t(guid.getMostSignificantBits() >> 16);
    auto part3 = uint16_t(guid.getMostSignificantBits());
    auto part4 = uint16_t(guid.getLeastSignificantBits() >> 48);
    uint64_t part5 = guid.getLeastSignificantBits() & 0x0000FFFFFFFFFFFFU;

    // clang-format off
    os  << std::hex
        << std::setfill<C>('0') << std::setw(8)  << part1 << '-'
        << std::setfill<C>('0') << std::setw(4)  << part2 << '-'
        << std::setfill<C>('0') << std::setw(4)  << part3 << '-'
        << std::setfill<C>('0') << std::setw(4)  << part4 << '-'
        << std::setfill<C>('0') << std::setw(12) << part5 << std::dec;
    // clang-format on

    return os;
}

/**
 * Input operator.
 *
 * @param is Input stream.
 * @param guid Guid to input.
 * @return Reference to the first param.
 */
template <typename C>
::std::basic_istream<C> &operator>>(std::basic_istream<C> &is, Guid &guid) {
    uint64_t parts[5];

    C delim;
    for (int i = 0; i < 4; ++i) {
        is >> std::hex >> parts[i] >> delim;
        if (delim != static_cast<C>('-'))
            return is;
    }

    is >> std::hex >> parts[4];
    guid = Guid(int64_t((parts[0] << 32) | (parts[1] << 16) | parts[2]), int64_t((parts[3] << 48) | parts[4]));
    return is;
}

} // namespace ignite
