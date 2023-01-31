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
#include <sstream>
#include <string>

namespace ignite::detail {

/** Protocol version. */
class protocol_version {
public:
    /**
     * Constructor.
     *
     * @param vmajor Major version part.
     * @param vminor Minor version part.
     * @param vpatch Patch version part.
     */
    constexpr protocol_version(int16_t vmajor, int16_t vminor, int16_t vpatch)
        : m_major(vmajor)
        , m_minor(vminor)
        , m_patch(vpatch) {}

    /**
     * Get major part.
     *
     * @return Major part.
     */
    [[nodiscard]] int16_t major() const { return m_major; }

    /**
     * Get minor part.
     *
     * @return Minor part.
     */
    [[nodiscard]] int16_t minor() const { return m_minor; }

    /**
     * Get patch version part.
     *
     * @return Patch version part.
     */
    [[nodiscard]] int16_t patch() const { return m_patch; }

    /**
     * Compare to another value.
     *
     * @param other Instance to compare to.
     * @return Zero if equals, negative number if less and positive if more.
     */
    [[nodiscard]] int32_t compare(const protocol_version &other) const {
        int32_t res = m_major - other.m_major;
        if (res != 0)
            return res;

        res = m_minor - other.m_minor;
        if (res != 0)
            return res;

        return m_patch - other.m_patch;
    }

    /**
     * Convert to string value.
     *
     * @return Protocol version.
     */
    [[nodiscard]] std::string to_string() const {
        std::stringstream buf;
        buf << m_major << '.' << m_minor << '.' << m_patch;

        return buf.str();
    }

private:
    /** Major part. */
    int16_t m_major;

    /** Minor part. */
    int16_t m_minor;

    /** Maintenance part. */
    int16_t m_patch;
};

/**
 * Comparison operator.
 *
 * @param val1 First value.
 * @param val2 Second value.
 * @return True if equal.
 */
inline bool operator==(const protocol_version &val1, const protocol_version &val2) {
    return val1.compare(val2) == 0;
}

/**
 * Comparison operator.
 *
 * @param val1 First value.
 * @param val2 Second value.
 * @return True if not equal.
 */
inline bool operator!=(const protocol_version &val1, const protocol_version &val2) {
    return val1.compare(val2) != 0;
}

/**
 * Comparison operator.
 *
 * @param val1 First value.
 * @param val2 Second value.
 * @return True if less.
 */
inline bool operator<(const protocol_version &val1, const protocol_version &val2) {
    return val1.compare(val2) < 0;
}

/**
 * Comparison operator.
 *
 * @param val1 First value.
 * @param val2 Second value.
 * @return True if less or equal.
 */
inline bool operator<=(const protocol_version &val1, const protocol_version &val2) {
    return val1.compare(val2) <= 0;
}

/**
 * Comparison operator.
 *
 * @param val1 First value.
 * @param val2 Second value.
 * @return True if greater.
 */
inline bool operator>(const protocol_version &val1, const protocol_version &val2) {
    return val1.compare(val2) > 0;
}

/**
 * Comparison operator.
 *
 * @param val1 First value.
 * @param val2 Second value.
 * @return True if greater or equal.
 */
inline bool operator>=(const protocol_version &val1, const protocol_version &val2) {
    return val1.compare(val2) >= 0;
}

} // namespace ignite::detail
