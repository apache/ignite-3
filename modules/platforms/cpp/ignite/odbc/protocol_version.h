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
#include <set>
#include <string>

namespace ignite {

/** Protocol version. */
class protocol_version {
public:
    /** Version 3.0.0. */
    static const protocol_version VERSION_3_0_0;

    /** Version set. */
    typedef std::set<protocol_version> version_set;

    /**
     * Get string to version map.
     *
     * @return String to version map.
     */
    static const version_set &get_supported();

    /**
     * Get current version.
     *
     * @return Current version.
     */
    static const protocol_version &get_current();

    /**
     * Parse string and extract protocol version.
     *
     * @throw IgniteException if version can not be parsed.
     * @param version Version string to parse.
     * @return Protocol version.
     */
    static protocol_version from_string(const std::string &version);

    /**
     * Convert to string value.
     *
     * @return Protocol version.
     */
    [[nodiscard]] std::string to_string() const;

    /**
     * Default constructor.
     */
    protocol_version() = default;

    /**
     * Constructor.
     *
     * @param vmajor Major version part.
     * @param vminor Minor version part.
     * @param vmaintenance Maintenance version part.
     */
    protocol_version(std::int16_t vmajor, std::int16_t vminor, std::int16_t vmaintenance)
        : m_major(vmajor)
        , m_minor(vminor)
        , m_maintenance(vmaintenance) {}

    /**
     * Get major part.
     *
     * @return Major part.
     */
    [[nodiscard]] std::int16_t get_major() const;

    /**
     * Get minor part.
     *
     * @return Minor part.
     */
    [[nodiscard]] std::int16_t get_minor() const;

    /**
     * Get maintenance part.
     *
     * @return Maintenance part.
     */
    [[nodiscard]] std::int16_t get_maintenance() const;

    /**
     * Check if the version is supported.
     *
     * @return True if the version is supported.
     */
    [[nodiscard]] bool is_supported() const;

    /**
     * compare to another value.
     *
     * @param other Instance to compare to.
     * @return Zero if equals, negative number if less and positive if more.
     */
    [[nodiscard]] std::int32_t compare(const protocol_version &other) const;

    /**
     * Comparison operator.
     *
     * @param val1 First value.
     * @param val2 Second value.
     * @return True if equal.
     */
    friend bool operator==(const protocol_version &val1, const protocol_version &val2);

    /**
     * Comparison operator.
     *
     * @param val1 First value.
     * @param val2 Second value.
     * @return True if not equal.
     */
    friend bool operator!=(const protocol_version &val1, const protocol_version &val2);

    /**
     * Comparison operator.
     *
     * @param val1 First value.
     * @param val2 Second value.
     * @return True if less.
     */
    friend bool operator<(const protocol_version &val1, const protocol_version &val2);

    /**
     * Comparison operator.
     *
     * @param val1 First value.
     * @param val2 Second value.
     * @return True if less or equal.
     */
    friend bool operator<=(const protocol_version &val1, const protocol_version &val2);

    /**
     * Comparison operator.
     *
     * @param val1 First value.
     * @param val2 Second value.
     * @return True if greater.
     */
    friend bool operator>(const protocol_version &val1, const protocol_version &val2);

    /**
     * Comparison operator.
     *
     * @param val1 First value.
     * @param val2 Second value.
     * @return True if greater or equal.
     */
    friend bool operator>=(const protocol_version &val1, const protocol_version &val2);

private:
    /** Set of supported versions. */
    const static version_set m_supported;

    /** Major part. */
    std::int16_t m_major{0};

    /** Minor part. */
    std::int16_t m_minor{0};

    /** Maintenance part. */
    std::int16_t m_maintenance{0};
};

} // namespace ignite
