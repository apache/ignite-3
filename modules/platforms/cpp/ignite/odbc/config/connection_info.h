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

#include "ignite/odbc/common_types.h"
#include "ignite/odbc/config/configuration.h"

#include <cstdint>
#include <map>

namespace ignite {

/**
 * Connection info.
 */
class connection_info {
public:
    /** Info type. */
    typedef unsigned short info_type;

    /**
     * Get formatted project version in a format XX.XX.XXXX (major.minor.patch).
     *
     * @return Formatted string version.
     */
    [[nodiscard]] static std::string get_formatted_project_version();

    /**
     * Convert type to string containing its name.
     * Debug function.
     * @param type Info type.
     * @return Null-terminated string containing types name.
     */
    [[nodiscard]] static const char *info_type_to_string(info_type type);

    /**
     * Constructor.
     *
     * @param config Configuration.
     */
    explicit connection_info(const configuration &config);

    // Default
    ~connection_info() = default;

    connection_info(connection_info &&) = delete;
    connection_info(const connection_info &) = delete;
    connection_info &operator=(connection_info &&) = delete;
    connection_info &operator=(const connection_info &) = delete;

    /**
     * Get info of any type.
     *
     * @param type Info type.
     * @param buf Result buffer pointer.
     * @param buffer_len Result buffer length.
     * @param result_len Result value length pointer.
     * @return True on success.
     */
    sql_result get_info(info_type type, void *buf, short buffer_len, short *result_len) const;

    /**
     * Set string info.
     *
     * @param type Info type.
     * @param value Info value.
     */
    void set_info(info_type type, std::string value);

    /**
     * Set int info.
     *
     * @param type Info type.
     * @param value Info value.
     */
    void set_info(info_type type, std::int32_t value);

    /**
     * Set short info.
     *
     * @param type Info type.
     * @param value Info value.
     */
    void set_info(info_type type, std::int16_t value);

    /**
     * Rebuild connection_info from config.
     */
    void rebuild();

private:
    /** Associative array of string parameters. */
    typedef std::map<info_type, std::string> string_info_map;

    /** Associative array of unsigned integer parameters. */
    typedef std::map<info_type, unsigned int> uint_info_map;

    /** Associative array of unsigned short parameters. */
    typedef std::map<info_type, unsigned short> ushort_info_map;

    /** String m_parameters. */
    string_info_map m_str_params;

    /** Integer m_parameters. */
    uint_info_map m_int_params;

    /** Short m_parameters. */
    ushort_info_map m_short_params;

    /** Configuration. */
    const configuration &config;
};

} // namespace ignite
