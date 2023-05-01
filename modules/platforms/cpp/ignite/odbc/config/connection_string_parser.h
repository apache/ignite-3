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

#include "ignite/odbc/config/configuration.h"
#include "ignite/odbc/diagnostic/diagnostic_record_storage.h"

#include <string>

namespace ignite
{

/**
 * ODBC configuration parser abstraction.
 */
class connection_string_parser
{
public:
    /** Connection attribute keywords. */
    struct key
    {
        /** Connection attribute keyword for Driver attribute. */
        static const std::string driver;

        /** Connection attribute keyword for address attribute. */
        static const std::string address;

        /** Connection attribute keyword for fetch results page size attribute. */
        static const std::string page_size;
    };

    /**
     * Constructor.
     *
     * @param cfg Configuration.
     */
    explicit connection_string_parser(configuration& cfg) : m_cfg(cfg) {}

    /**
     * Parse connect string.
     *
     * @param str String to parse.
     * @param len String length.
     * @param delimiter delimiter.
     * @param diag Diagnostics collector.
     */
    void parse_connection_string(const char* str, size_t len, char delimiter, diagnostic_record_storage* diag);

    /**
     * Parse connect string.
     *
     * @param str String to parse.
     * @param diag Diagnostics collector.
     */
    void parse_connection_string(const std::string& str, diagnostic_record_storage* diag);

    /**
     * Parse config attributes.
     *
     * @param str String to parse.
     * @param diag Diagnostics collector.
     */
    void parse_config_attributes(const char* str, diagnostic_record_storage* diag);

private:
    /**
     * Handle new attribute pair callback.
     *
     * @param key Key.
     * @param value Value.
     * @param diag Diagnostics collector.
     */
    void handle_attribute_pair(const std::string& key, const std::string& value, diagnostic_record_storage* diag);

    /**
     * Convert string to boolean value.
     *
     * @param msg Error message.
     * @param key Key.
     * @param value Value.
     * @return Resulting error message.
     */
    static std::string make_error_message(const std::string& msg, const std::string& key, const std::string& value);

    /** Configuration. */
    configuration& m_cfg;
};

} // namespace ignite
