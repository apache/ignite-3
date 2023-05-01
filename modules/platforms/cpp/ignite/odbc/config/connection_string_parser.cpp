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

#include "ignite/odbc/config/config_tools.h"
#include "ignite/odbc/config/connection_string_parser.h"
#include "ignite/odbc/nested_tx_mode.h"
#include "ignite/odbc/ssl_mode.h"
#include "ignite/odbc/utility.h"
#include "ignite/odbc/string_utils.h"

#include <vector>

namespace ignite {

const std::string connection_string_parser::key::driver{"driver"};
const std::string connection_string_parser::key::address{"address"};
const std::string connection_string_parser::key::page_size{"page_size"};

void connection_string_parser::parse_connection_string(const char* str, size_t len, char delimiter,
    diagnostic_record_storage* diag)
{
    std::string connect_str(str, len);

    while (connect_str.rbegin() != connect_str.rend() && *connect_str.rbegin() == 0)
        connect_str.erase(connect_str.size() - 1);

    while (!connect_str.empty())
    {
        size_t attr_begin = connect_str.rfind(delimiter);

        if (attr_begin == std::string::npos)
            attr_begin = 0;
        else
            ++attr_begin;

        size_t attr_eq_pos = connect_str.rfind('=');

        if (attr_eq_pos == std::string::npos)
            attr_eq_pos = 0;

        if (attr_begin < attr_eq_pos)
        {
            const char* key_begin = connect_str.data() + attr_begin;
            const char* key_end = connect_str.data() + attr_eq_pos;

            const char* value_begin = connect_str.data() + attr_eq_pos + 1;
            const char* value_end = connect_str.data() + connect_str.size();

            std::string key = strip_surrounding_whitespaces(key_begin, key_end);
            std::string value = strip_surrounding_whitespaces(value_begin, value_end);

            if (value[0] == '{' && value[value.size() - 1] == '}')
                value = value.substr(1, value.size() - 2);

            handle_attribute_pair(key, value, diag);
        }

        if (!attr_begin)
            break;

        connect_str.erase(attr_begin - 1);
    }
}

void connection_string_parser::parse_connection_string(const std::string& str, diagnostic_record_storage* diag)
{
    parse_connection_string(str.data(), str.size(), ';', diag);
}

void connection_string_parser::parse_config_attributes(const char* str, diagnostic_record_storage* diag)
{
    size_t len = 0;

    // Getting a list length. A list is terminated by two '\0'.
    while (str[len] || str[len + 1])
        ++len;

    ++len;

    parse_connection_string(str, len, '\0', diag);
}

void connection_string_parser::handle_attribute_pair(const std::string &key, const std::string &value,
    diagnostic_record_storage* diag)
{
    std::string lower_key = to_lower(key);

    if (lower_key == key::address)
    {
        std::vector<network::tcp_range> end_points;

        parse_address(value, end_points, diag);

        m_cfg.set_addresses(end_points);
    }
    else if (lower_key == key::page_size)
    {
        if (!all_digits(value))
        {
            if (diag)
            {
                diag->add_status_record(sql_state::S01S02_OPTION_VALUE_CHANGED,
                    make_error_message("Page size attribute value contains unexpected characters."
                        " Using default value.", key, value));
            }

            return;
        }

        if (value.size() >= sizeof("4294967295"))
        {
            if (diag)
            {
                diag->add_status_record(sql_state::S01S02_OPTION_VALUE_CHANGED,
                    make_error_message("Page size attribute value is too large. Using default value.", key, value));
            }

            return;
        }

        int64_t numValue = 0;
        std::stringstream conv;

        conv << value;
        conv >> numValue;

        if (numValue <= 0 || numValue > 0xFFFFFFFFL)
        {
            if (diag)
            {
                diag->add_status_record(sql_state::S01S02_OPTION_VALUE_CHANGED,
                    make_error_message("Page size attribute value is out of range.  Using default value.", key, value));
            }

            return;
        }

        m_cfg.set_page_size(static_cast<int32_t>(numValue));
    }
    else if (lower_key == key::driver)
    {
        m_cfg.set_driver(value);
    }
    else if (diag)
    {
        std::stringstream stream;

        stream << "Unknown attribute: '" << key << "'. Ignoring.";

        diag->add_status_record(sql_state::S01S02_OPTION_VALUE_CHANGED, stream.str());
    }
}

std::string connection_string_parser::make_error_message(const std::string& msg, const std::string& key,
    const std::string& value)
{
    std::stringstream stream;

    stream << msg << " [key='" << key << "', value='" << value << "']";

    return stream.str();
}

} // namespace ignite
