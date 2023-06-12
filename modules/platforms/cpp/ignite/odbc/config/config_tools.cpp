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

#include "ignite/odbc/string_utils.h"
#include "ignite/odbc/config/config_tools.h"
#include "ignite/odbc/config/configuration.h"

#include <ignite/common/utils.h>

#include <algorithm>
#include <sstream>

namespace {

using namespace ignite;

config_map::value_type parse_attribute_pair(std::string_view attribute) {
    auto res = split_once(attribute, '=');

    auto key = strip_surrounding_whitespaces(res.first);
    auto value = strip_surrounding_whitespaces(res.second);

    return config_map::value_type{key, value};
}

config_map parse_connection_string(std::string_view connect_str, char delimiter) {
    std::string_view parsed_str{connect_str};

    // Stripping zeroes from the end of the string
    while (!parsed_str.empty() && parsed_str.back() == '\0')
        parsed_str.remove_suffix(1);

    config_map res;
    for_every_delimited(parsed_str, delimiter, [&res](std::string_view attr_pair) {
        auto parsed_pair = parse_attribute_pair(attr_pair);
        if (!parsed_pair.first.empty())
            res.emplace(std::move(parsed_pair));
    });

    return res;
}

} // anonymous namespace

namespace ignite {

std::string addresses_to_string(const std::vector<end_point>& addresses)
{
    std::stringstream stream;

    auto it = addresses.begin();
    if (it != addresses.end())
    {
        stream << it->host << ':' << it->port;
        ++it;
    }

    for (; it != addresses.end(); ++it)
    {
        stream << ',' << it->host << ':' << it->port;
    }

    return stream.str();
}

std::vector<end_point> parse_address(std::string_view value, diagnostic_record_storage* diag)
{
    std::size_t addr_num = std::count(value.begin(), value.end(), ',') + 1;

    std::vector<end_point> end_points;
    end_points.reserve(addr_num);

    for_every_delimited(value, ',', [diag, &end_points](auto addr) {
        addr = strip_surrounding_whitespaces(addr);
        if (addr.empty())
            return;

        end_point ep;
        bool success = parse_single_address(addr, ep, diag);
        if (success)
            end_points.push_back(ep);
    });

    return end_points;
}

bool parse_single_address(std::string_view value, end_point &addr, diagnostic_record_storage* diag)
{
    std::int64_t colon_num = std::count(value.begin(), value.end(), ':');

    if (colon_num == 0)
    {
        addr.host = value;
        addr.port = configuration::default_value::port;

        return true;
    }

    if (colon_num != 1)
    {
        std::stringstream stream;

        stream << "Unexpected number of ':' characters in the following address: '"
            << value << "'. Ignoring address.";

        if (diag)
            diag->add_status_record(sql_state::S01S02_OPTION_VALUE_CHANGED, stream.str());

        return false;
    }

    std::size_t colon_pos = value.find(':');
    addr.host = value.substr(0, colon_pos);

    if (colon_pos == value.size() - 1)
    {
        std::stringstream stream;
        stream << "Port is missing in the following address: '" << value << "'. Ignoring address.";

        if (diag)
            diag->add_status_record(sql_state::S01S02_OPTION_VALUE_CHANGED, stream.str());

        return false;
    }

    auto port_str = value.substr(colon_pos + 1);
    addr.port = parse_port(port_str, diag);
    if (!addr.port)
        return false;

    return true;
}

std::uint16_t parse_port(std::string_view value, diagnostic_record_storage* diag)
{
    auto port_str = strip_surrounding_whitespaces(value);
    if (!std::all_of(port_str.begin(), port_str.end(), std::isdigit))
    {
        std::stringstream stream;
        stream << "Unexpected port characters: '" << port_str << "'. Ignoring address.";

        if (diag)
            diag->add_status_record(sql_state::S01S02_OPTION_VALUE_CHANGED, stream.str());

        return 0;
    }

    if (port_str.size() >= sizeof("65535"))
    {
        std::stringstream stream;
        stream << "Port value is too large: '" << port_str << "'. Ignoring address.";

        if (diag)
            diag->add_status_record(sql_state::S01S02_OPTION_VALUE_CHANGED, stream.str());

        return 0;
    }

    auto int_port = lexical_cast<std::int32_t>(port_str);
    if (int_port <= 0 || int_port > 0xFFFF)
    {
        std::stringstream stream;
        stream << "Port value is out of range: '" << port_str << "'. Ignoring address.";

        if (diag)
            diag->add_status_record(sql_state::S01S02_OPTION_VALUE_CHANGED, stream.str());

        return 0;
    }

    return static_cast<std::uint16_t>(int_port);
}

config_map parse_connection_string(std::string_view str) {
    return ::parse_connection_string(str, ';');
}

config_map parse_config_attributes(const char *str) {
    size_t len = 0;

    // Getting a list length. A list is terminated by two '\0'.
    while (str[len] || str[len + 1])
        ++len;

    return ::parse_connection_string({str, len}, '\0');
}

std::string normalize_argument_string(std::string_view value) {
    return to_lower(std::string{strip_surrounding_whitespaces(value)});
}

} // namespace ignite
