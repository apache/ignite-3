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
#include "ignite/odbc/config/configuration.h"
#include "ignite/odbc/odbc_error.h"
#include "ignite/odbc/string_utils.h"

#include <algorithm>
#include <sstream>

namespace {

using namespace ignite;

config_map::value_type parse_attribute_pair(std::string_view attribute) {
    auto res = split_once(attribute, '=');

    auto key = normalize_argument_string(res.first);
    auto value = trim(res.second);

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

std::string addresses_to_string(const std::vector<end_point> &addresses) {
    std::stringstream stream;

    auto it = addresses.begin();
    if (it != addresses.end()) {
        stream << it->host << ':' << it->port;
        ++it;
    }

    for (; it != addresses.end(); ++it) {
        stream << ',' << it->host << ':' << it->port;
    }

    return stream.str();
}

std::vector<end_point> parse_address(std::string_view value) {
    std::size_t addr_num = std::count(value.begin(), value.end(), ',') + 1;

    std::vector<end_point> end_points;
    end_points.reserve(addr_num);

    for_every_delimited(value, ',', [&end_points](auto addr) {
        addr = trim(addr);
        if (addr.empty())
            return;

        end_points.emplace_back(parse_single_address(addr));
    });

    return end_points;
}

end_point parse_single_address(std::string_view value) {
    auto colon_num = std::count(value.begin(), value.end(), ':');

    if (colon_num == 0)
        return {std::string(value), configuration::default_value::port};

    if (colon_num != 1) {
        throw odbc_error(sql_state::S01S00_INVALID_CONNECTION_STRING_ATTRIBUTE,
            "Unexpected number of ':' characters in the following address: '" + std::string(value));
    }

    auto colon_pos = value.find(':');
    auto host = value.substr(0, colon_pos);

    if (colon_pos == value.size() - 1) {
        throw odbc_error(sql_state::S01S00_INVALID_CONNECTION_STRING_ATTRIBUTE,
            "Port is missing in the following address: '" + std::string(value));
    }

    auto port_str = value.substr(colon_pos + 1);
    auto port = parse_port(port_str);

    return {std::string(host), port};
}

std::optional<std::int64_t> parse_int64(std::string_view value) {
    auto value_str = trim(value);
    if (!std::all_of(value_str.begin(), value_str.end(), [](char c) { return std::isdigit(c) || c == '-'; }))
        return std::nullopt;
    return lexical_cast<std::int64_t>(value_str);
}

std::uint16_t parse_port(std::string_view value) {
    auto port_opt = parse_int<std::uint16_t>(value);
    if (!port_opt || *port_opt == 0) {
        throw odbc_error(
            sql_state::S01S00_INVALID_CONNECTION_STRING_ATTRIBUTE, "Invalid port value: " + std::string(value));
    }
    return *port_opt;
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
    return to_lower(std::string{trim(value)});
}

} // namespace ignite
