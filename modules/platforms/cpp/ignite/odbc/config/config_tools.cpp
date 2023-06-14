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

std::vector<end_point> parse_address(const std::string& value, diagnostic_record_storage* diag)
{
    std::size_t addr_num = std::count(value.begin(), value.end(), ',') + 1;

    std::vector<end_point> end_points;
    end_points.reserve(addr_num);

    std::string parsed_addr(value);

    while (!parsed_addr.empty())
    {
        std::size_t addr_begin_pos = parsed_addr.rfind(',');

        if (addr_begin_pos == std::string::npos)
            addr_begin_pos = 0;
        else
            ++addr_begin_pos;

        const char* addr_begin = parsed_addr.data() + addr_begin_pos;
        const char* addr_end = parsed_addr.data() + parsed_addr.size();

        std::string addr = strip_surrounding_whitespaces(addr_begin, addr_end);

        if (!addr.empty())
        {
            end_point ep;
            bool success = parse_single_address(addr, ep, diag);
            if (success)
                end_points.push_back(ep);
        }

        if (!addr_begin_pos)
            break;

        parsed_addr.erase(addr_begin_pos - 1);
    }

    return end_points;
}

bool parse_single_address(const std::string& value, end_point &addr, diagnostic_record_storage* diag)
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

    std::string port_str = value.substr(colon_pos + 1);
    addr.port = parse_port(port_str, diag);
    if (!addr.port)
        return false;

    return true;
}

std::uint16_t parse_port(const std::string& value, diagnostic_record_storage* diag)
{
    std::string port = strip_surrounding_whitespaces(value.begin(), value.end());
    if (!all_digits(port))
    {
        std::stringstream stream;
        stream << "Unexpected port characters: '" << port << "'. Ignoring address.";

        if (diag)
            diag->add_status_record(sql_state::S01S02_OPTION_VALUE_CHANGED, stream.str());

        return 0;
    }

    if (port.size() >= sizeof("65535"))
    {
        std::stringstream stream;
        stream << "Port value is too large: '" << port << "'. Ignoring address.";

        if (diag)
            diag->add_status_record(sql_state::S01S02_OPTION_VALUE_CHANGED, stream.str());

        return 0;
    }

    std::int32_t int_port = 0;
    std::stringstream conv;

    conv << port;
    conv >> int_port;

    if (int_port <= 0 || int_port > 0xFFFF)
    {
        std::stringstream stream;
        stream << "Port value is out of range: '" << port << "'. Ignoring address.";

        if (diag)
            diag->add_status_record(sql_state::S01S02_OPTION_VALUE_CHANGED, stream.str());

        return 0;
    }

    return static_cast<std::uint16_t>(int_port);
}

} // namespace ignite
