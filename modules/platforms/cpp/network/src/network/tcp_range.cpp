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

#include <algorithm>
#include <limits>
#include <charconv>

#include "ignite/network/tcp_range.h"

namespace
{

uint16_t parsePort(std::string_view value)
{
    // Stripping whitespaces
    size_t begin = value.find_first_not_of(' ');
    size_t end = value.find_last_not_of(' ');
    if (begin == std::string_view::npos || end == std::string_view::npos)
        return 0;

    std::string_view port = value.substr(begin, end - begin + 1);
    if (port.empty())
        return 0;

    // Checking that all chars are digits
    if (port.find_first_not_of("1234567890") != std::string_view::npos)
        return 0;

    // Checking that number is not too big
    if (port.size() >= sizeof("65535"))
        return 0;

    int intPort;
    auto res = std::from_chars(port.data(), port.data() + port.size(), intPort);
    if (res.ec != std::errc())
        return 0;

    // Checking that number is within TCP port range
    if (intPort <= 0 || intPort > 0xFFFF)
        return 0;

    return static_cast<uint16_t>(intPort);
}

}


namespace ignite::network
{

std::optional<TcpRange> TcpRange::parse(std::string_view str, uint16_t defPort) {
    TcpRange res;
    size_t colonNum = std::count(str.begin(), str.end(), ':');

    if (colonNum == 0)
    {
        res.host = str;
        res.port = defPort;
        res.range = 0;

        return {std::move(res)};
    }

    if (colonNum != 1)
        return std::nullopt;

    size_t colonPos = str.find(':');

    res.host = str.substr(0, colonPos);

    if (colonPos == str.size() - 1)
        return std::nullopt;

    std::string_view portRange = str.substr(colonPos + 1);

    size_t sepPos = portRange.find('.');
    if (sepPos == std::string::npos)
    {
        res.range = 0;
        res.port = parsePort(portRange);

        if (!res.port)
            return std::nullopt;
        return {std::move(res)};
    }

    if (sepPos + 2 > portRange.size() || portRange[sepPos + 1] != '.')
        return std::nullopt;

    uint16_t rangeBegin = parsePort(portRange.substr(0, sepPos));
    if (!rangeBegin)
        return std::nullopt;

    uint16_t rangeEnd = parsePort(portRange.substr(sepPos + 2));
    if (!rangeEnd)
        return std::nullopt;

    if (rangeEnd < rangeBegin)
        return std::nullopt;

    res.port = rangeBegin;
    res.range = rangeEnd - rangeBegin;

    return {std::move(res)};
}

int TcpRange::compare(const TcpRange &other) const {
    if (port < other.port)
        return -1;

    if (port > other.port)
        return 1;

    if (range < other.range)
        return -1;

    if (range > other.range)
        return 1;

    return host.compare(other.host);
}

} // namespace ignite::network
