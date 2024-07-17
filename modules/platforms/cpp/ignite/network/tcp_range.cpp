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

#include "tcp_range.h"

#include <algorithm>
#include <charconv>
#include <limits>

namespace {

uint16_t parsePort(std::string_view value) {
    // Stripping whitespaces
    size_t begin = value.find_first_not_of(' ');
    size_t end = value.find_last_not_of(' ');
    if (begin == std::string_view::npos || end == std::string_view::npos)
        return 0;

    std::string_view port_str = value.substr(begin, end - begin + 1);
    if (port_str.empty())
        return 0;

    // Checking that all chars are digits
    if (port_str.find_first_not_of("1234567890") != std::string_view::npos)
        return 0;

    // Checking that number is not too big
    if (port_str.size() >= sizeof("65535"))
        return 0;

    int port;
    auto res = std::from_chars(port_str.data(), port_str.data() + port_str.size(), port);
    if (res.ec != std::errc())
        return 0;

    // Checking that number is within TCP port range
    if (port <= 0 || port > 0xFFFF)
        return 0;

    return static_cast<uint16_t>(port);
}

} // namespace

namespace ignite::network {

std::optional<tcp_range> tcp_range::parse(std::string_view str, uint16_t def_port) {
    tcp_range res;
    size_t colon_num = std::count(str.begin(), str.end(), ':');

    if (colon_num == 0) {
        res.host = str;
        res.port = def_port;
        res.range = 0;

        return {std::move(res)};
    }

    if (colon_num != 1)
        return std::nullopt;

    size_t colon_pos = str.find(':');

    res.host = str.substr(0, colon_pos);

    if (colon_pos == str.size() - 1)
        return std::nullopt;

    std::string_view port_range = str.substr(colon_pos + 1);

    size_t sep_pos = port_range.find('.');
    if (sep_pos == std::string::npos) {
        res.range = 0;
        res.port = parsePort(port_range);

        if (!res.port)
            return std::nullopt;
        return {std::move(res)};
    }

    if (sep_pos + 2 > port_range.size() || port_range[sep_pos + 1] != '.')
        return std::nullopt;

    uint16_t range_begin = parsePort(port_range.substr(0, sep_pos));
    if (!range_begin)
        return std::nullopt;

    uint16_t range_end = parsePort(port_range.substr(sep_pos + 2));
    if (!range_end)
        return std::nullopt;

    if (range_end < range_begin)
        return std::nullopt;

    res.port = range_begin;
    res.range = range_end - range_begin;

    return {std::move(res)};
}

int tcp_range::compare(const tcp_range &other) const {
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
