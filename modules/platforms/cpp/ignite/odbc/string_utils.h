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

#include <algorithm>
#include <functional>
#include <sstream>

namespace ignite {

/**
 * Remove leading spaces.
 *
 * @param str String view.
 * @return String view without leading spaces.
 */
inline std::string_view ltrim(std::string_view str) {
    while (!str.empty() && std::isspace(str.front()))
        str.remove_prefix(1);

    return str;
}

/**
 * Remove trailing spaces.
 *
 * @param str String view.
 * @return String view without trailing spaces.
 */
inline std::string_view rtrim(std::string_view str) {
    while (!str.empty() && std::isspace(str.back()))
        str.remove_suffix(1);

    return str;
}

/**
 * Remove leading and trailing spaces.
 *
 * @param str String view.
 * @return String view without leading and trailing spaces.
 */
inline std::string_view trim(std::string_view str) {
    return ltrim(rtrim(str));
}

/**
 * Split string once by delimiter.
 * If string has no delimiter, the second returned string is empty.
 *
 * @param delimiter Delimiter.
 * @return Pair of values split by the first encountered delimiter. Delimiter itself is not included.
 */
inline std::pair<std::string_view, std::string_view> split_once(std::string_view str, char delimiter) {
    auto delim_pos = str.find(delimiter);
    if (delim_pos == decltype(str)::npos)
        return {str, {}};

    std::string_view first{str.data(), delim_pos};
    std::string_view second{str};
    second.remove_prefix(delim_pos + 1);
    return {first, second};
}

/**
 * Call action for every substring separated by delimiter.
 *
 * @param str String.
 * @param delimiter Delimiter.
 * @param action Action to perform.
 */
inline void for_every_delimited(
    std::string_view str, char delimiter, const std::function<void(std::string_view)> &action) {
    while (!str.empty()) {
        auto res = split_once(str, delimiter);
        action(res.first);
        str = res.second;
    }
}

/**
 * Split string by delimiter.
 *
 * @param delimiter Delimiter.
 * @return Vector with resulting string parts.
 */
inline std::vector<std::string_view> split(std::string_view str, char delimiter) {
    std::vector<std::string_view> res;
    for_every_delimited(str, delimiter, [&res](auto part) { res.push_back(part); });
    return res;
}

/**
 * Casts value of one type to another type, using string stream.
 *
 * @param val Input value.
 * @param res Resulted value.
 */
template<typename T1, typename T2>
void lexical_cast(const T2 &val, T1 &res) {
    std::stringstream converter;

    converter << val;
    converter >> res;
}

/**
 * Casts value of one type to another type, using string stream.
 *
 * @param val Input value.
 * @return Resulted value.
 */
template<typename T1, typename T2>
T1 lexical_cast(const T2 &val) {
    T1 res;

    lexical_cast<T1, T2>(val, res);

    return res;
}

/**
 * Convert to lower.
 *
 * @param value Value.
 * @return Lower value.
 */
[[nodiscard]] inline std::string to_lower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), ::tolower);
    return value;
}

/**
 * Convert to upper.
 *
 * @param value Value.
 * @return Upper value.
 */
[[nodiscard]] inline std::string to_upper(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), ::toupper);
    return value;
}

} // namespace ignite
