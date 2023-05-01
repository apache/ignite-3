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
#include <sstream>

namespace ignite {

/**
 * Check if all characters are digits.
 *
 * @param val Value to check.
 */
inline bool all_digits(const std::string &val)
{
    std::string::const_iterator i = val.begin();
    while (i != val.end() && isdigit(*i))
        ++i;

    return i == val.end();
}

/**
 * Skip leading spaces.
 *
 * @param begin Iterator to the beginning of the character sequence.
 * @param end Iterator to the end of the character sequence.
 * @return Iterator to first non-blanc character.
 */
template<typename Iterator>
Iterator skip_leading_spaces(Iterator begin, Iterator end)
{
    Iterator res = begin;
    while (isspace(*res) && res != end)
        ++res;

    return res;
}

/**
 * Skip trailing spaces.
 *
 * @param begin Iterator to the beginning of the character sequence.
 * @param end Iterator to the end of the character sequence.
 * @return Iterator to last non-blanc character.
 */
template<typename Iterator>
Iterator skip_trailing_spaces(Iterator begin, Iterator end)
{
    Iterator res = end - 1;
    while (isspace(*res) && res != begin - 1)
        --res;

    return res + 1;
}

/**
 * Remove leading and trailing spaces.
 *
 * @param begin Iterator to the beginning of the character sequence.
 * @param end Iterator to the end of the character sequence.
 * @return String without leading and trailing spaces.
 */
template<typename Iterator>
std::string strip_surrounding_whitespaces(Iterator begin, Iterator end)
{
    std::string res;

    if (begin >= end)
        return res;

    Iterator skipped_leading = skip_leading_spaces(begin, end);
    Iterator skipped_trailing = skip_trailing_spaces(skipped_leading, end);

    res.reserve(skipped_trailing - skipped_leading);

    std::copy(skipped_leading, skipped_trailing, std::back_inserter(res));

    return res;
}


/**
 * Casts value of one type to another type, using string stream.
 *
 * @param val Input value.
 * @param res Resulted value.
 */
template<typename T1, typename T2>
void lexical_cast(const T2& val, T1& res)
{
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
T1 lexical_cast(const T2 &val)
{
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
[[nodiscard]] std::string to_lower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), ::tolower);
    return value;
}

} // namespace ignite
