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

#include "ignite/client/detail/table/name_utils.h"
#include "ignite/client/table/qualified_name.h"
#include "ignite/client/detail/argument_check_utils.h"
#include "ignite/common/detail/string_utils.h"

#include "uni_algo/case.h"
#include "uni_algo/prop.h"
#include "uni_algo/ranges_conv.h"

namespace ignite::detail {

std::string quote(std::string_view &str) {
    auto quote_num = std::count(str.begin(), str.end(), qualified_name::QUOTE_CHAR);

    std::string res;
    res.reserve(str.size() + quote_num + 2);

    res.push_back(qualified_name::QUOTE_CHAR);

    if (quote_num) {
        for (auto c : str) {
            if (c == qualified_name::QUOTE_CHAR) {
                res.push_back(qualified_name::QUOTE_CHAR);
            }
            res.push_back(c);
        }
    } else {
        res.append(str);
    }

    res.push_back(qualified_name::QUOTE_CHAR);
    return res;
}

std::string unquote(std::string_view &identifier)
{
    if (identifier.empty())
        return {};

    if (identifier.front() == qualified_name::QUOTE_CHAR) {
        identifier.remove_prefix(1);
        identifier.remove_suffix(1);

        auto quote_num = std::count(identifier.begin(), identifier.end(), qualified_name::QUOTE_CHAR);
        if (quote_num == 0)
            return std::string{identifier};

        std::string res;
        res.reserve(identifier.size() - quote_num);

        auto ignore_quote = false;
        for (auto c : identifier) {
            if (ignore_quote && c != qualified_name::QUOTE_CHAR) {
                throw ignite_error(error::code::ILLEGAL_ARGUMENT,
                    "Quoted identifier contains non-escaped quotes: " + std::string(identifier));
            }

            if (c == qualified_name::QUOTE_CHAR) {
                if (ignore_quote) {
                    ignore_quote = false;
                    continue;
                }
                ignore_quote = true;
            }
            res.push_back(c);
        }

        return res;
    }

    return una::cases::to_uppercase_utf8(identifier);
}

std::string parse_identifier(std::string_view &identifier)
{
    auto separator_num =
        std::count(identifier.begin(), identifier.end(), qualified_name::SEPARATOR_CHAR);

    detail::arg_check::is_true(separator_num == 0, "Unexpected separator in identifier: " + std::string{identifier});

    return unquote(identifier);
}

bool is_identifier_extend(char32_t codepoint) {
    constexpr std::int32_t UNICODE_MIDDLE_DOT = 0xB7;

    auto cat = una::codepoint::get_general_category(codepoint);

    return codepoint == UNICODE_MIDDLE_DOT
        || cat == una::codepoint::general_category::Mn
        || cat == una::codepoint::general_category::Mc
        || cat == una::codepoint::general_category::Nd
        || cat == una::codepoint::general_category::Pc
        || cat == una::codepoint::general_category::Cf;
}

} // namespace ignite::detail


