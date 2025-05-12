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

#include "ignite/client/table/qualified_name.h"
#include "ignite/client/detail/argument_check_utils.h"
#include "ignite/common/detail/string_utils.h"

#include "uni_algo/case.h"
#include "uni_algo/prop.h"
#include "uni_algo/ranges_conv.h"

#include <cctype>

namespace {
using namespace ignite;

/**
 * Quote string.
 *
 * @param str A string to quote.
 * @return Quoted string.
 */
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

/**
 * Unquotes the specified identifier, or converts it to upper case if it is not quoted.
 *
 * @param identifier Identifier
 * @return Unquoted or uppercased identifier
 */
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

/**
 * Check whether the char is an identifier extend is U+00B7, or any character in the Unicode General Category classes
 * "Mn", "Mc", "Nd", "Pc", or "Cf".
 *
 * @param codepoint Char to check.
 * @return @c true if the char is an identifier extend.
 */
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

} // anonymous namespace

namespace ignite {

qualified_name qualified_name::create(std::string schema_name, std::string object_name) {
    detail::arg_check::container_non_empty(object_name, "Object name");

    if (schema_name.empty()) {
        schema_name = DEFAULT_SCHEMA_NAME;
    }

    // TODO: Parse names here.
    return {std::move(schema_name), std::move(object_name)};
}

qualified_name qualified_name::parse(std::string_view simple_or_canonical_name) {
    detail::arg_check::container_non_empty(simple_or_canonical_name, "Object name");

    auto separator_num =
        std::count(simple_or_canonical_name.begin(), simple_or_canonical_name.end(), qualified_name::SEPARATOR_CHAR);

    if (separator_num == 0) {
        return create({}, unquote(simple_or_canonical_name));
    }

    detail::arg_check::is_true(separator_num == 1,
        "Canonical name should have at most two parts: " + std::string{simple_or_canonical_name});

    auto [schema_name, object_name] = split_once(simple_or_canonical_name, SEPARATOR_CHAR);
    detail::arg_check::container_non_empty(schema_name, "Schema part of the canonical name");
    detail::arg_check::container_non_empty(object_name, "Object part of the canonical name");

    return create(unquote(schema_name), unquote(object_name));
}

std::string qualified_name::quote_if_needed(std::string_view name) {
    if (name.empty()) {
        return std::string{name};
    }

    if (!std::isupper(name[0]) && name[0] != '_') {
        return quote(name);
    }

    auto other_chars = name;
    other_chars.remove_prefix(1);

    auto utf8_view = una::ranges::utf8_view(other_chars);
    for (char32_t cur : utf8_view) {
        if (!std::isupper(cur) && cur != '_' && !is_identifier_extend(cur)) {
            return quote(name);
        }
    }

    return std::string{name};
}

} // namespace ignite
