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

#include "uni_algo/ranges_conv.h"

namespace {
using namespace ignite;

std::pair<std::string_view, std::string_view> split_at(std::string_view input, size_t pos) {
    return {input.substr(0,pos), input.substr(pos + 1)};
}

} // anonymous namespace


namespace ignite {

qualified_name qualified_name::create(std::string_view schema_name, std::string_view object_name) {
    detail::arg_check::container_non_empty(object_name, "Object name");

    if (schema_name.empty()) {
        schema_name = DEFAULT_SCHEMA_NAME;
    }

    return {detail::parse_identifier(schema_name, QUOTE_CHAR, SEPARATOR_CHAR),
        detail::parse_identifier(object_name, QUOTE_CHAR, SEPARATOR_CHAR)};
}

qualified_name qualified_name::parse(std::string_view simple_or_canonical_name) {
    detail::arg_check::container_non_empty(simple_or_canonical_name, "Object name");

    auto utf8_view = una::ranges::utf8_view(simple_or_canonical_name);
    auto separator_pos = detail::find_separator(
        simple_or_canonical_name, utf8_view.begin(), utf8_view.end(), char32_t(QUOTE_CHAR), char32_t(SEPARATOR_CHAR));

    if (separator_pos == utf8_view.end()) {
        return create({}, simple_or_canonical_name);
    }

    auto next_separator = detail::find_separator(simple_or_canonical_name, std::next(separator_pos),
     utf8_view.end(), char32_t(QUOTE_CHAR), char32_t(SEPARATOR_CHAR));

    detail::arg_check::is_true(next_separator == utf8_view.end(),
        "Canonical name should have at most two parts: '" + std::string{simple_or_canonical_name} + "'");

    auto offset = separator_pos.begin() - utf8_view.begin().begin();
    auto [schema_name, object_name] = split_at(simple_or_canonical_name, offset);
    detail::arg_check::container_non_empty(schema_name, "Schema part of the canonical name");
    detail::arg_check::container_non_empty(object_name, "Object part of the canonical name");

    return create(schema_name, object_name);
}

const std::string & qualified_name::get_canonical_name() const {
    if (m_canonical_name.empty()) {
        m_canonical_name
            = detail::quote_if_needed(m_schema_name, QUOTE_CHAR)
            + SEPARATOR_CHAR
            + detail::quote_if_needed(m_object_name, QUOTE_CHAR);
    }
    return m_canonical_name;
}

} // namespace ignite
