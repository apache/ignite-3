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

#include <cctype>

namespace ignite {

qualified_name qualified_name::create(std::string_view schema_name, std::string_view object_name) {
    detail::arg_check::container_non_empty(object_name, "Object name");

    if (schema_name.empty()) {
        schema_name = DEFAULT_SCHEMA_NAME;
    }

    return {detail::parse_identifier(schema_name), detail::parse_identifier(object_name)};
}

qualified_name qualified_name::parse(std::string_view simple_or_canonical_name) {
    detail::arg_check::container_non_empty(simple_or_canonical_name, "Object name");

    auto separator_num =
        std::count(simple_or_canonical_name.begin(), simple_or_canonical_name.end(), qualified_name::SEPARATOR_CHAR);

    if (separator_num == 0) {
        return create({}, simple_or_canonical_name);
    }

    detail::arg_check::is_true(separator_num == 1,
        "Canonical name should have at most two parts: " + std::string{simple_or_canonical_name});

    auto [schema_name, object_name] = split_once(simple_or_canonical_name, SEPARATOR_CHAR);
    detail::arg_check::container_non_empty(schema_name, "Schema part of the canonical name");
    detail::arg_check::container_non_empty(object_name, "Object part of the canonical name");

    return create(schema_name, object_name);
}

std::string qualified_name::quote_if_needed(std::string_view name) {
    if (name.empty()) {
        return std::string{name};
    }

    if (!std::isupper(name[0]) && name[0] != '_') {
        return detail::quote(name);
    }

    auto other_chars = name;
    other_chars.remove_prefix(1);

    auto utf8_view = una::ranges::utf8_view(other_chars);
    for (char32_t cur : utf8_view) {
        if (!std::isupper(cur) && cur != '_' && !detail::is_identifier_extend(cur)) {
            return detail::quote(name);
        }
    }

    return std::string{name};
}

} // namespace ignite
