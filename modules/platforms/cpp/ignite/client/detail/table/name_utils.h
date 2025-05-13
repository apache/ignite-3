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

#include <string_view>

namespace ignite::detail {

/**
 * Quote string.
 *
 * @param str A string to quote.
 * @return Quoted string.
 */
[[nodiscard]] std::string quote(std::string_view &str);

/**
 * Wraps the given name with double quotes if it is not uppercased non-quoted name,
 * e.g. "myColumn" -> "\"myColumn\"", "MYCOLUMN" -> "MYCOLUMN".
 *
 * @param name Name.
 * @return Quoted name.
 */
[[nodiscard]] std::string quote_if_needed(std::string_view name);

/**
 * Unquotes the specified identifier, or converts it to upper case if it is not quoted.
 *
 * @param identifier Identifier
 * @return Unquoted or uppercased identifier
 */
[[nodiscard]] std::string unquote(std::string_view &identifier);

/**
 * Parses the specified identifier.
 *
 * @param identifier Identifier
 * @return Parsed identifier.
 */
[[nodiscard]] std::string parse_identifier(std::string_view &identifier);

/**
 * Check whether the char is an identifier extend is U+00B7, or any character in the Unicode General Category classes
 * "Mn", "Mc", "Nd", "Pc", or "Cf".
 *
 * @param codepoint Char to check.
 * @return @c true if the char is an identifier extend.
 */
[[nodiscard]] bool is_identifier_extend(char32_t codepoint);

} // namespace ignite
