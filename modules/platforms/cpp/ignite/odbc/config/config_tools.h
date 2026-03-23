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

#include "ignite/common/end_point.h"

#include <map>
#include <string>
#include <vector>

namespace ignite {

/**
 * Convert the address list to string.
 *
 * @param addresses Addresses.
 * @return Resulting string.
 */
[[nodiscard]] std::string addresses_to_string(const std::vector<end_point> &addresses);

/**
 * Parse address.
 *
 * @throw odbc_error on error.
 * @param value String value to parse.
 * @return End points list.
 */
[[nodiscard]] std::vector<end_point> parse_address(std::string_view value);

/** Configuration options map */
typedef std::map<std::string, std::string> config_map;

/**
 * Parse connection string into a map containing configuration attributes.
 *
 * @param str Connection string.
 * @return A map containing configuration attributes.
 */
[[nodiscard]] config_map parse_connection_string(std::string_view str);

/**
 * Parse DSN configuration string into a map containing configuration attributes.
 *
 * @param str DSN string. Must be terminated with two subsequent '\0'.
 * @return A map containing configuration attributes.
 */
[[nodiscard]] config_map parse_config_attributes(const char *str);

/**
 * Normalize argument string, i.e. strip leading and trailing whitespaces and convert to lowercase.
 *
 * @param value Value.
 * @return Normalized string.
 */
[[nodiscard]] std::string normalize_argument_string(std::string_view value);

} // namespace ignite
