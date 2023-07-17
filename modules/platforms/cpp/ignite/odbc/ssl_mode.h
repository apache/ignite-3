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

#include "ignite/odbc/diagnostic/diagnosable.h"

#include <string>

namespace ignite {

/** SSL Mode enum. */
enum class ssl_mode {
    DISABLE = 0,

    REQUIRE = 1,

    UNKNOWN = 100
};

/**
 * Convert mode from string.
 *
 * @param val String value.
 * @param dflt Default value to return on error.
 * @return Corresponding enum value.
 */
[[nodiscard]] ssl_mode ssl_mode_from_string(const std::string &val, ssl_mode dflt = ssl_mode::UNKNOWN);

/**
 * Convert mode to string.
 *
 * @param val Value to convert.
 * @return String value.
 */
[[nodiscard]] std::string to_string(ssl_mode val);

} // namespace ignite
