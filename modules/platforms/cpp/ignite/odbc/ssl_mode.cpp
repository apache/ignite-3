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

#include "ignite/odbc/ssl_mode.h"
#include "ignite/odbc/config/config_tools.h"

#include <algorithm>

/** A string token for ssl_mode::DISABLE. */
const std::string DISABLE_TOKEN{"disable"};

/** A string token for ssl_mode::REQUIRE. */
const std::string REQUIRE_TOKEN{"require"};

/** A string token for ssl_mode::UNKNOWN. */
const std::string UNKNOWN_TOKEN{"unknown"};

namespace ignite {

ssl_mode ssl_mode_from_string(std::string_view val, ssl_mode dflt) {
    std::string lower_val = normalize_argument_string(val);

    if (lower_val == DISABLE_TOKEN)
        return ssl_mode::DISABLE;

    if (lower_val == REQUIRE_TOKEN)
        return ssl_mode::REQUIRE;

    return dflt;
}

std::string ssl_mode_to_string(ssl_mode val) {
    switch (val) {
        case ssl_mode::DISABLE:
            return DISABLE_TOKEN;

        case ssl_mode::REQUIRE:
            return REQUIRE_TOKEN;

        case ssl_mode::UNKNOWN:
        default:
            return UNKNOWN_TOKEN;
    }
}

} // namespace ignite
