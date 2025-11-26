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

/** A string token for ssl_mode::DISABLE. */
const std::string DISABLE_TOKEN{"disable"};

/** A string token for ssl_mode::REQUIRE. */
const std::string REQUIRE_TOKEN{"require"};

/** A string token for ssl_mode::UNKNOWN. */
const std::string UNKNOWN_TOKEN{"unknown"};

namespace ignite {

ssl_mode_t ssl_mode_from_string(const std::string &val, ssl_mode_t dflt) {
    std::string lower_val = normalize_argument_string(val);

    if (lower_val == DISABLE_TOKEN)
        return ssl_mode_t::DISABLE;

    if (lower_val == REQUIRE_TOKEN)
        return ssl_mode_t::REQUIRE;

    return dflt;
}

std::string to_string(ssl_mode_t val) {
    switch (val) {
        case ssl_mode_t::DISABLE:
            return DISABLE_TOKEN;

        case ssl_mode_t::REQUIRE:
            return REQUIRE_TOKEN;

        case ssl_mode_t::UNKNOWN:
        default:
            return UNKNOWN_TOKEN;
    }
}

} // namespace ignite
