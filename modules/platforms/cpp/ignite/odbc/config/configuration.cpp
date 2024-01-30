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

#include "ignite/odbc/config/configuration.h"
#include "ignite/odbc/config/config_tools.h"
#include "ignite/odbc/odbc_error.h"
#include "ignite/odbc/string_utils.h"

#include <string>

/** Configuration keys . */
namespace key {
/** Key for fetch results page size attribute. */
static inline const std::string page_size{"page_size"};

/** Key for Driver attribute. */
static inline const std::string host{"host"};

/** Key for TCP port attribute. */
static inline const std::string port{"port"};

/** Key for address attribute. */
static inline const std::string address{"address"};

/** Key for address attribute. */
static inline const std::string schema{"schema"};

/** Key for authentication identity. */
static inline const std::string identity{"identity"};

/** Key for authentication secret. */
static inline const std::string secret{"secret"};

} // namespace key

namespace ignite {

void try_get_string_param(
    value_with_default<std::string> &dst, const config_map &config_params, const std::string &key) {
    auto it = config_params.find(key);
    if (it != config_params.end()) {
        dst = {it->second, true};
    }
}

void configuration::from_config_map(const config_map &config_params) {
    *this = configuration();

    auto page_size_it = config_params.find(key::page_size);
    if (page_size_it != config_params.end()) {
        auto page_size_opt = parse_int<std::int32_t>(page_size_it->second);
        if (!page_size_opt)
            throw odbc_error(sql_state::S01S00_INVALID_CONNECTION_STRING_ATTRIBUTE,
                "Invalid page size value: " + page_size_it->second);

        m_page_size = {*page_size_opt, true};
    }

    auto address_it = config_params.find(key::address);
    if (address_it != config_params.end())
        m_end_points = {parse_address(address_it->second), true};
    else {
        end_point ep;
        auto host_it = config_params.find(key::host);
        if (host_it == config_params.end())
            throw odbc_error(
                sql_state::S01S00_INVALID_CONNECTION_STRING_ATTRIBUTE, "Connection address is not specified");

        auto host = host_it->second;
        uint16_t port = default_value::port;

        auto port_it = config_params.find(key::port);
        if (port_it != config_params.end())
            port = parse_port(port_it->second);

        m_end_points = {{{host, port}}, true};
    }

    try_get_string_param(m_schema, config_params, key::schema);
    try_get_string_param(m_auth_identity, config_params, key::identity);
    try_get_string_param(m_auth_secret, config_params, key::secret);
}

} // namespace ignite
