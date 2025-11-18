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
#include "ignite/common/detail/string_utils.h"

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

/** Key for timezone. */
static inline const std::string timezone{"timezone"};

/** Key for SSL mode. */
static inline const std::string ssl_mode{"ssl_mode"};

/** Key for the SSL private key file path. */
static inline const std::string ssl_key_file{"ssl_key_file"};

/** Key for the SSL certificate file path. */
static inline const std::string ssl_cert_file{"ssl_cert_file"};

/** Key for the SSL certificate authority file path. */
static inline const std::string ssl_ca_file{"ssl_ca_file"};

/** Key for the heartbeat interval in ms. */
static inline const std::string heartbeat_interval{"heartbeat_interval"};

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

    auto ssl_mode_it = config_params.find(key::ssl_mode);
    if (ssl_mode_it != config_params.end()) {
        auto ssl_mode = ssl_mode_from_string(ssl_mode_it->second);
        if (ssl_mode == ssl_mode_t::UNKNOWN) {
            throw odbc_error(sql_state::S01S00_INVALID_CONNECTION_STRING_ATTRIBUTE,
                "Unsupported SSL mode value: " + ssl_mode_it->second);
        }
        m_ssl_mode = {ssl_mode, true};
    }

    try_get_string_param(m_schema, config_params, key::schema);

    try_get_string_param(m_auth_identity, config_params, key::identity);
    try_get_string_param(m_auth_secret, config_params, key::secret);

    try_get_string_param(m_timezone, config_params, key::timezone);

    try_get_string_param(m_ssl_key_file, config_params, key::ssl_key_file);
    try_get_string_param(m_ssl_cert_file, config_params, key::ssl_cert_file);
    try_get_string_param(m_ssl_ca_file, config_params, key::ssl_ca_file);

    auto heartbeat_interval_it = config_params.find(key::heartbeat_interval);
    if (heartbeat_interval_it != config_params.end()) {
        auto heartbeat_interval_opt = parse_int<std::int32_t>(heartbeat_interval_it->second);
        if (!heartbeat_interval_opt)
            throw odbc_error(sql_state::S01S00_INVALID_CONNECTION_STRING_ATTRIBUTE,
                "Invalid heartbeat interval value: " + heartbeat_interval_it->second);

        m_heartbeat_interval = {std::chrono::milliseconds{*heartbeat_interval_opt}, true};
    }
}

} // namespace ignite
