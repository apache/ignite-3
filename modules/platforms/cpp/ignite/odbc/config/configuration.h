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

#include "ignite/odbc/config/config_tools.h"
#include "ignite/odbc/config/value_with_default.h"
#include "ignite/odbc/ssl_mode.h"

#include "ignite/protocol/protocol_context.h"
#include "ignite/common/end_point.h"

#include <chrono>
#include <cstdint>
#include <string>

namespace ignite {

/**
 * ODBC configuration abstraction.
 */
class configuration {
public:
    /** Default values for configuration. */
    struct default_value {
        /** Default value for fetch results page size attribute. */
        static inline const std::int32_t page_size{1024};

        /** Default value for Driver attribute. */
        static inline const std::string host{"localhost"};

        /** Default value for TCP port attribute. */
        static inline const std::uint16_t port{protocol::protocol_context::DEFAULT_TCP_PORT};

        /** Default value for Address attribute. */
        static inline const std::vector<end_point> address{{host, port}};

        /** Default value for Schema attribute. */
        static inline const std::string schema{"PUBLIC"};

        /** Default value for Timezone attribute. */
        static inline const std::string timezone{};

        /** Default value for SSL mode. */
        static inline const ssl_mode_t ssl_mode{ssl_mode_t::DISABLE};

        /** Default heartbeat interval. */
        static inline const std::chrono::seconds heartbeat_interval{std::chrono::seconds{30}};
    };

    // Default.
    configuration() = default;

    // With auth.
    configuration(std::string identity, std::string secret)
        : m_auth_identity{std::move(identity), true}
        , m_auth_secret{std::move(secret), true} {}

    /**
     * Get addresses.
     *
     * @return Addresses.
     */
    [[nodiscard]] const value_with_default<std::vector<end_point>> &get_address() const { return m_end_points; }

    /**
     * Set addresses.
     *
     * @param addr Addresses.
     */
    void set_address(std::string addr) { m_end_points = {parse_address(addr), true}; }

    /**
     * Get fetch results page size.
     *
     * @return Fetch results page size.
     */
    [[nodiscard]] const value_with_default<std::int32_t> &get_page_size() const { return m_page_size; }

    /**
     * Set fetch results page size.
     *
     * @param page_size Fetch results page size.
     */
    void set_page_size(std::int32_t page_size) { m_page_size = {page_size, true}; }

    /**
     * Get schema.
     *
     * @return Schema.
     */
    [[nodiscard]] const value_with_default<std::string> &get_schema() const { return m_schema; }

    /**
     * Set schema.
     *
     * @param schema Schema.
     */
    void set_schema(std::string schema) { m_schema = {std::move(schema), true}; }

    /**
     * Get the authentication type.
     *
     * @return Authentication type.
     */
    [[nodiscard]] const std::string &get_auth_type() const { return TYPE; };

    /**
     * Get identity.
     *
     * @return Identity.
     */
    [[nodiscard]] const value_with_default<std::string> &get_auth_identity() const { return m_auth_identity; };

    /**
     * Set identity.
     *
     * @param identity Identity.
     */
    void set_auth_identity(std::string identity) { m_auth_identity = {std::move(identity), true}; }

    /**
     * Get secret.
     *
     * @return Secret.
     */
    [[nodiscard]] const value_with_default<std::string> &get_auth_secret() const { return m_auth_secret; };

    /**
     * Set secret.
     *
     * @param secret Secret.
     */
    void set_auth_secret(std::string secret) { m_auth_secret = {std::move(secret), true}; }

    /**
     * Get Timezone.
     *
     * @return Timezone.
     */
    [[nodiscard]] const value_with_default<std::string> &get_timezone() const { return m_timezone; };

    /**
     * Get the SSL Mode.
     *
     * @return SSL Mode.
     */
    [[nodiscard]] const value_with_default<ssl_mode_t> &get_ssl_mode() const { return m_ssl_mode; }

    /**
     * Set SSL mode.
     *
     * @param ssl_mode SSL Mode.
     */
    void set_ssl_mode(ssl_mode_t ssl_mode) { m_ssl_mode = { ssl_mode, true }; }

    /**
     * Get the SSL private key file path.
     *
     * @return SSL private key file path.
     */
    [[nodiscard]] const value_with_default<std::string> &get_ssl_key_file() const { return m_ssl_key_file; }

    /**
     * Set SSL key file.
     *
     * @param ssl_key_file SSL key file.
     */
    void set_ssl_key_file(std::string ssl_key_file) { m_ssl_key_file = {std::move(ssl_key_file), true}; }

    /**
     * Get the SSL certificate file path.
     *
     * @return SSL certificate file path.
     */
    [[nodiscard]] const value_with_default<std::string> &get_ssl_cert_file() const { return m_ssl_cert_file; }

    /**
     * Set SSL certificate file.
     *
     * @param ssl_cert_file SSL certificate file.
     */
    void set_ssl_cert_file(std::string ssl_cert_file) { m_ssl_cert_file = {std::move(ssl_cert_file), true}; }

    /**
     * Get the SSL certificate authority file path.
     *
     * @return SSL certificate authority file path.
     */
    [[nodiscard]] const value_with_default<std::string> &get_ssl_ca_file() const { return m_ssl_ca_file; }

    /**
     * Set SSL CA certificate file.
     *
     * @param ssl_ca_file SSL CA certificate file.
     */
    void set_ssl_ca_file(std::string ssl_ca_file) { m_ssl_ca_file = {std::move(ssl_ca_file), true}; }

    /**
     * Get the heartbeat interval.
     *
     * @return Heartbeat interval.
     */
    [[nodiscard]] const value_with_default<std::chrono::milliseconds> &get_heartbeat_interval() const {
        return m_heartbeat_interval;
    }

    /**
     * Set heartbeat interval.
     *
     * @param interval Heartbeat interval in ms.
     */
    void set_heartbeat_interval(std::chrono::milliseconds interval) { m_heartbeat_interval = {interval, true}; }

    /**
     * Fill from configuration params.
     *
     * @throw odbc_error On parsing error.
     * @param config_params Configuration params
     */
    void from_config_map(const config_map &config_params);

private:
    /** Type constant. */
    inline static const std::string TYPE{"basic"};

    /** Request and response page size. */
    value_with_default<std::int32_t> m_page_size{default_value::page_size, false};

    /** Connection end-points. */
    value_with_default<std::vector<end_point>> m_end_points{default_value::address, false};

    /** Schema. */
    value_with_default<std::string> m_schema{default_value::schema, false};

    /** Identity. */
    value_with_default<std::string> m_auth_identity{{}, false};

    /** Secret. */
    value_with_default<std::string> m_auth_secret{{}, false};

    /** Timezone. */
    value_with_default<std::string> m_timezone{default_value::timezone, false};

    /** SSL mode. */
    value_with_default<ssl_mode_t> m_ssl_mode{default_value::ssl_mode, false};

    /** SSL private key file path. */
    value_with_default<std::string> m_ssl_key_file{{}, false};

    /** SSL certificate file path. */
    value_with_default<std::string> m_ssl_cert_file{{}, false};

    /** SSL certificate authority file path. */
    value_with_default<std::string> m_ssl_ca_file{{}, false};

    /** Heartbeat interval. */
    value_with_default<std::chrono::milliseconds> m_heartbeat_interval{default_value::heartbeat_interval, false};
};

} // namespace ignite
