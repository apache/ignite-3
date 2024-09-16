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

#include <ignite/client/ignite_client_authenticator.h>
#include <ignite/client/ignite_logger.h>
#include <ignite/client/ssl_mode.h>

#include <initializer_list>
#include <memory>
#include <string>
#include <vector>

namespace ignite {

/**
 * Ignite client configuration.
 */
class ignite_client_configuration {
public:
    /**
     * TCP port used by client by default if not specified explicitly.
     */
    static constexpr uint16_t DEFAULT_PORT = 10800;

    // Default
    ignite_client_configuration() = default;

    /**
     * Constructor.
     *
     * @param endpoints Endpoints list.
     */
    ignite_client_configuration(std::initializer_list<std::string_view> endpoints)
        : m_endpoints(endpoints.begin(), endpoints.end()) {}

    /**
     * Constructor.
     *
     * @param endpoints Endpoints list.
     */
    ignite_client_configuration(std::vector<std::string> endpoints) // NOLINT(google-explicit-constructor)
        : m_endpoints(std::move(endpoints)) {}

    /**
     * Get endpoints.
     *
     * @see set_endpoints() for more detailed description.
     *
     * @return Endpoints.
     */
    [[nodiscard]] const std::vector<std::string> &get_endpoints() const { return m_endpoints; }

    /**
     * Set endpoints.
     *
     * Examples of supported formats:
     * - 192.168.1.25 - Default port is used, see DEFAULT_PORT;
     * - 192.168.1.25:780 - Custom port;
     * - my-host.com - Default port is used, see DEFAULT_PORT;
     * - my-host.com:780 - Custom port;
     *
     * Default is "localhost"
     *
     * @param endpoints Endpoints.
     */
    void set_endpoints(std::initializer_list<std::string_view> endpoints) {
        ignite_client_configuration::m_endpoints.assign(endpoints.begin(), endpoints.end());
    }

    /**
     * Set endpoints.
     *
     * Examples of supported formats:
     * - 192.168.1.25 - Default port is used, see DEFAULT_PORT;
     * - 192.168.1.25:780 - Custom port;
     * - my-host.com - Default port is used, see DEFAULT_PORT;
     * - my-host.com:780 - Custom port;
     *
     * Default is "localhost"
     *
     * @param endpoints Endpoints.
     */
    void set_endpoints(std::vector<std::string> endpoints) {
        ignite_client_configuration::m_endpoints = std::move(endpoints);
    }

    /**
     * Get logger.
     *
     * @return Current logger.
     */
    [[nodiscard]] const std::shared_ptr<ignite_logger> &get_logger() const { return m_logger; }

    /**
     * Set logger to be used by client.
     *
     * The logger is @c nullptr by default, which means no logging is performed.
     *
     * @param logger Logger to use.
     */
    void set_logger(std::shared_ptr<ignite_logger> logger) { m_logger = std::move(logger); }

    /**
     * Get connection limit.
     *
     * By default, C++ client establishes a connection to every server node listed in @c end_points. Use
     * this setting to limit the number of active connections. This reduces initial connection time and the
     * resource usage, but can have a negative effect on cache operation performance.
     *
     * Zero value means that number of active connections is not limited.
     *
     * The default value is zero.
     *
     * @return Active connection limit.
     */
    [[nodiscard]] uint32_t get_connection_limit() const { return m_connection_limit; }

    /**
     * Set connection limit.
     *
     * @see GetConnectionsLimit for details.
     *
     * @param limit Connections limit to set.
     */
    void set_connection_limit(uint32_t limit) { m_connection_limit = limit; }

    /**
     * Gets the authenticator.
     *
     * @see Also see basic_authenticator.
     *
     * @return Authenticator
     */
    [[nodiscard]] std::shared_ptr<ignite_client_authenticator> get_authenticator() const { return m_authenticator; }

    /**
     * Sets the authenticator.
     *
     * @param authenticator Authenticator. Do not use authentication if authenticator is @c nullptr.
     */
    void set_authenticator(std::shared_ptr<ignite_client_authenticator> authenticator) {
        m_authenticator = std::move(authenticator);
    }

    /**
     * Get SSL mode.
     *
     * @see ssl_mode for details.
     *
     * @return SSL mode.
     */
    [[nodiscard]] ssl_mode get_ssl_mode() const {
        return m_ssl_mode;
    }

    /**
     * Set SSL mode.
     *
     * @see ssl_mode for details.
     *
     * @param ssl_mode SSL mode.
     */
    void set_ssl_mode(ssl_mode ssl_mode) {
        m_ssl_mode = ssl_mode;
    }

    /**
     * Get file path to SSL certificate to use during connection establishment.
     *
     * @return File path to SSL certificate.
     */
    [[nodiscard]] const std::string& get_ssl_cert_file() const {
        return m_ssl_cert_file;
    }

    /**
     * Set file path to SSL certificate to use during connection establishment.
     *
     * @param sslCertFile File path to SSL certificate.
     */
    void set_ssl_cert_file(const std::string& ssl_cert_file) {
        m_ssl_cert_file = ssl_cert_file;
    }

    /**
     * Get file path to SSL private key to use during connection establishment.
     *
     * @return File path to SSL private key.
     */
    [[nodiscard]] const std::string& get_ssl_key_file() const {
        return m_ssl_key_file;
    }

    /**
     * Set file path to SSL private key to use during connection establishment.
     *
     * @param sslKeyFile File path to SSL private key.
     */
    void set_ssl_key_file(const std::string& ssl_key_file) {
        m_ssl_key_file = ssl_key_file;
    }

    /**
     * Get file path to SSL certificate authority to authenticate server certificate during connection
     *  establishment.
     *
     * @return File path to SSL certificate authority.
     */
    [[nodiscard]] const std::string& get_ssl_ca_file() const {
        return m_ssl_ca_file;
    }

    /**
     * Set file path to SSL certificate authority to authenticate server certificate during connection
     *  establishment.
     *
     * @param sslCaFile File path to SSL certificate authority.
     */
    void set_ssl_ca_file(const std::string& ssl_ca_file) {
        m_ssl_ca_file = ssl_ca_file;
    }


private:
    /** Endpoints. */
    std::vector<std::string> m_endpoints{"localhost"};

    /** Logger. */
    std::shared_ptr<ignite_logger> m_logger{};

    /** Authenticator. */
    std::shared_ptr<ignite_client_authenticator> m_authenticator{};

    /** Active connections limit. */
    uint32_t m_connection_limit{0};

    /** SSL Mode. */
    ssl_mode m_ssl_mode{ssl_mode::DISABLE};

    /** File path to SSL certificate. */
    std::string m_ssl_cert_file;

    /** File path to SSL private key. */
    std::string m_ssl_key_file;

    /** File path to SSL certificate authority. */
    std::string m_ssl_ca_file;
};

} // namespace ignite
