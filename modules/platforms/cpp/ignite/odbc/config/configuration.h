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

#include "ignite/common/end_point.h"

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
        static inline const std::uint16_t port{10800};

        /** Default value for Address attribute. */
        static inline const std::vector<end_point> address{{host, port}};

        /** Default value for Schema attribute. */
        static inline const std::string schema{"PUBLIC"};

        /** Default value for Timezone attribute. */
        static inline const std::string timezone{};
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
     * Get fetch results page size.
     *
     * @return Fetch results page size.
     */
    [[nodiscard]] const value_with_default<std::int32_t> &get_page_size() const { return m_page_size; }

    /**
     * Get schema.
     *
     * @return Schema.
     */
    [[nodiscard]] const value_with_default<std::string> &get_schema() const { return m_schema; }

    /**
     * Get authentication type.
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
     * Get secret.
     *
     * @return Secret.
     */
    [[nodiscard]] const value_with_default<std::string> &get_auth_secret() const { return m_auth_secret; };

    /**
     * Get Timezone.
     *
     * @return Timezone.
     */
    [[nodiscard]] const value_with_default<std::string> &get_timezone() const { return m_timezone; };

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
    value_with_default<std::string> m_auth_identity{"", false};

    /** Secret. */
    value_with_default<std::string> m_auth_secret{"", false};

    /** Timezone. */
    value_with_default<std::string> m_timezone{"", false};
};

} // namespace ignite
