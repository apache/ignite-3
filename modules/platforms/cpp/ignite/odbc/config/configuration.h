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

#include "ignite/odbc/config/settable_value.h"
#include "ignite/odbc/config/config_tools.h"
#include "ignite/odbc/protocol_version.h"
#include "ignite/odbc/ssl_mode.h"

#include "ignite/common/end_point.h"

#include <cstdint>
#include <string>
#include <map>

// TODO: IGNITE-19206 needs to be replaced. Every option should be represented by a class instance.
namespace ignite
{

/**
 * ODBC configuration abstraction.
 */
class configuration
{
public:
    /** Default values for configuration. */
    struct default_value
    {
        /** Default value for Driver attribute. */
        static inline const std::string driver{"Apache Ignite"};

        /** Default value for address attribute. */
        static inline const std::string address{};

        /** Default value for fetch results page size attribute. */
        static inline const std::int32_t page_size{1024};

        /** Default value for Driver attribute. */
        static inline const std::string host{"localhost"};

        /** Default value for TCP port attribute. */
        static inline const std::uint16_t port{10800};
    };

    /**
     * Default constructor.
     */
    configuration()
        : m_driver(default_value::driver)
        , m_page_size(default_value::page_size)
        , m_end_points({}) { }

    /**
     * Get Driver.
     *
     * @return Driver name.
     */
    [[nodiscard]] const std::string& get_driver() const;

    /**
     * Set driver.
     *
     * @param driver Driver.
     */
    void set_driver(const std::string& driver);

    /**
     * Get addresses.
     *
     * @return Addresses.
     */
    [[nodiscard]] const std::vector<end_point>& get_addresses() const;

    /**
     * Set addresses to connect to.
     *
     * @param end_points Addresses.
     */
    void set_addresses(const std::vector<end_point>& end_points);

    /**
     * Check if the value set.
     *
     * @return @true if the value set.
     */
    [[nodiscard]] bool is_addresses_set() const;

    /**
     * Get fetch results page size.
     *
     * @return Fetch results page size.
     */
    [[nodiscard]] std::int32_t get_page_size() const;

    /**
     * Set fetch results page size.
     *
     * @param size Fetch results page size.
     */
    void set_page_size(std::int32_t size);

    /**
     * Check if the value set.
     *
     * @return @true if the value set.
     */
    [[nodiscard]] bool is_page_size_set() const;

    /**
     * Fill from configuration params.
     *
     * @param config_params Configuration params
     * @param diag Diagnostics collector.
     */
    void from_config_map(const config_map &config_params, diagnostic_record_storage *diag);

private:
    /** Driver name. */
    settable_value<std::string> m_driver;

    /** Request and response page size. */
    settable_value<int32_t> m_page_size;

    /** Connection end-points. */
    settable_value< std::vector<end_point> > m_end_points;
};

} // namespace ignite
