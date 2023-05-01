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
#include "ignite/odbc/protocol_version.h"
#include "ignite/odbc/ssl_mode.h"

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
    /** Argument map type. */
    typedef std::map<std::string, std::string> argument_map;

    /** Default values for configuration. */
    struct default_value
    {
        /** Default value for Driver attribute. */
        static const std::string driver;

        /** Default value for address attribute. */
        static const std::string address;

        /** Default value for fetch results page size attribute. */
        static const int32_t pageSize;
    };

    /**
     * Default constructor.
     */
    configuration()
        : m_driver(default_value::driver)
        , m_page_size(default_value::pageSize)
        , m_end_points({}) { }

    /**
     * Convert configure to connect string.
     *
     * @return Connect string.
     */
    [[nodiscard]] std::string to_connection_string() const;

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
    [[nodiscard]] const std::vector<network::tcp_range>& get_addresses() const;

    /**
     * Set addresses to connect to.
     *
     * @param end_points Addresses.
     */
    void set_addresses(const std::vector<network::tcp_range>& end_points);

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
    [[nodiscard]] int32_t get_page_size() const;

    /**
     * Set fetch results page size.
     *
     * @param size Fetch results page size.
     */
    void set_page_size(int32_t size);

    /**
     * Check if the value set.
     *
     * @return @true if the value set.
     */
    [[nodiscard]] bool is_page_size_set() const;

    /**
     * Get argument map.
     *
     * @param res Resulting argument map.
     */
    void to_map(argument_map& res) const;

private:
    /**
     * Add key and value to the argument map.
     *
     * @param map Map.
     * @param key Key.
     * @param value Value.
     */
    template<typename T>
    static void add_to_map(argument_map& map, const std::string& key, const settable_value<T>& value);

    /** Driver name. */
    settable_value<std::string> m_driver;

    /** Request and response page size. */
    settable_value<int32_t> m_page_size;

    /** Connection end-points. */
    settable_value< std::vector<network::tcp_range> > m_end_points;
};

template<>
void configuration::add_to_map<std::string>(argument_map& map, const std::string& key,
    const settable_value<std::string>& value);

template<>
void configuration::add_to_map<int32_t>(argument_map& map, const std::string& key,
    const settable_value<int32_t>& value);

template<>
void configuration::add_to_map< std::vector<network::tcp_range> >(argument_map& map, const std::string& key,
    const settable_value< std::vector<network::tcp_range> >& value);

} // namespace ignite
