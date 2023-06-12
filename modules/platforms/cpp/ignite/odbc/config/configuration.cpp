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

#include "ignite/odbc/string_utils.h"
#include "ignite/odbc/config/config_tools.h"
#include "ignite/odbc/config/configuration.h"

#include <string>
#include <sstream>

namespace ignite {

const std::string configuration::default_value::driver{"Apache Ignite"};
const std::string configuration::default_value::address{};
const std::int32_t configuration::default_value::page_size{1024};
const std::uint16_t configuration::default_value::port{10800};

std::string configuration::to_connection_string() const
{
    argument_map arguments;

    to_map(arguments);

    std::stringstream connect_string_buffer;

    for (argument_map::const_iterator it = arguments.begin(); it != arguments.end(); ++it)
    {
        const std::string& key = it->first;
        const std::string& value = it->second;

        if (value.empty())
            continue;

        if (value.find(' ') == std::string::npos)
            connect_string_buffer << key << '=' << value << ';';
        else
            connect_string_buffer << key << "={" << value << "};";
    }

    return connect_string_buffer.str();
}

const std::string& configuration::get_driver() const
{
    return m_driver.get_value();
}

void configuration::set_driver(const std::string& driver)
{
    m_driver.set_value(driver);
}

const std::vector<end_point>& configuration::get_addresses() const
{
    return m_end_points.get_value();
}

void configuration::set_addresses(const std::vector<end_point>& end_points)
{
    m_end_points.set_value(end_points);
}

bool configuration::is_addresses_set() const
{
    return m_end_points.is_set();
}

void configuration::set_page_size(int32_t size)
{
    m_page_size.set_value(size);
}

bool configuration::is_page_size_set() const
{
    return m_page_size.is_set();
}

int32_t configuration::get_page_size() const
{
    return m_page_size.get_value();
}

void configuration::to_map(argument_map& res) const
{
    // TODO
    add_to_map(res, "driver", m_driver);
    add_to_map(res, "address", m_end_points);
    add_to_map(res, "page_size", m_page_size);
}

void configuration::from_config_map(config_map config_params, diagnostic_record_storage *diag) {
    // TODO
}

template<>
void configuration::add_to_map(argument_map& map, const std::string& key, const settable_value<int32_t>& value)
{
    if (value.is_set())
        map[key] = lexical_cast<std::string>(value.get_value());
}

template<>
void configuration::add_to_map(argument_map& map, const std::string& key, const settable_value<std::string>& value)
{
    if (value.is_set())
        map[key] = value.get_value();
}

template<>
void configuration::add_to_map(argument_map& map, const std::string& key,
    const settable_value< std::vector<end_point> >& value)
{
    if (value.is_set())
        map[key] = addresses_to_string(value.get_value());
}

} // namespace ignite
