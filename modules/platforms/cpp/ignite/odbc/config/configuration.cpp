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

namespace ignite {

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

void configuration::from_config_map(const config_map &config_params, diagnostic_record_storage *diag) {
    // TODO
}

} // namespace ignite
