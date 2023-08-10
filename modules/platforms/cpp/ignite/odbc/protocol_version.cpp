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

#include "ignite/odbc/protocol_version.h"
#include "ignite/odbc/odbc_error.h"
#include "ignite/odbc/utility.h"

#include <sstream>

namespace ignite {
const protocol_version protocol_version::VERSION_3_0_0(3, 0, 0);

protocol_version::version_set::value_type supported_array[] = {
    protocol_version::VERSION_3_0_0,
};

const protocol_version::version_set protocol_version::m_supported(
    supported_array, supported_array + (sizeof(supported_array) / sizeof(supported_array[0])));

const protocol_version::version_set &protocol_version::get_supported() {
    return m_supported;
}

const protocol_version &protocol_version::get_current() {
    return VERSION_3_0_0;
}

void throw_parse_error() {
    throw odbc_error(sql_state::SHY092_OPTION_TYPE_OUT_OF_RANGE,
        "Invalid version format. Valid format is X.Y.Z, where X, Y and Z are major, "
        "minor and maintenance version parts of Ignite since which protocol is introduced.");
}

protocol_version protocol_version::from_string(const std::string &version) {
    protocol_version res;

    std::stringstream buf(version);

    buf >> res.m_major;

    if (!buf.good())
        throw_parse_error();

    if (buf.get() != '.' || !buf.good())
        throw_parse_error();

    buf >> res.m_minor;

    if (!buf.good())
        throw_parse_error();

    if (buf.get() != '.' || !buf.good())
        throw_parse_error();

    buf >> res.m_maintenance;

    if (buf.bad())
        throw_parse_error();

    return res;
}

std::string protocol_version::to_string() const {
    std::stringstream buf;
    buf << m_major << '.' << m_minor << '.' << m_maintenance;

    return buf.str();
}

int16_t protocol_version::get_major() const {
    return m_major;
}

int16_t protocol_version::get_minor() const {
    return m_minor;
}

int16_t protocol_version::get_maintenance() const {
    return m_maintenance;
}

bool protocol_version::is_supported() const {
    return m_supported.count(*this) != 0;
}

int32_t protocol_version::compare(const protocol_version &other) const {
    int32_t res = m_major - other.m_major;

    if (res == 0)
        res = m_minor - other.m_minor;

    if (res == 0)
        res = m_maintenance - other.m_maintenance;

    return res;
}

bool operator==(const protocol_version &val1, const protocol_version &val2) {
    return val1.compare(val2) == 0;
}

bool operator!=(const protocol_version &val1, const protocol_version &val2) {
    return val1.compare(val2) != 0;
}

bool operator<(const protocol_version &val1, const protocol_version &val2) {
    return val1.compare(val2) < 0;
}

bool operator<=(const protocol_version &val1, const protocol_version &val2) {
    return val1.compare(val2) <= 0;
}

bool operator>(const protocol_version &val1, const protocol_version &val2) {
    return val1.compare(val2) > 0;
}

bool operator>=(const protocol_version &val1, const protocol_version &val2) {
    return val1.compare(val2) >= 0;
}

} // namespace ignite
