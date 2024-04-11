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

#include "ignite/protocol/protocol_version.h"
#include "ignite/common/ignite_error.h"

#include <sstream>

namespace ignite::protocol {

const protocol_version protocol_version::VERSION_3_0_0{3, 0, 0};

void throw_parse_error() {
    throw ignite_error("Invalid version format. Valid format is X.Y.Z, where X, Y and Z are major, "
                       "minor and maintenance version parts of Ignite since which protocol is introduced.");
}

std::optional<protocol_version> protocol_version::from_string(const std::string &version) {
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

    buf >> res.m_patch;

    if (buf.bad())
        throw_parse_error();

    return res;
}

std::string protocol_version::to_string() const {
    std::stringstream buf;
    buf << m_major << '.' << m_minor << '.' << m_patch;

    return buf.str();
}

int32_t protocol_version::compare(const protocol_version &other) const {
    int32_t res = m_major - other.m_major;

    if (res == 0)
        res = m_minor - other.m_minor;

    if (res == 0)
        res = m_patch - other.m_patch;

    return res;
}

} // namespace ignite::protocol
