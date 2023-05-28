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

#include "ignite/odbc/message.h"

namespace ignite
{

void handshake_request::write(protocol::writer &writer, const protocol_version&) const
{
    writer.write(std::int8_t(request_type::HANDSHAKE));

    protocol_version version = m_config.get_protocol_version();
    writer.write(version.get_major());
    writer.write(version.get_minor());
    writer.write(version.get_maintenance());

    writer.write(std::int8_t(client_type::ODBC));
}

void response::read(protocol::reader &reader, const protocol_version& ver)
{
    m_status = response_status(reader.read_int32());

    if (m_status == response_status::SUCCESS)
        read_on_success(reader, ver);
    else
        m_error = reader.read_string();
}

void handshake_response::read(protocol::reader &reader, const protocol_version&)
{
    m_accepted = reader.read_bool();

    if (!m_accepted)
    {
        auto major = reader.read_int16();
        auto minor = reader.read_int16();
        auto maintenance = reader.read_int16();

        m_current_ver = protocol_version(major, minor, maintenance);
        m_error = reader.read_string();
    }
}

} // namespace ignite
