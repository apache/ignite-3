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

#include "ignite/protocol/messages.h"
#include "ignite/protocol/buffer_adapter.h"
#include "ignite/protocol/reader.h"
#include "ignite/protocol/utils.h"
#include "ignite/protocol/writer.h"

namespace ignite::protocol {

std::vector<std::byte> make_handshake_request(
    std::int8_t client_type, protocol_version ver, std::map<std::string, std::string> extensions) {
    std::vector<std::byte> message;
    buffer_adapter buffer(message);
    buffer.write_raw(bytes_view(MAGIC_BYTES));

    write_message_to_buffer(buffer, [=, &extensions](protocol::writer &writer) {
        writer.write(ver.get_major());
        writer.write(ver.get_minor());
        writer.write(ver.get_patch());

        writer.write(client_type);

        // Features.
        writer.write_binary_empty();

        // Extensions.
        writer.write_map(extensions);
    });

    return message;
}

handshake_response parse_handshake_response(bytes_view message) {
    handshake_response res{};

    protocol::reader reader(message);

    auto ver_major = reader.read_int16();
    auto ver_minor = reader.read_int16();
    auto ver_patch = reader.read_int16();

    protocol::protocol_version ver(ver_major, ver_minor, ver_patch);
    res.context.set_version(ver);
    res.error = protocol::try_read_error(reader);

    if (res.error)
        return res;

    UNUSED_VALUE reader.read_int64(); // TODO: IGNITE-17606 Implement heartbeats
    UNUSED_VALUE reader.read_string_nullable(); // Cluster node ID. Needed for partition-aware compute.
    UNUSED_VALUE reader.read_string_nullable(); // Cluster node name. Needed for partition-aware compute.

    res.context.set_cluster_id(reader.read_uuid());
    res.context.set_cluster_name(reader.read_string());

    res.observable_timestamp = reader.read_int64();

    auto dbms_ver_major = reader.read_uint8();
    auto dbms_ver_minor = reader.read_uint8();
    auto dbms_ver_maintenance = reader.read_uint8();
    auto dbms_ver_patch = reader.read_uint8_nullable();
    auto dbms_ver_pre_release = reader.read_string_nullable();

    res.context.set_server_version(
        {dbms_ver_major, dbms_ver_minor, dbms_ver_maintenance, dbms_ver_patch, dbms_ver_pre_release});

    reader.skip(); // Features.
    reader.skip(); // Extensions.

    return res;
}

} // namespace ignite::protocol
