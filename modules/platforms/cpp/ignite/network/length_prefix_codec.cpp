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

#include "length_prefix_codec.h"

#include "ignite/common/detail/bytes.h"
#include <ignite/protocol/utils.h>

namespace ignite::network {

length_prefix_codec::length_prefix_codec()
    : m_packet_size(-1)
    , m_packet()
    , m_magic_received(false) {
}

data_buffer_owning length_prefix_codec::encode(data_buffer_owning &data) {
    // Just pass data as is, because we encode message size in
    // the application to avoid unnecessary re-allocations and copying.
    return data.consume_entirely();
}

void length_prefix_codec::reset_buffer() {
    m_packet_size = -1;
    m_packet.clear();
}

data_buffer_ref length_prefix_codec::decode(data_buffer_ref &data) {
    if (!m_magic_received) {
        consume(data, int32_t(protocol::MAGIC_BYTES.size()));

        if (m_packet.size() < protocol::MAGIC_BYTES.size())
            return {};

        if (!std::equal(protocol::MAGIC_BYTES.begin(), protocol::MAGIC_BYTES.end(), m_packet.begin(), m_packet.end()))
            throw ignite_error("Unknown protocol response");

        reset_buffer();
        m_magic_received = true;
    }

    if (m_packet.empty() || m_packet.size() == (PACKET_HEADER_SIZE + m_packet_size))
        reset_buffer();

    if (m_packet_size < 0) {
        consume(data, PACKET_HEADER_SIZE);

        if (m_packet.size() < PACKET_HEADER_SIZE)
            return {};

        m_packet_size = detail::bytes::load<detail::endian::BIG, int32_t>(m_packet.data());
    }

    consume(data, m_packet_size + PACKET_HEADER_SIZE);

    if (m_packet.size() == m_packet_size + PACKET_HEADER_SIZE)
        return {m_packet, PACKET_HEADER_SIZE, m_packet_size + PACKET_HEADER_SIZE};

    return {};
}

void length_prefix_codec::consume(data_buffer_ref &data, size_t desired) {
    auto to_copy = desired - m_packet.size();
    if (to_copy <= 0)
        return;

    data.consume_by(m_packet, size_t(to_copy));
}

} // namespace ignite::network
