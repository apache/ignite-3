/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "ignite/protocol/utils.h"

#include <ignite/network/length_prefix_codec.h>

namespace ignite::network
{

LengthPrefixCodec::LengthPrefixCodec() :
    m_packetSize(-1),
    m_packet(),
    m_magicReceived(false) { }

DataBuffer LengthPrefixCodec::encode(DataBuffer& data)
{
    // Just pass data as is, because we encode message size in
    // the application to avoid unnecessary re-allocations and copying.
    return data.consumeEntirely();
}

void LengthPrefixCodec::resetBuffer()
{
    m_packetSize = -1;
    m_packet.clear();
}

DataBuffer LengthPrefixCodec::decode(DataBuffer& data)
{
    if (!m_magicReceived)
    {
        consume(data, protocol::MAGIC_BYTES.size());

        if (m_packet.size() < protocol::MAGIC_BYTES.size())
            return {};

        if (!std::equal(protocol::MAGIC_BYTES.begin(), protocol::MAGIC_BYTES.end(), m_packet.begin(), m_packet.end()))
            throw IgniteError("Unknown protocol response");

        resetBuffer();
        m_magicReceived = true;
    }

    if (m_packet.empty() || m_packet.size() == (PACKET_HEADER_SIZE + m_packetSize))
        resetBuffer();

    if (m_packetSize < 0)
    {
        consume(data, PACKET_HEADER_SIZE);

        if (m_packet.size() < PACKET_HEADER_SIZE)
            return {};

        m_packetSize = protocol::readInt32(m_packet.data());
    }

    consume(data, m_packetSize + PACKET_HEADER_SIZE);

    if (m_packet.size() == m_packetSize + PACKET_HEADER_SIZE)
        return {std::make_shared<protocol::Buffer>(m_packet), PACKET_HEADER_SIZE, m_packetSize};

    return {};
}

void LengthPrefixCodec::consume(DataBuffer &data, int32_t desired)
{
    int32_t toCopy = desired - int32_t(m_packet.size());
    if (toCopy <= 0)
        return;

    if (data.getSize() < toCopy)
        toCopy = data.getSize();

    data.beConsumed(m_packet, toCopy);
}

} // namespace ignite::network
