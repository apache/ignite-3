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

#pragma once

#include <vector>
#include <cstddef>

#include <common/ignite_error.h>

#include <ignite/network/codec.h>

namespace ignite::network
{

/**
 * Codec that decodes messages prefixed with int32 length.
 */
class LengthPrefixCodec : public Codec
{
public:
    /** Packet header size in bytes. */
    static constexpr size_t PACKET_HEADER_SIZE = 4;

    // Default
    ~LengthPrefixCodec() override = default;

    /**
     * Constructor.
     */
    LengthPrefixCodec();

    /**
     * Encode provided data.
     *
     * @param data Data to encode.
     * @return Encoded data. Returning null is ok.
     *
     * @throw IgniteError on error.
     */
    DataBufferOwning encode(DataBufferOwning& data) override;

    /**
     * Decode provided data.
     *
     * @param data Data to decode.
     * @return Decoded data. Returning null means data is not yet ready.
     *
     * @throw IgniteError on error.
     */
    DataBufferRef decode(DataBufferRef& data) override;

private:
    /**
     * Consume the right amount of provided data to make packet closer to desired size.
     *
     * @param data Data to consume.
     * @param desired Desired resulting size of packet.
     */
    void consume(DataBufferRef& data, size_t desired);

    /**
     * Reset packet buffer.
     */
    void resetBuffer();

    /** Size of the current packet. */
    int32_t m_packetSize;

    /** Current packet. */
    std::vector<std::byte> m_packet;

    /** Magic bytes received. */
    bool m_magicReceived;
};

/**
 * Factory for LengthPrefixCodec.
 */
class LengthPrefixCodecFactory : public Factory<Codec>
{
public:
    // Default
    LengthPrefixCodecFactory() = default;

    /**
     * Build instance.
     *
     * @return New instance of type @c T.
     */
    std::unique_ptr<Codec> build() override
    {
        return std::make_unique<LengthPrefixCodec>();
    }
};

} // namespace ignite::network
