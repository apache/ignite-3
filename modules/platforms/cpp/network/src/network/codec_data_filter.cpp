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

#include <ignite/network/codec_data_filter.h>

namespace ignite::network
{

CodecDataFilter::CodecDataFilter(std::shared_ptr<Factory<Codec>> factory) :
    m_codecFactory(std::move(factory)),
    m_codecs(),
    m_codecsMutex() { }

bool CodecDataFilter::send(uint64_t id, std::vector<std::byte>&& data)
{
    std::shared_ptr<Codec> codec = FindCodec(id);
    if (!codec)
        return false;

    DataBufferOwning data0(std::move(data));
    while (true) {
        auto out = codec->encode(data0);
        if (out.isEmpty())
            break;

        bool res = DataFilterAdapter::send(id, std::move(out).extractData());
        if (!res)
            return res;
    }

    return true;
}

void CodecDataFilter::onConnectionSuccess(const EndPoint &addr, uint64_t id)
{
    {
        std::lock_guard<std::mutex> lock(m_codecsMutex);

        std::shared_ptr<Codec> codec(m_codecFactory->build());
        m_codecs.insert(std::make_pair(id, codec));
    }

    DataFilterAdapter::onConnectionSuccess(addr, id);
}

void CodecDataFilter::onConnectionClosed(uint64_t id, std::optional<IgniteError> err)
{
    {
        std::lock_guard<std::mutex> lock(m_codecsMutex);

        m_codecs.erase(id);
    }

    DataFilterAdapter::onConnectionClosed(id, std::move(err));
}

void CodecDataFilter::onMessageReceived(uint64_t id, BytesView msg)
{
    std::shared_ptr<Codec> codec = FindCodec(id);
    if (!codec)
        return;

    DataBufferRef msg0(msg);
    while (true)
    {
        DataBufferRef out = codec->decode(msg0);

        if (out.isEmpty())
            break;

        DataFilterAdapter::onMessageReceived(id, out.getBytesView());
    }
}

std::shared_ptr<Codec> CodecDataFilter::FindCodec(uint64_t id)
{
    std::lock_guard<std::mutex> lock(m_codecsMutex);

    auto it = m_codecs.find(id);
    if (it == m_codecs.end())
        return {};

    return it->second;
}

} // namespace ignite::network
