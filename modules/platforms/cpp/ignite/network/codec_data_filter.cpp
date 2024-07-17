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

#include "codec_data_filter.h"

namespace ignite::network {

codec_data_filter::codec_data_filter(std::shared_ptr<detail::factory<codec>> factory)
    : m_codec_factory(std::move(factory))
    , m_codecs()
    , m_codecs_mutex() {
}

bool codec_data_filter::send(uint64_t id, std::vector<std::byte> &&data) {
    std::shared_ptr<codec> codec = find_codec(id);
    if (!codec)
        return false;

    data_buffer_owning data0(std::move(data));
    while (true) {
        auto out = codec->encode(data0);
        if (out.empty())
            break;

        bool res = data_filter_adapter::send(id, std::move(out).extract_data());
        if (!res)
            return res;
    }

    return true;
}

void codec_data_filter::on_connection_success(const end_point &addr, uint64_t id) {
    {
        std::lock_guard<std::mutex> lock(m_codecs_mutex);

        std::shared_ptr<codec> codec(m_codec_factory->build());
        m_codecs.insert(std::make_pair(id, codec));
    }

    data_filter_adapter::on_connection_success(addr, id);
}

void codec_data_filter::on_connection_closed(uint64_t id, std::optional<ignite_error> err) {
    {
        std::lock_guard<std::mutex> lock(m_codecs_mutex);

        m_codecs.erase(id);
    }

    data_filter_adapter::on_connection_closed(id, std::move(err));
}

void codec_data_filter::on_message_received(uint64_t id, bytes_view msg) {
    std::shared_ptr<codec> codec = find_codec(id);
    if (!codec)
        return;

    data_buffer_ref msg0(msg);
    while (true) {
        data_buffer_ref out = codec->decode(msg0);

        if (out.empty())
            break;

        data_filter_adapter::on_message_received(id, out.get_bytes_view());
    }
}

std::shared_ptr<codec> codec_data_filter::find_codec(uint64_t id) {
    std::lock_guard<std::mutex> lock(m_codecs_mutex);

    auto it = m_codecs.find(id);
    if (it == m_codecs.end())
        return {};

    return it->second;
}

} // namespace ignite::network
