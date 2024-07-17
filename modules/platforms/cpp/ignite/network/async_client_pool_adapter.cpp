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

#include "async_client_pool_adapter.h"

#include "error_handling_filter.h"

namespace ignite::network {

async_client_pool_adapter::async_client_pool_adapter(data_filters filters, std::shared_ptr<async_client_pool> pool)
    : m_filters(std::move(filters))
    , m_pool(std::move(pool))
    , m_sink(m_pool.get()) {
    m_filters.insert(m_filters.begin(), std::make_shared<error_handling_filter>());

    for (const auto &filter : m_filters) {
        filter->set_sink(m_sink);
        m_sink = filter.get();
    }
}

void async_client_pool_adapter::start(std::vector<tcp_range> addrs, uint32_t connLimit) {
    m_pool->start(std::move(addrs), connLimit);
}

void async_client_pool_adapter::stop() {
    m_pool->stop();
}

void async_client_pool_adapter::set_handler(std::weak_ptr<async_handler> handler) {
    auto handler0 = std::move(handler);
    for (auto it = m_filters.rbegin(); it != m_filters.rend(); ++it) {
        (*it)->set_handler(std::move(handler0));
        handler0 = *it;
    }

    m_pool->set_handler(std::move(handler0));
}

bool async_client_pool_adapter::send(uint64_t id, std::vector<std::byte> &&data) {
    return m_sink->send(id, std::move(data));
}

void async_client_pool_adapter::close(uint64_t id, std::optional<ignite_error> err) {
    m_sink->close(id, std::move(err));
}

} // namespace ignite::network
