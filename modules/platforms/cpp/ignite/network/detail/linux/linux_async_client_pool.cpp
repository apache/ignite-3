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

#include "linux_async_client_pool.h"

#include "../utils.h"
#include "sockets.h"

#include <algorithm>

namespace ignite::network::detail {

LinuxAsyncClientPool::LinuxAsyncClientPool()
    : m_stopping(true)
    , m_asyncHandler()
    , m_workerThread(*this)
    , m_idGen(0)
    , m_clientsMutex()
    , m_clientIdMap() {
}

LinuxAsyncClientPool::~LinuxAsyncClientPool() {
    internalStop();
}

void LinuxAsyncClientPool::start(const std::vector<TcpRange> addrs, uint32_t connLimit) {
    if (!m_stopping)
        throw ignite_error("Client pool is already started");

    m_idGen = 0;
    m_stopping = false;

    try {
        m_workerThread.start(connLimit, addrs);
    } catch (...) {
        stop();

        throw;
    }
}

void LinuxAsyncClientPool::stop() {
    internalStop();
}

bool LinuxAsyncClientPool::send(uint64_t id, std::vector<std::byte> &&data) {
    if (m_stopping)
        return false;

    auto client = find_client(id);
    if (!client)
        return false;

    return client->send(std::move(data));
}

void LinuxAsyncClientPool::close(uint64_t id, std::optional<ignite_error> err) {
    if (m_stopping)
        return;

    std::shared_ptr<LinuxAsyncClient> client = find_client(id);
    if (client && !client->isClosed())
        client->shutdown(std::move(err));
}

void LinuxAsyncClientPool::closeAndRelease(uint64_t id, std::optional<ignite_error> err) {
    if (m_stopping)
        return;

    std::shared_ptr<LinuxAsyncClient> client;
    {
        std::lock_guard<std::mutex> lock(m_clientsMutex);

        auto it = m_clientIdMap.find(id);
        if (it == m_clientIdMap.end())
            return;

        client = it->second;

        m_clientIdMap.erase(it);
    }

    bool closed = client->close();
    if (closed) {
        ignite_error err0(client->getCloseError());
        if (err0.get_status_code() == status_code::SUCCESS)
            err0 = ignite_error(status_code::NETWORK, "Connection closed by server");

        if (!err)
            err = std::move(err0);

        handleConnectionClosed(id, err);
    }
}

bool LinuxAsyncClientPool::addClient(std::shared_ptr<LinuxAsyncClient> client) {
    if (m_stopping)
        return false;

    auto clientAddr = client->getAddress();
    uint64_t clientId;
    {
        std::lock_guard<std::mutex> lock(m_clientsMutex);

        clientId = ++m_idGen;
        client->setId(clientId);

        m_clientIdMap[clientId] = std::move(client);
    }

    handleConnectionSuccess(clientAddr, clientId);

    return true;
}

void LinuxAsyncClientPool::handleConnectionError(const EndPoint &addr, ignite_error err) {
    auto asyncHandler0 = m_asyncHandler.lock();
    if (asyncHandler0)
        asyncHandler0->on_connection_error(addr, std::move(err));
}

void LinuxAsyncClientPool::handleConnectionSuccess(const EndPoint &addr, uint64_t id) {
    auto asyncHandler0 = m_asyncHandler.lock();
    if (asyncHandler0)
        asyncHandler0->on_connection_success(addr, id);
}

void LinuxAsyncClientPool::handleConnectionClosed(uint64_t id, std::optional<ignite_error> err) {
    auto asyncHandler0 = m_asyncHandler.lock();
    if (asyncHandler0)
        asyncHandler0->on_connection_closed(id, std::move(err));
}

void LinuxAsyncClientPool::handleMessageReceived(uint64_t id, bytes_view msg) {
    auto asyncHandler0 = m_asyncHandler.lock();
    if (asyncHandler0)
        asyncHandler0->on_message_received(id, msg);
}

void LinuxAsyncClientPool::handleMessageSent(uint64_t id) {
    auto asyncHandler0 = m_asyncHandler.lock();
    if (asyncHandler0)
        asyncHandler0->on_message_sent(id);
}

void LinuxAsyncClientPool::internalStop() {
    m_stopping = true;
    m_workerThread.stop();

    {
        std::lock_guard<std::mutex> lock(m_clientsMutex);

        for (auto [_, client] : m_clientIdMap) {
            ignite_error err("Client stopped");
            handleConnectionClosed(client->id(), err);
        }

        m_clientIdMap.clear();
    }
}

std::shared_ptr<LinuxAsyncClient> LinuxAsyncClientPool::find_client(uint64_t id) const {
    std::lock_guard<std::mutex> lock(m_clientsMutex);

    auto it = m_clientIdMap.find(id);
    if (it == m_clientIdMap.end())
        return {};

    return it->second;
}

} // namespace ignite::network::detail
