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

#include <algorithm>

#include <network/utils.h>

#include "network/sockets.h"
#include "network/linux_async_client_pool.h"

namespace ignite::network
{

LinuxAsyncClientPool::LinuxAsyncClientPool() :
    m_stopping(true),
    m_asyncHandler(),
    m_workerThread(*this),
    m_idGen(0),
    m_clientsMutex(),
    m_clientIdMap() { }

LinuxAsyncClientPool::~LinuxAsyncClientPool()
{
    internalStop();
}

void LinuxAsyncClientPool::start(const std::vector<TcpRange> addrs, uint32_t connLimit)
{
    if (!m_stopping)
        throw IgniteError("Client pool is already started");

    m_idGen = 0;
    m_stopping = false;

    try
    {
        m_workerThread.start(connLimit, addrs);
    }
    catch (...)
    {
        stop();

        throw;
    }
}

void LinuxAsyncClientPool::stop()
{
    internalStop();
}

bool LinuxAsyncClientPool::send(uint64_t id, const DataBuffer &data)
{
    if (m_stopping)
        return false;

    auto client = findClient(id);
    if (!client)
        return false;

    return client->send(data);
}

void LinuxAsyncClientPool::close(uint64_t id, std::optional<IgniteError> err)
{
    if (m_stopping)
        return;

    std::shared_ptr<LinuxAsyncClient> client = findClient(id);
    if (client && !client->isClosed())
        client->shutdown(std::move(err));
}

void LinuxAsyncClientPool::closeAndRelease(uint64_t id, std::optional<IgniteError> err)
{
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
    if (closed)
    {
        IgniteError err0(client->getCloseError());
        if (err0.getStatusCode() == StatusCode::SUCCESS)
            err0 = IgniteError(StatusCode::NETWORK, "Connection closed by server");

        if (!err)
            err = std::move(err0);

        handleConnectionClosed(id, err);
    }
}

bool LinuxAsyncClientPool::addClient(std::shared_ptr<LinuxAsyncClient> client)
{
    if (m_stopping)
        return false;

    auto clientAddr = client->getAddress();
    auto clientId = client->getId();
    {
        std::lock_guard<std::mutex> lock(m_clientsMutex);

        uint64_t id = ++m_idGen;
        client->setId(id);

        m_clientIdMap[id] = std::move(client);
    }

    handleConnectionSuccess(clientAddr, clientId);

    return true;
}

void LinuxAsyncClientPool::handleConnectionError(const EndPoint &addr, IgniteError err)
{
    auto asyncHandler0 = m_asyncHandler.lock();
    if (asyncHandler0)
        asyncHandler0->onConnectionError(addr, std::move(err));
}

void LinuxAsyncClientPool::handleConnectionSuccess(const EndPoint &addr, uint64_t id)
{
    auto asyncHandler0 = m_asyncHandler.lock();
    if (asyncHandler0)
        asyncHandler0->onConnectionSuccess(addr, id);
}

void LinuxAsyncClientPool::handleConnectionClosed(uint64_t id, std::optional<IgniteError> err)
{
    auto asyncHandler0 = m_asyncHandler.lock();
    if (asyncHandler0)
        asyncHandler0->onConnectionClosed(id, std::move(err));
}

void LinuxAsyncClientPool::handleMessageReceived(uint64_t id, const DataBuffer &msg)
{
    auto asyncHandler0 = m_asyncHandler.lock();
    if (asyncHandler0)
        asyncHandler0->onMessageReceived(id, msg);
}

void LinuxAsyncClientPool::handleMessageSent(uint64_t id)
{
    auto asyncHandler0 = m_asyncHandler.lock();
    if (asyncHandler0)
        asyncHandler0->onMessageSent(id);
}

void LinuxAsyncClientPool::internalStop()
{
    m_stopping = true;
    m_workerThread.stop();

    {
        std::lock_guard<std::mutex> lock(m_clientsMutex);

        for (auto [_, client] : m_clientIdMap)
        {
            IgniteError err("Client stopped");
            handleConnectionClosed(client->getId(), err);
        }

        m_clientIdMap.clear();
    }
}

std::shared_ptr<LinuxAsyncClient> LinuxAsyncClientPool::findClient(uint64_t id) const
{
    std::lock_guard<std::mutex> lock(m_clientsMutex);

    auto it = m_clientIdMap.find(id);
    if (it == m_clientIdMap.end())
        return {};

    return it->second;
}

} // namespace ignite::network
