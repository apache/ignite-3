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
#include "network/win_async_client_pool.h"

// Using NULLs as specified by WinAPI
#ifdef __JETBRAINS_IDE__
#   pragma ide diagnostic ignored "modernize-use-nullptr"
#endif

namespace ignite::network
{

WinAsyncClientPool::WinAsyncClientPool() :
    m_stopping(true),
    m_asyncHandler(),
    m_connectingThread(),
    m_workerThread(),
    m_idGen(0),
    m_iocp(NULL),
    m_clientsMutex(),
    m_clientIdMap() { }

WinAsyncClientPool::~WinAsyncClientPool()
{
    internalStop();
}

void WinAsyncClientPool::start(std::vector<TcpRange> addrs, uint32_t connLimit)
{
    if (!m_stopping)
        throw IgniteError(StatusCode::GENERIC, "Client pool is already started");

    m_stopping = false;

    InitWsa();

    m_iocp = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);
    if (!m_iocp)
        throwLastSystemError("Failed to create IOCP instance");

    try
    {
        m_connectingThread.start(*this, connLimit, std::move(addrs));
        m_workerThread.start(*this, m_iocp);
    }
    catch (...)
    {
        stop();

        throw;
    }
}

void WinAsyncClientPool::stop()
{
    internalStop();
}

void WinAsyncClientPool::internalStop()
{
    m_stopping = true;
    m_connectingThread.stop();

    {
        std::lock_guard<std::mutex> lock(m_clientsMutex);

        for (auto it = m_clientIdMap.begin(); it != m_clientIdMap.end(); ++it)
        {
            WinAsyncClient& client = *it->second;

            client.shutdown(nullptr);
            client.close();
        }
    }

    m_workerThread.stop();

    CloseHandle(m_iocp);
    m_iocp = NULL;

    m_clientIdMap.clear();
}

bool WinAsyncClientPool::addClient(const std::shared_ptr<WinAsyncClient>& client)
{
    uint64_t id;
    {
        std::lock_guard<std::mutex> lock(m_clientsMutex);

        if (m_stopping)
            return false;

        id = ++m_idGen;
        client->setId(id);

        HANDLE iocp0 = client->addToIocp(m_iocp);
        if (iocp0 == NULL)
            throwLastSystemError("Can not add socket to IOCP");

        m_iocp = iocp0;

        m_clientIdMap[id] = client;
    }

    PostQueuedCompletionStatus(m_iocp, 0, reinterpret_cast<ULONG_PTR>(client.get()), NULL);

    return true;
}

void WinAsyncClientPool::handleConnectionError(const EndPoint &addr, const IgniteError &err)
{
    auto asyncHandler0 = m_asyncHandler.lock();
    if (asyncHandler0)
        asyncHandler0->onConnectionError(addr, err);
}

void WinAsyncClientPool::handleConnectionSuccess(const EndPoint &addr, uint64_t id)
{
    auto asyncHandler0 = m_asyncHandler.lock();
    if (asyncHandler0)
        asyncHandler0->onConnectionSuccess(addr, id);
}

void WinAsyncClientPool::handleConnectionClosed(uint64_t id, const IgniteError *err)
{
    auto asyncHandler0 = m_asyncHandler.lock();
    if (asyncHandler0)
        asyncHandler0->onConnectionClosed(id, err);
}

void WinAsyncClientPool::handleMessageReceived(uint64_t id, const DataBuffer &msg)
{
    auto asyncHandler0 = m_asyncHandler.lock();
    if (asyncHandler0)
        asyncHandler0->onMessageReceived(id, msg);
}

void WinAsyncClientPool::handleMessageSent(uint64_t id)
{
    auto asyncHandler0 = m_asyncHandler.lock();
    if (asyncHandler0)
        asyncHandler0->onMessageSent(id);
}

bool WinAsyncClientPool::send(uint64_t id, const DataBuffer& data)
{
    if (m_stopping)
        return false;

    auto client = findClient(id);
    if (!client)
        return false;

    return client->send(data);
}

void WinAsyncClientPool::closeAndRelease(uint64_t id, const IgniteError* err)
{
    std::shared_ptr<WinAsyncClient> client;
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
        m_connectingThread.notifyFreeAddress(client->getRange());

        IgniteError err0(client->getCloseError());
        if (err0.getStatusCode() == StatusCode::SUCCESS)
            err0 = IgniteError(StatusCode::NETWORK, "Connection closed by server");

        if (!err)
            err = &err0;

        handleConnectionClosed(id, err);
    }
}

void WinAsyncClientPool::close(uint64_t id, const IgniteError* err)
{
    auto client = findClient(id);
    if (client && !client->isClosed())
        client->shutdown(err);
}

std::shared_ptr<WinAsyncClient> WinAsyncClientPool::findClient(uint64_t id) const
{
    std::lock_guard<std::mutex> lock(m_clientsMutex);

    return findClientLocked(id);
}

std::shared_ptr<WinAsyncClient> WinAsyncClientPool::findClientLocked(uint64_t id) const
{
    auto it = m_clientIdMap.find(id);
    if (it == m_clientIdMap.end())
        return {};

    return it->second;
}

void WinAsyncClientPool::setHandler(std::weak_ptr<AsyncHandler> handler)
{
    m_asyncHandler = handler;
}

} // namespace ignite::network
