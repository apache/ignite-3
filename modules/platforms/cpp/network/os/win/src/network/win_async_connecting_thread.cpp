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

#include "network/utils.h"

#include "network/sockets.h"
#include "network/win_async_client_pool.h"
#include "network/win_async_connecting_thread.h"

// Using NULLs as specified by WinAPI
#ifdef __JETBRAINS_IDE__
#   pragma ide diagnostic ignored "modernize-use-nullptr"
#endif

namespace
{
    ignite::network::FibonacciSequence<10> fibonacci10;
}

namespace ignite::network
{

WinAsyncConnectingThread::WinAsyncConnectingThread() :
    m_thread(),
    m_clientPool(nullptr),
    m_stopping(false),
    m_failedAttempts(0),
    m_minAddrs(0),
    m_addrsMutex(),
    m_connectNeeded(),
    m_nonConnected() { }

void WinAsyncConnectingThread::run()
{
    assert(m_clientPool != nullptr);

    while (!m_stopping)
    {
        TcpRange range = getRandomAddress();

        if (m_stopping || range.isEmpty())
            break;

        std::shared_ptr<WinAsyncClient> client = tryConnect(range);

        if (!client)
        {
            ++m_failedAttempts;

            auto msToWait = static_cast<DWORD>(1000 * fibonacci10.getValue(m_failedAttempts));
            if (msToWait)
                Sleep(msToWait);

            continue;
        }

        m_failedAttempts = 0;

        if (m_stopping)
        {
            client->close();

            return;
        }

        try
        {
            bool added = m_clientPool->addClient(client);

            if (!added)
            {
                client->close();

                continue;
            }

            {
                std::lock_guard<std::mutex> lock(m_addrsMutex);

                auto it = std::find(m_nonConnected.begin(), m_nonConnected.end(), range);
                if (it != m_nonConnected.end())
                    m_nonConnected.erase(it);
            }
        }
        catch (const IgniteError& err)
        {
            client->close();

            m_clientPool->handleConnectionError(client->getAddress(), err);

            continue;
        }
    }
}

void WinAsyncConnectingThread::notifyFreeAddress(const TcpRange &range)
{
    std::lock_guard<std::mutex> lock(m_addrsMutex);

    m_nonConnected.push_back(range);
    m_connectNeeded.notify_one();
}

void WinAsyncConnectingThread::start(WinAsyncClientPool& clientPool, size_t limit, std::vector<TcpRange> addrs)
{
    m_stopping = false;
    m_clientPool = &clientPool;
    m_failedAttempts = 0;
    m_nonConnected = std::move(addrs);

    if (!limit || limit > m_nonConnected.size())
        m_minAddrs = 0;
    else
        m_minAddrs = m_nonConnected.size() - limit;

    m_thread = std::thread(&WinAsyncConnectingThread::run, this);
}

void WinAsyncConnectingThread::stop()
{
    if (m_stopping)
        return;

    m_stopping = true;

    {
        std::lock_guard<std::mutex> lock(m_addrsMutex);
        m_connectNeeded.notify_one();
    }

    m_thread.join();
    m_nonConnected.clear();
}

std::shared_ptr<WinAsyncClient> WinAsyncConnectingThread::tryConnect(const TcpRange& range)
{
    for (uint16_t port = range.port; port <= (range.port + range.range); ++port)
    {
        EndPoint addr(range.host, port);
        try
        {
            SOCKET socket = tryConnect(addr);

            return std::make_shared<WinAsyncClient>(socket, addr, range, int32_t(BUFFER_SIZE));
        }
        catch (const IgniteError& err)
        {
            m_clientPool->handleConnectionError(addr, err);
        }
    }

    return {};
}

SOCKET WinAsyncConnectingThread::tryConnect(const EndPoint& addr)
{
    addrinfo hints{};
    memset(&hints, 0, sizeof(hints));

    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    std::stringstream converter;
    converter << addr.port;
    std::string strPort = converter.str();

    // Resolve the server address and port
    addrinfo *result = NULL;
    int res = getaddrinfo(addr.host.c_str(), strPort.c_str(), &hints, &result);

    if (res != 0)
        throwNetworkError("Can not resolve host: " + addr.host + ":" + strPort);

    std::string lastErrorMsg = "Failed to resolve host";

    SOCKET socket = INVALID_SOCKET; // NOLINT(modernize-use-auto)

    // Attempt to connect to an address until one succeeds
    for (addrinfo *it = result; it != NULL; it = it->ai_next)
    {
        lastErrorMsg = "Failed to establish connection with the host";

        socket = WSASocket(it->ai_family, it->ai_socktype, it->ai_protocol, NULL, 0, WSA_FLAG_OVERLAPPED);

        if (socket == INVALID_SOCKET)
            throwNetworkError("Socket creation failed: " + getLastSocketErrorMessage());

        TrySetSocketOptions(socket, BUFFER_SIZE, TRUE, TRUE, TRUE);

        // Connect to server.
        res = WSAConnect(socket, it->ai_addr, static_cast<int>(it->ai_addrlen), NULL, NULL, NULL, NULL);
        if (SOCKET_ERROR == res)
        {
            closesocket(socket);
            socket = INVALID_SOCKET;

            int lastError = WSAGetLastError();

            if (lastError != WSAEWOULDBLOCK)
            {
                lastErrorMsg.append(": ").append(getSocketErrorMessage(lastError));

                continue;
            }
        }

        break;
    }

    freeaddrinfo(result);

    if (socket == INVALID_SOCKET)
        throwNetworkError(lastErrorMsg);

    return socket;
}

TcpRange WinAsyncConnectingThread::getRandomAddress() const
{
    std::unique_lock<std::mutex> lock(m_addrsMutex);

    if (m_stopping)
        return {};

    while (m_nonConnected.size() <= m_minAddrs)
    {
        m_connectNeeded.wait(lock);

        if (m_stopping)
            return {};
    }

    // TODO: Re-write to round-robin
    size_t idx = rand() % m_nonConnected.size();
    TcpRange range = m_nonConnected.at(idx);

    return range;
}

} // namespace ignite::network
