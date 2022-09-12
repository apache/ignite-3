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

#include "network/sockets.h"
#include "network/win_async_client.h"
#include "network/win_async_client_pool.h"
#include "network/win_async_worker_thread.h"

// Using NULLs as specified by WinAPI
#ifdef __JETBRAINS_IDE__
#   pragma ide diagnostic ignored "modernize-use-nullptr"
#endif

namespace ignite::network
{

WinAsyncWorkerThread::WinAsyncWorkerThread() :
    m_thread(),
    m_stopping(false),
    m_clientPool(nullptr),
    m_iocp(NULL) { }

void WinAsyncWorkerThread::start(WinAsyncClientPool& clientPool0, HANDLE iocp0)
{
    assert(iocp0 != NULL);
    m_iocp = iocp0;
    m_clientPool = &clientPool0;

    m_thread = std::thread(&WinAsyncWorkerThread::run, this);
}

void WinAsyncWorkerThread::run()
{
    assert(m_clientPool != nullptr);

    while (!m_stopping)
    {
        DWORD bytesTransferred = 0;
        ULONG_PTR key = NULL;
        LPOVERLAPPED overlapped = NULL;

        BOOL ok = GetQueuedCompletionStatus(m_iocp, &bytesTransferred, &key, &overlapped, INFINITE);

        if (m_stopping)
            break;

        if (!key)
            continue;

        auto client = reinterpret_cast<WinAsyncClient*>(key);

        if (!ok || (NULL != overlapped && 0 == bytesTransferred))
        {
            m_clientPool->closeAndRelease(client->GetId(), nullptr);

            continue;
        }

        if (!overlapped)
        {
            // This mean new client is connected.
            m_clientPool->handleConnectionSuccess(client->getAddress(), client->GetId());

            bool success = client->receive();
            if (!success)
                m_clientPool->closeAndRelease(client->GetId(), nullptr);

            continue;
        }

        try
        {
            auto operation = reinterpret_cast<IoOperation*>(overlapped);
            switch (operation->kind)
            {
                case IoOperationKind::SEND:
                {
                    bool success = client->processSent(bytesTransferred);

                    if (!success)
                        m_clientPool->closeAndRelease(client->GetId(), nullptr);

                    m_clientPool->handleMessageSent(client->GetId());

                    break;
                }

                case IoOperationKind::RECEIVE:
                {
                    DataBuffer data = client->processReceived(bytesTransferred);

                    if (!data.isEmpty())
                        m_clientPool->handleMessageReceived(client->GetId(), data);

                    bool success = client->receive();

                    if (!success)
                        m_clientPool->closeAndRelease(client->GetId(), nullptr);

                    break;
                }

                default:
                    break;
            }
        }
        catch (const IgniteError& err)
        {
            m_clientPool->closeAndRelease(client->GetId(), &err);
        }
    }
}

void WinAsyncWorkerThread::stop()
{
    if (m_stopping)
        return;

    m_stopping = true;

    PostQueuedCompletionStatus(m_iocp, 0, 0, NULL);

    m_thread.join();
}

} // namespace ignite::network

