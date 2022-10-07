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

#include "win_async_worker_thread.h"

#include "sockets.h"
#include "win_async_client.h"
#include "win_async_client_pool.h"

#include <algorithm>
#include <cassert>

// Using NULLs as specified by WinAPI
#ifdef __JETBRAINS_IDE__
# pragma ide diagnostic ignored "modernize-use-nullptr"
#endif

namespace ignite::network::detail {

WinAsyncWorkerThread::WinAsyncWorkerThread()
    : m_thread()
    , m_stopping(false)
    , m_clientPool(nullptr)
    , m_iocp(NULL) {
}

void WinAsyncWorkerThread::start(WinAsyncClientPool &clientPool0, HANDLE iocp0) {
    assert(iocp0 != NULL);
    m_iocp = iocp0;
    m_clientPool = &clientPool0;

    m_thread = std::thread(&WinAsyncWorkerThread::run, this);
}

void WinAsyncWorkerThread::run() {
    assert(m_clientPool != nullptr);

    while (!m_stopping) {
        DWORD bytesTransferred = 0;
        ULONG_PTR key = NULL;
        LPOVERLAPPED overlapped = NULL;

        BOOL ok = GetQueuedCompletionStatus(m_iocp, &bytesTransferred, &key, &overlapped, INFINITE);

        if (m_stopping)
            break;

        if (!key)
            continue;

        auto client = reinterpret_cast<WinAsyncClient *>(key);

        if (!ok || (NULL != overlapped && 0 == bytesTransferred)) {
            m_clientPool->closeAndRelease(client->getId(), std::nullopt);

            continue;
        }

        if (!overlapped) {
            // This mean new client is connected.
            m_clientPool->handleConnectionSuccess(client->getAddress(), client->getId());

            bool success = client->receive();
            if (!success)
                m_clientPool->closeAndRelease(client->getId(), std::nullopt);

            continue;
        }

        try {
            auto operation = reinterpret_cast<IoOperation *>(overlapped);
            switch (operation->kind) {
                case IoOperationKind::SEND: {
                    bool success = client->processSent(bytesTransferred);

                    if (!success)
                        m_clientPool->closeAndRelease(client->getId(), std::nullopt);

                    m_clientPool->handleMessageSent(client->getId());

                    break;
                }

                case IoOperationKind::RECEIVE: {
                    auto data = client->processReceived(bytesTransferred);

                    if (!data.empty())
                        m_clientPool->handleMessageReceived(client->getId(), data);

                    bool success = client->receive();

                    if (!success)
                        m_clientPool->closeAndRelease(client->getId(), std::nullopt);

                    break;
                }

                default:
                    break;
            }
        } catch (const ignite_error &err) {
            m_clientPool->closeAndRelease(client->getId(), err);
        }
    }
}

void WinAsyncWorkerThread::stop() {
    if (m_stopping)
        return;

    m_stopping = true;

    PostQueuedCompletionStatus(m_iocp, 0, 0, NULL);

    m_thread.join();
}

} // namespace ignite::network::detail
