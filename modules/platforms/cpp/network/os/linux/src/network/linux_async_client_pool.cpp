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

#include <ignite/common/utils.h>
#include <network/utils.h>

#include "network/sockets.h"
#include "network/linux_async_client_pool.h"

namespace ignite
{
    namespace network
    {
        LinuxAsyncClientPool::LinuxAsyncClientPool() :
            m_stopping(true),
            m_asyncHandler(0),
            m_workerThread(*this),
            m_idGen(0),
            m_clientsMutex(),
            m_clientIdMap()
        {
            // No-op.
        }

        LinuxAsyncClientPool::~LinuxAsyncClientPool()
        {
            InternalStop();
        }

        void LinuxAsyncClientPool::start(const std::vector<TcpRange> &addrs, uint32_t connLimit)
        {
            if (!m_stopping)
                throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Client pool is already started");

            m_idGen = 0;
            m_stopping = false;

            try
            {
                m_workerThread.Start0(connLimit, addrs);
            }
            catch (...)
            {
                stop();

                throw;
            }
        }

        void LinuxAsyncClientPool::stop()
        {
            InternalStop();
        }

        bool LinuxAsyncClientPool::send(uint64_t id, const DataBuffer &data)
        {
            if (m_stopping)
                return false;

            SP_LinuxAsyncClient client = FindClient(id);
            if (!client.IsValid())
                return false;

            return client->send(data);
        }

        void LinuxAsyncClientPool::close(uint64_t id, const IgniteError *err)
        {
            if (m_stopping)
                return;

            SP_LinuxAsyncClient client = FindClient(id);
            if (client.IsValid() && !client->isClosed())
                client->shutdown(err);
        }

        void LinuxAsyncClientPool::closeAndRelease(uint64_t id, const IgniteError *err)
        {
            if (m_stopping)
                return;

            SP_LinuxAsyncClient client;
            {
                common::concurrent::CsLockGuard lock(m_clientsMutex);

                std::map<uint64_t, SP_LinuxAsyncClient>::iterator it = m_clientIdMap.find(id);
                if (it == m_clientIdMap.end())
                    return;

                client = it->second;

                m_clientIdMap.erase(it);
            }

            bool closed = client->close();
            if (closed)
            {
                IgniteError err0(client->getCloseError());
                if (err0.GetCode() == IgniteError::IGNITE_SUCCESS)
                    err0 = IgniteError(IgniteError::IGNITE_ERR_NETWORK_FAILURE, "Connection closed by server");

                if (!err)
                    err = &err0;

                HandleConnectionClosed(id, err);
            }
        }

        bool LinuxAsyncClientPool::addClient(SP_LinuxAsyncClient &client)
        {
            if (m_stopping)
                return false;

            LinuxAsyncClient& clientRef = *client.Get();
            {
                common::concurrent::CsLockGuard lock(m_clientsMutex);

                uint64_t id = ++m_idGen;
                clientRef.setId(id);

                m_clientIdMap[id] = client;
            }

            handleConnectionSuccess(clientRef.getAddress(), clientRef.GetId());

            return true;
        }

        void LinuxAsyncClientPool::handleConnectionError(const EndPoint &addr, const IgniteError &err)
        {
            AsyncHandler* asyncHandler0 = m_asyncHandler;
            if (asyncHandler0)
                asyncHandler0->onConnectionError(addr, err);
        }

        void LinuxAsyncClientPool::handleConnectionSuccess(const EndPoint &addr, uint64_t id)
        {
            AsyncHandler* asyncHandler0 = m_asyncHandler;
            if (asyncHandler0)
                asyncHandler0->onConnectionSuccess(addr, id);
        }

        void LinuxAsyncClientPool::HandleConnectionClosed(uint64_t id, const IgniteError *err)
        {
            AsyncHandler* asyncHandler0 = m_asyncHandler;
            if (asyncHandler0)
                asyncHandler0->onConnectionClosed(id, err);
        }

        void LinuxAsyncClientPool::handleMessageReceived(uint64_t id, const DataBuffer &msg)
        {
            AsyncHandler* asyncHandler0 = m_asyncHandler;
            if (asyncHandler0)
                asyncHandler0->onMessageReceived(id, msg);
        }

        void LinuxAsyncClientPool::handleMessageSent(uint64_t id)
        {
            AsyncHandler* asyncHandler0 = m_asyncHandler;
            if (asyncHandler0)
                asyncHandler0->onMessageSent(id);
        }

        void LinuxAsyncClientPool::InternalStop()
        {
            m_stopping = true;
            m_workerThread.stop();

            {
                common::concurrent::CsLockGuard lock(m_clientsMutex);

                std::map<uint64_t, SP_LinuxAsyncClient>::iterator it;
                for (it = m_clientIdMap.begin(); it != m_clientIdMap.end(); ++it)
                {
                    LinuxAsyncClient& client = *it->second.Get();

                    IgniteError err(IgniteError::IGNITE_ERR_GENERIC, "Client stopped");
                    HandleConnectionClosed(client.GetId(), &err);
                }

                m_clientIdMap.clear();
            }
        }

        SP_LinuxAsyncClient LinuxAsyncClientPool::FindClient(uint64_t id) const
        {
            common::concurrent::CsLockGuard lock(m_clientsMutex);

            std::map<uint64_t, SP_LinuxAsyncClient>::const_iterator it = m_clientIdMap.find(id);
            if (it == m_clientIdMap.end())
                return SP_LinuxAsyncClient();

            return it->second;
        }
    }
}
