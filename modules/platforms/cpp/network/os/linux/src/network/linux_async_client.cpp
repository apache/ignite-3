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

#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <sys/epoll.h>
#include <unistd.h>

#include <algorithm>

#include <ignite/common/utils.h>
#include <network/utils.h>

#include "network/sockets.h"
#include "network/linux_async_client.h"

namespace ignite
{
    namespace network
    {
        LinuxAsyncClient::LinuxAsyncClient(int fd, const EndPoint &addr, const TcpRange &range) :
            m_state(State::CONNECTED),
            fd(fd),
            epoll(-1),
            id(0),
            addr(addr),
            range(range),
            m_sendPackets(),
            m_sendMutex(),
            m_recvPacket(),
            m_closeErr(IgniteError::IGNITE_SUCCESS)
        {
            // No-op.
        }

        LinuxAsyncClient::~LinuxAsyncClient()
        {
            shutdown(0);

            close();
        }

        bool LinuxAsyncClient::shutdown(const IgniteError* err)
        {
            common::concurrent::CsLockGuard lock(m_sendMutex);
            if (m_state != State::CONNECTED)
                return false;

            m_closeErr = err ? *err : IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Connection closed by application");
            shutdown(fd, SHUT_RDWR);
            m_state = State::SHUTDOWN;

            return true;
        }

        bool LinuxAsyncClient::close()
        {
            if (State::CLOSED == m_state)
                return false;

            StopMonitoring();
            close(fd);
            fd = -1;
            m_state = State::CLOSED;

            return true;
        }

        bool LinuxAsyncClient::send(const DataBuffer& data)
        {
            common::concurrent::CsLockGuard lock(m_sendMutex);

            m_sendPackets.push_back(data);

            if (m_sendPackets.size() > 1)
                return true;

            return sendNextPacketLocked();
        }

        bool LinuxAsyncClient::sendNextPacketLocked()
        {
            if (m_sendPackets.empty())
                return true;

            DataBuffer& packet = m_sendPackets.front();

            ssize_t ret = send(fd, packet.GetData(), packet.GetSize(), 0);
            if (ret < 0)
                return false;

            packet.Skip(static_cast<int32_t>(ret));

            EnableSendNotifications();

            return true;
        }

        DataBuffer LinuxAsyncClient::receive()
        {
            using namespace impl::interop;

            if (!m_recvPacket.IsValid())
            {
                m_recvPacket = SP_InteropMemory(new InteropUnpooledMemory(BUFFER_SIZE));
                m_recvPacket->Length(BUFFER_SIZE);
            }

            ssize_t res = recv(fd, m_recvPacket->Data(), m_recvPacket->Length(), 0);
            if (res < 0)
                return DataBuffer();

            return DataBuffer(m_recvPacket, 0, static_cast<int32_t>(res));
        }

        bool LinuxAsyncClient::StartMonitoring(int epoll0)
        {
            if (epoll0 < 0)
                return false;

            epoll_event event;
            std::memset(&event, 0, sizeof(event));
            event.data.ptr = this;
            event.events = EPOLLIN | EPOLLOUT | EPOLLRDHUP;

            int res = epoll_ctl(epoll0, EPOLL_CTL_ADD, fd, &event);
            if (res < 0)
                return false;

            epoll = epoll0;

            return true;
        }

        void LinuxAsyncClient::StopMonitoring()
        {
            epoll_event event;
            std::memset(&event, 0, sizeof(event));

            epoll_ctl(epoll, EPOLL_CTL_DEL, fd, &event);
        }

        void LinuxAsyncClient::EnableSendNotifications()
        {
            epoll_event event;
            std::memset(&event, 0, sizeof(event));
            event.data.ptr = this;
            event.events = EPOLLIN | EPOLLOUT | EPOLLRDHUP;

            epoll_ctl(epoll, EPOLL_CTL_MOD, fd, &event);
        }

        void LinuxAsyncClient::DisableSendNotifications()
        {
            epoll_event event;
            std::memset(&event, 0, sizeof(event));
            event.data.ptr = this;
            event.events = EPOLLIN | EPOLLRDHUP;

            epoll_ctl(epoll, EPOLL_CTL_MOD, fd, &event);
        }

        bool LinuxAsyncClient::processSent()
        {
            common::concurrent::CsLockGuard lock(m_sendMutex);

            if (m_sendPackets.empty())
            {
                DisableSendNotifications();

                return true;
            }

            DataBuffer& front = m_sendPackets.front();

            if (front.isEmpty())
                m_sendPackets.pop_front();

            return sendNextPacketLocked();
        }
    }
}
