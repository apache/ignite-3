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
#include <sys/epoll.h>
#include <unistd.h>

#include <cstring>
#include <algorithm>

#include "network/utils.h"

#include "network/sockets.h"
#include "network/linux_async_client.h"

namespace ignite::network
{

LinuxAsyncClient::LinuxAsyncClient(int fd, EndPoint addr, TcpRange range) :
    m_state(State::CONNECTED),
    m_fd(fd),
    m_epoll(-1),
    m_id(0),
    m_addr(std::move(addr)),
    m_range(std::move(range)),
    m_sendPackets(),
    m_sendMutex(),
    m_recvPacket(BUFFER_SIZE),
    m_closeErr() { }

LinuxAsyncClient::~LinuxAsyncClient()
{
    shutdown(std::nullopt);

    close();
}

bool LinuxAsyncClient::shutdown(std::optional<IgniteError> err)
{
    std::lock_guard<std::mutex> lock(m_sendMutex);
    if (m_state != State::CONNECTED)
        return false;

    m_closeErr = err ? std::move(*err) : IgniteError("Connection closed by application");
    ::shutdown(m_fd, SHUT_RDWR);
    m_state = State::SHUTDOWN;

    return true;
}

bool LinuxAsyncClient::close()
{
    if (State::CLOSED == m_state)
        return false;

    stopMonitoring();
    ::close(m_fd);
    m_fd = -1;
    m_state = State::CLOSED;

    return true;
}

bool LinuxAsyncClient::send(const DataBufferOwning& data)
{
    std::lock_guard<std::mutex> lock(m_sendMutex);

    m_sendPackets.push_back(data);
    if (m_sendPackets.size() > 1)
        return true;

    return sendNextPacketLocked();
}

bool LinuxAsyncClient::sendNextPacketLocked()
{
    if (m_sendPackets.empty())
        return true;

    auto& packet = m_sendPackets.front();
    auto dataView = packet.getBytesView();

    ssize_t ret = ::send(m_fd, dataView.data(), dataView.size(), 0);
    if (ret < 0)
        return false;

    packet.skip(static_cast<int32_t>(ret));

    enableSendNotifications();

    return true;
}

DataBufferRef LinuxAsyncClient::receive()
{
    ssize_t res = recv(m_fd, m_recvPacket.data(), m_recvPacket.size(), 0);
    if (res < 0)
        return {};

    return {m_recvPacket, 0, size_t(res)};
}

bool LinuxAsyncClient::startMonitoring(int epoll0)
{
    if (epoll0 < 0)
        return false;

    epoll_event event{};
    memset(&event, 0, sizeof(event));
    event.data.ptr = this;
    event.events = EPOLLIN | EPOLLOUT | EPOLLRDHUP;

    int res = epoll_ctl(epoll0, EPOLL_CTL_ADD, m_fd, &event);
    if (res < 0)
        return false;

    m_epoll = epoll0;

    return true;
}

void LinuxAsyncClient::stopMonitoring() // NOLINT(readability-make-member-function-const)
{
    epoll_event event{};
    memset(&event, 0, sizeof(event));

    epoll_ctl(m_epoll, EPOLL_CTL_DEL, m_fd, &event);
}

void LinuxAsyncClient::enableSendNotifications()
{
    epoll_event event{};
    memset(&event, 0, sizeof(event));
    event.data.ptr = this;
    event.events = EPOLLIN | EPOLLOUT | EPOLLRDHUP;

    epoll_ctl(m_epoll, EPOLL_CTL_MOD, m_fd, &event);
}

void LinuxAsyncClient::disableSendNotifications()
{
    epoll_event event{};
    memset(&event, 0, sizeof(event));
    event.data.ptr = this;
    event.events = EPOLLIN | EPOLLRDHUP;

    epoll_ctl(m_epoll, EPOLL_CTL_MOD, m_fd, &event);
}

bool LinuxAsyncClient::processSent()
{
    std::lock_guard<std::mutex> lock(m_sendMutex);

    if (m_sendPackets.empty())
    {
        disableSendNotifications();

        return true;
    }

    if (m_sendPackets.front().isEmpty())
        m_sendPackets.pop_front();

    return sendNextPacketLocked();
}

} // namespace ignite::network
