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
#include <netdb.h>
#include <sys/epoll.h>

#include <cstring>
#include <iterator>

#include "network/connecting_context.h"

namespace ignite::network
{

ConnectingContext::ConnectingContext(TcpRange range) :
    m_range(std::move(range)),
    m_nextPort(range.port),
    m_info(nullptr),
    m_currentInfo(nullptr) { }

ConnectingContext::~ConnectingContext()
{
    reset();
}

void ConnectingContext::reset()
{
    if (m_info)
    {
        freeaddrinfo(m_info);
        m_info = nullptr;
        m_currentInfo = nullptr;
    }

    m_nextPort = m_range.port;
}

addrinfo* ConnectingContext::next()
{
    if (m_currentInfo)
        m_currentInfo = m_currentInfo->ai_next;

    while (!m_currentInfo)
    {
        if (m_info)
        {
            freeaddrinfo(m_info);
            m_info = nullptr;
        }

        if (m_nextPort > m_range.port + m_range.range)
            return nullptr;

        addrinfo hints{};
        std::memset(&hints, 0, sizeof(hints));

        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_protocol = IPPROTO_TCP;

        std::string strPort = std::to_string(m_nextPort);

        // Resolve the server address and port
        int res = getaddrinfo(m_range.host.c_str(), strPort.c_str(), &hints, &m_info);
        if (res != 0)
            return nullptr;

        m_currentInfo = m_info;
        ++m_nextPort;
    }

    return m_currentInfo;
}

EndPoint ConnectingContext::getAddress() const
{
    if (!m_currentInfo)
        throw IgniteError("There is no current address");

    return {m_range.host, uint16_t(m_nextPort - 1)};
}

std::shared_ptr<LinuxAsyncClient> ConnectingContext::toClient(int fd)
{
    return std::make_shared<LinuxAsyncClient>(fd, getAddress(), m_range);
}

} // namespace ignite::network
