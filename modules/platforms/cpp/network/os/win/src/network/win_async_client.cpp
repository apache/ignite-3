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

#include <cassert>

#include <algorithm>

#include <network/utils.h>

#include "network/sockets.h"
#include "network/win_async_client.h"

namespace ignite::network
{

WinAsyncClient::WinAsyncClient(SOCKET socket, EndPoint addr, TcpRange range, int32_t m_bufLen) :
    m_bufLen(m_bufLen),
    m_state(State::CONNECTED),
    m_socket(socket),
    m_id(0),
    m_addr(std::move(addr)),
    m_range(std::move(range)),
    m_closeErr()
{
    memset(&m_currentSend, 0, sizeof(m_currentSend));
    m_currentSend.kind = IoOperationKind::SEND;

    memset(&m_currentRecv, 0, sizeof(m_currentRecv));
    m_currentRecv.kind = IoOperationKind::RECEIVE;
}

WinAsyncClient::~WinAsyncClient()
{
    if (State::IN_POOL == m_state)
        shutdown(std::nullopt);

    close();
}

bool WinAsyncClient::shutdown(std::optional<IgniteError> err)
{
    std::lock_guard<std::mutex> lock(m_sendMutex);

    if (State::CONNECTED != m_state && State::IN_POOL != m_state)
        return false;

    m_closeErr = err ? std::move(*err) : IgniteError("Connection closed by application");

    ::shutdown(m_socket, SD_BOTH);

    m_state = State::SHUTDOWN;

    return true;
}

bool WinAsyncClient::close()
{
    if (State::CLOSED == m_state)
        return false;

    ::closesocket(m_socket);

    m_sendPackets.clear();
    m_recvPacket.clear();

    m_state = State::CLOSED;

    return true;
}

HANDLE WinAsyncClient::addToIocp(HANDLE iocp)
{
    assert(State::CONNECTED == m_state);

    HANDLE res = CreateIoCompletionPort((HANDLE)m_socket, iocp, reinterpret_cast<DWORD_PTR>(this), 0);

    if (!res)
        return res;

    m_state = State::IN_POOL;

    return res;
}

bool WinAsyncClient::send(std::vector<std::byte>&& data)
{
    std::lock_guard<std::mutex> lock(m_sendMutex);

    if (State::CONNECTED != m_state && State::IN_POOL != m_state)
        return false;

    m_sendPackets.emplace_back(std::move(data));

    if (m_sendPackets.size() > 1)
        return true;

    return sendNextPacketLocked();
}

bool WinAsyncClient::sendNextPacketLocked()
{
    if (m_sendPackets.empty())
        return true;

    auto dataView = m_sendPackets.front().getBytesView();
    DWORD flags = 0;

    WSABUF buffer;
    buffer.buf = (CHAR*)dataView.data();
    buffer.len = (ULONG)dataView.size();

    int ret = ::WSASend(m_socket, &buffer, 1, NULL, flags, &m_currentSend.overlapped, NULL); // NOLINT(modernize-use-nullptr)

    return ret != SOCKET_ERROR || WSAGetLastError() == ERROR_IO_PENDING;
}

bool WinAsyncClient::receive()
{
    // We do not need locking on receive as we're always reading in a single thread at most.
    // If this ever changes we'd need to add mutex locking here.
    if (State::CONNECTED != m_state && State::IN_POOL != m_state)
        return false;

    if (m_recvPacket.empty())
        clearReceiveBuffer();

    DWORD flags = 0;
    WSABUF buffer;
    buffer.buf = (CHAR*)m_recvPacket.data();
    buffer.len = (ULONG)m_recvPacket.size();

    int ret = ::WSARecv(m_socket, &buffer, 1, NULL, &flags, &m_currentRecv.overlapped, NULL); // NOLINT(modernize-use-nullptr)

    return ret != SOCKET_ERROR || WSAGetLastError() == ERROR_IO_PENDING;
}

void WinAsyncClient::clearReceiveBuffer()
{
    if (m_recvPacket.empty())
        m_recvPacket.resize(m_bufLen);
}

BytesView WinAsyncClient::processReceived(size_t bytes)
{
    return {m_recvPacket.data(), bytes};
}

bool WinAsyncClient::processSent(size_t bytes)
{
    std::lock_guard<std::mutex> lock(m_sendMutex);

    auto& front = m_sendPackets.front();

    front.skip(static_cast<int32_t>(bytes));

    if (front.isEmpty())
        m_sendPackets.pop_front();

    return sendNextPacketLocked();
}

} // namespace ignite::network
