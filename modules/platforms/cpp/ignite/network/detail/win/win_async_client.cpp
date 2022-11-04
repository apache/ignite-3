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

#include "win_async_client.h"
#include "sockets.h"

#include "../utils.h"

#include <algorithm>
#include <cassert>

namespace ignite::network::detail {

win_async_client::win_async_client(SOCKET socket, end_point addr, tcp_range range, int32_t m_bufLen)
    : m_bufLen(m_bufLen)
    , m_state(state::CONNECTED)
    , m_socket(socket)
    , m_id(0)
    , m_addr(std::move(addr))
    , m_range(std::move(range))
    , m_close_err() {
    memset(&m_current_send, 0, sizeof(m_current_send));
    m_current_send.kind = io_operation_kind::SEND;

    memset(&m_current_recv, 0, sizeof(m_current_recv));
    m_current_recv.kind = io_operation_kind::RECEIVE;
}

win_async_client::~win_async_client() {
    if (state::IN_POOL == m_state)
        shutdown(std::nullopt);

    close();
}

bool win_async_client::shutdown(std::optional<ignite_error> err) {
    std::lock_guard<std::mutex> lock(m_send_mutex);

    if (state::CONNECTED != m_state && state::IN_POOL != m_state)
        return false;

    m_close_err = err ? std::move(*err) : ignite_error("Connection closed by application");

    ::shutdown(m_socket, SD_BOTH);

    m_state = state::SHUTDOWN;

    return true;
}

bool win_async_client::close() {
    if (state::CLOSED == m_state)
        return false;

    ::closesocket(m_socket);

    m_send_packets.clear();
    m_recv_packet.clear();

    m_state = state::CLOSED;

    return true;
}

HANDLE win_async_client::add_to_iocp(HANDLE iocp) {
    assert(state::CONNECTED == m_state);

    HANDLE res = CreateIoCompletionPort((HANDLE) m_socket, iocp, reinterpret_cast<DWORD_PTR>(this), 0);

    if (!res)
        return res;

    m_state = state::IN_POOL;

    return res;
}

bool win_async_client::send(std::vector<std::byte> &&data) {
    std::lock_guard<std::mutex> lock(m_send_mutex);

    if (state::CONNECTED != m_state && state::IN_POOL != m_state)
        return false;

    m_send_packets.emplace_back(std::move(data));

    if (m_send_packets.size() > 1)
        return true;

    return send_next_packet_locked();
}

bool win_async_client::send_next_packet_locked() {
    if (m_send_packets.empty())
        return true;

    auto dataView = m_send_packets.front().get_bytes_view();
    DWORD flags = 0;

    WSABUF buffer;
    buffer.buf = (CHAR *) dataView.data();
    buffer.len = (ULONG) dataView.size();

    int ret =
        ::WSASend(m_socket, &buffer, 1, NULL, flags, &m_current_send.overlapped, NULL); // NOLINT(modernize-use-nullptr)

    return ret != SOCKET_ERROR || WSAGetLastError() == ERROR_IO_PENDING;
}

bool win_async_client::receive() {
    // We do not need locking on receive as we're always reading in a single thread at most.
    // If this ever changes we'd need to add mutex locking here.
    if (state::CONNECTED != m_state && state::IN_POOL != m_state)
        return false;

    if (m_recv_packet.empty())
        clear_receive_buffer();

    DWORD flags = 0;
    WSABUF buffer;
    buffer.buf = (CHAR *) m_recv_packet.data();
    buffer.len = (ULONG) m_recv_packet.size();

    int ret = ::WSARecv(
        m_socket, &buffer, 1, NULL, &flags, &m_current_recv.overlapped, NULL); // NOLINT(modernize-use-nullptr)

    return ret != SOCKET_ERROR || WSAGetLastError() == ERROR_IO_PENDING;
}

void win_async_client::clear_receive_buffer() {
    if (m_recv_packet.empty())
        m_recv_packet.resize(m_bufLen);
}

bytes_view win_async_client::process_received(size_t bytes) {
    return {m_recv_packet.data(), bytes};
}

bool win_async_client::process_sent(size_t bytes) {
    std::lock_guard<std::mutex> lock(m_send_mutex);

    auto &front = m_send_packets.front();

    front.skip(static_cast<int32_t>(bytes));

    if (front.empty())
        m_send_packets.pop_front();

    return send_next_packet_locked();
}

} // namespace ignite::network::detail
