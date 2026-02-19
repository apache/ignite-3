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

#include <ignite/network/detail/linux/linux_async_client.h>

#include <algorithm>
#include <cstring>

#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

// We don't want to use epoll-shim macro here, because we have other close() functions.
#undef close

namespace ignite::network::detail {

linux_async_client::linux_async_client(int fd, end_point addr, tcp_range range)
    : m_state(state::CONNECTED)
    , m_fd(fd)
    , m_epoll(-1)
    , m_id(0)
    , m_addr(std::move(addr))
    , m_range(std::move(range))
    , m_send_packets()
    , m_send_mutex()
    , m_recv_packet(BUFFER_SIZE)
    , m_close_err() {
}

linux_async_client::~linux_async_client() {
    shutdown(std::nullopt);

    close();
}

bool linux_async_client::shutdown(std::optional<ignite_error> err) {
    std::lock_guard<std::mutex> lock(m_send_mutex);
    if (m_state != state::CONNECTED)
        return false;

    m_close_err = err ? std::move(*err) : ignite_error("Connection closed by application");
    ::shutdown(m_fd, SHUT_RDWR);
    m_state = state::SHUTDOWN;

    return true;
}

bool linux_async_client::close() {
    if (state::CLOSED == m_state)
        return false;

    stop_monitoring();
    ::close(m_fd);
    m_fd = -1;
    m_state = state::CLOSED;

    return true;
}

bool linux_async_client::send(std::vector<std::byte> &&data) {
    std::lock_guard<std::mutex> lock(m_send_mutex);

    m_send_packets.emplace_back(std::move(data));
    if (m_send_packets.size() > 1)
        return true;

    return send_next_packet_locked();
}

bool linux_async_client::send_next_packet_locked() {
    if (m_send_packets.empty())
        return true;

    auto &packet = m_send_packets.front();
    auto dataView = packet.get_bytes_view();

    ssize_t ret = ::send(m_fd, dataView.data(), dataView.size(), SO_NOSIGPIPE);
    if (ret < 0)
        return false;

    packet.skip(static_cast<int32_t>(ret));

    enable_send_notifications();

    return true;
}

bytes_view linux_async_client::receive() {
    ssize_t res = recv(m_fd, m_recv_packet.data(), m_recv_packet.size(), 0);
    if (res < 0)
        return {};

    return {m_recv_packet.data(), size_t(res)};
}

bool linux_async_client::start_monitoring(int epoll0) {
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

void linux_async_client::stop_monitoring() // NOLINT(readability-make-member-function-const)
{
    epoll_event event{};
    memset(&event, 0, sizeof(event));

    epoll_ctl(m_epoll, EPOLL_CTL_DEL, m_fd, &event);
}

void linux_async_client::enable_send_notifications() {
    epoll_event event{};
    memset(&event, 0, sizeof(event));
    event.data.ptr = this;
    event.events = EPOLLIN | EPOLLOUT | EPOLLRDHUP;

    epoll_ctl(m_epoll, EPOLL_CTL_MOD, m_fd, &event);
}

void linux_async_client::disable_send_notifications() {
    epoll_event event{};
    memset(&event, 0, sizeof(event));
    event.data.ptr = this;
    event.events = EPOLLIN | EPOLLRDHUP;

    epoll_ctl(m_epoll, EPOLL_CTL_MOD, m_fd, &event);
}

bool linux_async_client::process_sent() {
    std::lock_guard<std::mutex> lock(m_send_mutex);

    if (m_send_packets.empty()) {
        disable_send_notifications();

        return true;
    }

    if (m_send_packets.front().empty())
        m_send_packets.pop_front();

    return send_next_packet_locked();
}

} // namespace ignite::network::detail
