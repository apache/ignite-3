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

#include "linux_async_worker_thread.h"

#include "../utils.h"
#include "linux_async_client_pool.h"

#include <algorithm>
#include <cstring>

#include <netdb.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

namespace ignite::network::detail {

namespace {

fibonacci_sequence<10> fibonacci10;

} // namespace

linux_async_worker_thread::linux_async_worker_thread(linux_async_client_pool &client_pool)
    : m_client_pool(client_pool)
    , m_stopping(true)
    , m_epoll(-1)
    , m_stop_event(-1)
    , m_non_connected()
    , m_current_connection()
    , m_current_client()
    , m_failed_attempts(0)
    , m_last_connection_time()
    , m_min_addrs(0)
    , m_thread() {
    memset(&m_last_connection_time, 0, sizeof(m_last_connection_time));
}

linux_async_worker_thread::~linux_async_worker_thread() {
    stop();
}

void linux_async_worker_thread::start(size_t limit, std::vector<tcp_range> addrs) {
    m_epoll = epoll_create(1);
    if (m_epoll < 0)
        throw_last_system_error("Failed to create epoll instance");

    m_stop_event = eventfd(0, EFD_NONBLOCK);
    if (m_stop_event < 0) {
        std::string msg = get_last_system_error("Failed to create stop event instance", "");
        close(m_stop_event);
        throw ignite_error(error::code::INTERNAL, msg);
    }

    epoll_event event{};
    memset(&event, 0, sizeof(event));

    event.events = EPOLLIN;

    int res = epoll_ctl(m_epoll, EPOLL_CTL_ADD, m_stop_event, &event);
    if (res < 0) {
        std::string msg = get_last_system_error("Failed to create stop event instance", "");
        close(m_stop_event);
        close(m_epoll);
        throw ignite_error(error::code::INTERNAL, msg);
    }

    m_stopping = false;
    m_failed_attempts = 0;
    m_non_connected = std::move(addrs);

    m_current_connection.reset();
    m_current_client.reset();

    if (!limit || limit > m_non_connected.size())
        m_min_addrs = 0;
    else
        m_min_addrs = m_non_connected.size() - limit;

    m_thread = std::thread(&linux_async_worker_thread::run, this);
}

void linux_async_worker_thread::stop() {
    if (m_stopping)
        return;

    m_stopping = true;

    int64_t value = 1;
    ssize_t res = write(m_stop_event, &value, sizeof(value));

    (void) res;
    assert(res == sizeof(value));

    m_thread.join();

    close(m_stop_event);
    close(m_epoll);

    m_non_connected.clear();
    m_current_connection.reset();
}

void linux_async_worker_thread::run() {
    while (!m_stopping) {
        handle_new_connections();

        if (m_stopping)
            break;

        handle_connection_events();
    }
}

void linux_async_worker_thread::handle_new_connections() {
    if (!should_initiate_new_connection())
        return;

    if (calculate_connection_timeout() > 0)
        return;

    addrinfo *addr = nullptr;
    if (m_current_connection)
        addr = m_current_connection->next();

    if (!addr) {
        // TODO: Use round-robin instead.
        size_t idx = rand() % m_non_connected.size();
        const tcp_range &range = m_non_connected.at(idx);

        m_current_connection = std::make_unique<connecting_context>(range);
        addr = m_current_connection->next();
        if (!addr) {
            m_current_connection.reset();
            report_connection_error(end_point(), "Can not resolve a single address from range: " + range.to_string());
            ++m_failed_attempts;
            return;
        }
    }

    // Create a socket for connecting to server
    int socket_fd = socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);
    if (SOCKET_ERROR == socket_fd) {
        report_connection_error(
            m_current_connection->current_address(), "Socket creation failed: " + get_last_socket_error_message());
        return;
    }

    try_set_socket_options(socket_fd, linux_async_client::BUFFER_SIZE, true, true, true);
    bool success = set_non_blocking_mode(socket_fd, true);
    if (!success) {
        report_connection_error(m_current_connection->current_address(),
            "Can not make non-blocking socket: " + get_last_socket_error_message());
        return;
    }

    m_current_client = m_current_connection->to_client(socket_fd);
    bool ok = m_current_client->start_monitoring(m_epoll);
    if (!ok)
        throw_last_system_error("Can not add file descriptor to epoll");

    // Connect to server.
    int res = connect(socket_fd, addr->ai_addr, addr->ai_addrlen);
    if (SOCKET_ERROR == res) {
        int last_error = errno;

        clock_gettime(CLOCK_MONOTONIC, &m_last_connection_time);

        if (last_error != EWOULDBLOCK && last_error != EINPROGRESS) {
            handle_connection_failed(
                "Failed to establish connection with the host: " + get_socket_error_message(last_error));
            return;
        }
    }
}

void linux_async_worker_thread::handle_connection_events() {
    enum { MAX_EVENTS = 16 };

    epoll_event events[MAX_EVENTS];

    int timeout = calculate_connection_timeout();

    int res = epoll_wait(m_epoll, events, MAX_EVENTS, timeout);

    if (res <= 0)
        return;

    for (int i = 0; i < res; ++i) {
        epoll_event &current_event = events[i];
        auto client = static_cast<linux_async_client *>(current_event.data.ptr);
        if (!client)
            continue;

        if (client == m_current_client.get()) {
            if (current_event.events & (EPOLLRDHUP | EPOLLERR)) {
                handle_connection_failed("Can not establish connection");
                continue;
            }

            handle_connection_success(client);
        }

        if (current_event.events & (EPOLLRDHUP | EPOLLERR | EPOLLHUP)) {
            handle_connection_closed(client);
            continue;
        }

        if (current_event.events & EPOLLIN) {
            auto msg = client->receive();
            if (msg.empty()) {
                handle_connection_closed(client);
                continue;
            }

            m_client_pool.handle_message_received(client->id(), msg);
        }

        if (current_event.events & EPOLLOUT) {
            bool ok = client->process_sent();
            if (!ok) {
                handle_connection_closed(client);
                continue;
            }

            m_client_pool.handle_message_sent(client->id());
        }
    }
}

void linux_async_worker_thread::report_connection_error(const end_point &addr, std::string msg) {
    ignite_error err(error::code::CONNECTION, std::move(msg));
    m_client_pool.handle_connection_error(addr, err);
}

void linux_async_worker_thread::handle_connection_failed(std::string msg) {
    assert(m_current_client);

    m_current_client->stop_monitoring();
    m_current_client->close();

    report_connection_error(m_current_client->address(), std::move(msg));

    m_current_client.reset();
    ++m_failed_attempts;
}

void linux_async_worker_thread::handle_connection_closed(linux_async_client *client) {
    client->stop_monitoring();

    m_non_connected.push_back(client->get_range());

    m_client_pool.close_and_release(client->id(), std::nullopt);
}

void linux_async_worker_thread::handle_connection_success(linux_async_client *client) {
    m_non_connected.erase(std::find(m_non_connected.begin(), m_non_connected.end(), client->get_range()));

    m_client_pool.add_client(std::move(m_current_client));

    m_current_client.reset();
    m_current_connection.reset();

    m_failed_attempts = 0;

    clock_gettime(CLOCK_MONOTONIC, &m_last_connection_time);
}

int linux_async_worker_thread::calculate_connection_timeout() const {
    if (!should_initiate_new_connection())
        return -1;

    if (m_last_connection_time.tv_sec == 0)
        return 0;

    int timeout = int(fibonacci10.get_value(m_failed_attempts) * 1000);

    timespec now{};
    clock_gettime(CLOCK_MONOTONIC, &now);

    int passed = int(
        (now.tv_sec - m_last_connection_time.tv_sec) * 1000 + (now.tv_nsec - m_last_connection_time.tv_nsec) / 1000000);

    timeout -= passed;
    if (timeout < 0)
        timeout = 0;

    return timeout;
}

bool linux_async_worker_thread::should_initiate_new_connection() const {
    return !m_current_client && m_non_connected.size() > m_min_addrs;
}

} // namespace ignite::network::detail
