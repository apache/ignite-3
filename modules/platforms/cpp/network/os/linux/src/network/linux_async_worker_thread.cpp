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

#include <netdb.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstring>

#include <algorithm>

#include <network/utils.h>

#include "network/linux_async_client_pool.h"
#include "network/linux_async_worker_thread.h"

namespace {
ignite::network::FibonacciSequence<10> fibonacci10;
}

namespace ignite::network {

LinuxAsyncWorkerThread::LinuxAsyncWorkerThread(LinuxAsyncClientPool &clientPool)
    : m_clientPool(clientPool)
    , m_stopping(true)
    , m_epoll(-1)
    , m_stopEvent(-1)
    , m_nonConnected()
    , m_currentConnection()
    , m_currentClient()
    , m_failedAttempts(0)
    , m_lastConnectionTime()
    , m_minAddrs(0)
    , m_thread() {
    memset(&m_lastConnectionTime, 0, sizeof(m_lastConnectionTime));
}

LinuxAsyncWorkerThread::~LinuxAsyncWorkerThread() {
    stop();
}

void LinuxAsyncWorkerThread::start(size_t limit, std::vector<TcpRange> addrs) {
    m_epoll = epoll_create(1);
    if (m_epoll < 0)
        throwLastSystemError("Failed to create epoll instance");

    m_stopEvent = eventfd(0, EFD_NONBLOCK);
    if (m_stopEvent < 0) {
        std::string msg = getLastSystemError("Failed to create stop event instance", "");
        close(m_stopEvent);
        throw ignite_error(status_code::OS, msg);
    }

    epoll_event event{};
    memset(&event, 0, sizeof(event));

    event.events = EPOLLIN;

    int res = epoll_ctl(m_epoll, EPOLL_CTL_ADD, m_stopEvent, &event);
    if (res < 0) {
        std::string msg = getLastSystemError("Failed to create stop event instance", "");
        close(m_stopEvent);
        close(m_epoll);
        throw ignite_error(status_code::OS, msg);
    }

    m_stopping = false;
    m_failedAttempts = 0;
    m_nonConnected = std::move(addrs);

    m_currentConnection.reset();
    m_currentClient.reset();

    if (!limit || limit > m_nonConnected.size())
        m_minAddrs = 0;
    else
        m_minAddrs = m_nonConnected.size() - limit;

    m_thread = std::thread(&LinuxAsyncWorkerThread::run, this);
}

void LinuxAsyncWorkerThread::stop() {
    if (m_stopping)
        return;

    m_stopping = true;

    int64_t value = 1;
    ssize_t res = write(m_stopEvent, &value, sizeof(value));

    (void)res;
    assert(res == sizeof(value));

    m_thread.join();

    close(m_stopEvent);
    close(m_epoll);

    m_nonConnected.clear();
    m_currentConnection.reset();
}

void LinuxAsyncWorkerThread::run() {
    while (!m_stopping) {
        handleNewConnections();

        if (m_stopping)
            break;

        handleConnectionEvents();
    }
}

void LinuxAsyncWorkerThread::handleNewConnections() {
    if (!shouldInitiateNewConnection())
        return;

    if (calculateConnectionTimeout() > 0)
        return;

    addrinfo *addr = nullptr;
    if (m_currentConnection)
        addr = m_currentConnection->next();

    if (!addr) {
        // TODO: Use round-robin instead.
        size_t idx = rand() % m_nonConnected.size();
        const TcpRange &range = m_nonConnected.at(idx);

        m_currentConnection = std::make_unique<ConnectingContext>(range);
        addr = m_currentConnection->next();
        if (!addr) {
            m_currentConnection.reset();
            reportConnectionError(EndPoint(), "Can not resolve a single address from range: " + range.toString());
            ++m_failedAttempts;

            return;
        }
    }

    // Create a socket for connecting to server
    int socketFd = socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);
    if (SOCKET_ERROR == socketFd) {
        reportConnectionError(
            m_currentConnection->getAddress(), "Socket creation failed: " + sockets::getLastSocketErrorMessage());

        return;
    }

    sockets::trySetSocketOptions(socketFd, LinuxAsyncClient::BUFFER_SIZE, true, true, true);
    bool success = sockets::setNonBlockingMode(socketFd, true);
    if (!success) {
        reportConnectionError(m_currentConnection->getAddress(),
            "Can not make non-blocking socket: " + sockets::getLastSocketErrorMessage());

        return;
    }

    m_currentClient = m_currentConnection->toClient(socketFd);
    bool ok = m_currentClient->startMonitoring(m_epoll);
    if (!ok)
        throwLastSystemError("Can not add file descriptor to epoll");

    // Connect to server.
    int res = connect(socketFd, addr->ai_addr, addr->ai_addrlen);
    if (SOCKET_ERROR == res) {
        int lastError = errno;

        clock_gettime(CLOCK_MONOTONIC, &m_lastConnectionTime);

        if (lastError != EWOULDBLOCK && lastError != EINPROGRESS) {
            handleConnectionFailed(
                "Failed to establish connection with the host: " + sockets::getSocketErrorMessage(lastError));

            return;
        }
    }
}

void LinuxAsyncWorkerThread::handleConnectionEvents() {
    enum { MAX_EVENTS = 16 };
    epoll_event events[MAX_EVENTS];

    int timeout = calculateConnectionTimeout();

    int res = epoll_wait(m_epoll, events, MAX_EVENTS, timeout);

    if (res <= 0)
        return;

    for (int i = 0; i < res; ++i) {
        epoll_event &currentEvent = events[i];
        auto client = static_cast<LinuxAsyncClient *>(currentEvent.data.ptr);
        if (!client)
            continue;

        if (client == m_currentClient.get()) {
            if (currentEvent.events & (EPOLLRDHUP | EPOLLERR)) {
                handleConnectionFailed("Can not establish connection");

                continue;
            }

            handleConnectionSuccess(client);
        }

        if (currentEvent.events & (EPOLLRDHUP | EPOLLERR | EPOLLHUP)) {
            handleConnectionClosed(client);

            continue;
        }

        if (currentEvent.events & EPOLLIN) {
            auto msg = client->receive();
            if (msg.empty()) {
                handleConnectionClosed(client);

                continue;
            }

            m_clientPool.handleMessageReceived(client->getId(), msg);
        }

        if (currentEvent.events & EPOLLOUT) {
            bool ok = client->processSent();
            if (!ok) {
                handleConnectionClosed(client);

                continue;
            }

            m_clientPool.handleMessageSent(client->getId());
        }
    }
}

void LinuxAsyncWorkerThread::reportConnectionError(const EndPoint &addr, std::string msg) {
    ignite_error err(status_code::NETWORK, std::move(msg));
    m_clientPool.handleConnectionError(addr, err);
}

void LinuxAsyncWorkerThread::handleConnectionFailed(std::string msg) {
    assert(m_currentClient);

    m_currentClient->stopMonitoring();
    m_currentClient->close();

    reportConnectionError(m_currentClient->getAddress(), std::move(msg));

    m_currentClient.reset();
    ++m_failedAttempts;
}

void LinuxAsyncWorkerThread::handleConnectionClosed(LinuxAsyncClient *client) {
    client->stopMonitoring();

    m_nonConnected.push_back(client->getRange());

    m_clientPool.closeAndRelease(client->getId(), std::nullopt);
}

void LinuxAsyncWorkerThread::handleConnectionSuccess(LinuxAsyncClient *client) {
    m_nonConnected.erase(std::find(m_nonConnected.begin(), m_nonConnected.end(), client->getRange()));

    m_clientPool.addClient(std::move(m_currentClient));

    m_currentClient.reset();
    m_currentConnection.reset();

    m_failedAttempts = 0;

    clock_gettime(CLOCK_MONOTONIC, &m_lastConnectionTime);
}

int LinuxAsyncWorkerThread::calculateConnectionTimeout() const {
    if (!shouldInitiateNewConnection())
        return -1;

    if (m_lastConnectionTime.tv_sec == 0)
        return 0;

    int timeout = int(fibonacci10.getValue(m_failedAttempts) * 1000);

    timespec now{};
    clock_gettime(CLOCK_MONOTONIC, &now);

    int passed =
        int((now.tv_sec - m_lastConnectionTime.tv_sec) * 1000 + (now.tv_nsec - m_lastConnectionTime.tv_nsec) / 1000000);

    timeout -= passed;
    if (timeout < 0)
        timeout = 0;

    return timeout;
}

bool LinuxAsyncWorkerThread::shouldInitiateNewConnection() const {
    return !m_currentClient && m_nonConnected.size() > m_minAddrs;
}

} // namespace ignite::network
