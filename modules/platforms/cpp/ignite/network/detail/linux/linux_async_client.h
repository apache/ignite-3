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

#pragma once

#include "sockets.h"

#include <ignite/network/async_handler.h>
#include <ignite/network/codec.h>
#include <ignite/network/end_point.h>
#include <ignite/network/tcp_range.h>

#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>

namespace ignite::network::detail {

/**
 * Linux-specific implementation of async network client.
 */
class LinuxAsyncClient {
    /**
     * State.
     */
    enum class State {
        CONNECTED,

        SHUTDOWN,

        CLOSED,
    };

public:
    static constexpr size_t BUFFER_SIZE = 0x10000;

    /**
     * Constructor.
     *
     * @param fd Socket file descriptor.
     * @param addr Address.
     * @param range Range.
     */
    LinuxAsyncClient(int fd, EndPoint addr, TcpRange range);

    /**
     * Destructor.
     *
     * Should not be destructed from external threads.
     * Can be destructed from WorkerThread.
     */
    ~LinuxAsyncClient();

    /**
     * Shutdown client.
     *
     * Can be called from external threads.
     * Can be called from WorkerThread.
     *
     * @param err Error message. Can be null.
     * @return @c true if shutdown performed successfully.
     */
    bool shutdown(std::optional<ignite_error> err);

    /**
     * Close client.
     *
     * Should not be called from external threads.
     * Can be called from WorkerThread.
     *
     * @return @c true if shutdown performed successfully.
     */
    bool close();

    /**
     * Send packet using client.
     *
     * @param data Data to send.
     * @return @c true on success.
     */
    bool send(std::vector<std::byte> &&data);

    /**
     * Initiate next receive of data.
     *
     * @return @c true on success.
     */
    bytes_view receive();

    /**
     * Process sent data.
     *
     * @return @c true on success.
     */
    bool processSent();

    /**
     * Start monitoring client.
     *
     * @param epoll Epoll file descriptor.
     * @return @c true on success.
     */
    bool startMonitoring(int epoll);

    /**
     * Stop monitoring client.
     */
    void stopMonitoring();

    /**
     * Enable epoll notifications.
     */
    void enableSendNotifications();

    /**
     * Disable epoll notifications.
     */
    void disableSendNotifications();

    /**
     * Get client ID.
     *
     * @return Client ID.
     */
    [[nodiscard]] uint64_t id() const { return m_id; }

    /**
     * Set ID.
     *
     * @param id ID to set.
     */
    void setId(uint64_t id) { m_id = id; }

    /**
     * Get address.
     *
     * @return Address.
     */
    [[nodiscard]] const EndPoint &getAddress() const { return m_addr; }

    /**
     * Get range.
     *
     * @return Range.
     */
    [[nodiscard]] const TcpRange &getRange() const { return m_range; }

    /**
     * Check whether client is closed.
     *
     * @return @c true if closed.
     */
    [[nodiscard]] bool isClosed() const { return m_state == State::CLOSED; }

    /**
     * Get closing error for the connection. Can be IGNITE_SUCCESS.
     *
     * @return Connection error.
     */
    [[nodiscard]] const ignite_error &getCloseError() const { return m_closeErr; }

private:
    /**
     * Send next packet in queue.
     *
     * @warning Can only be called when holding m_sendMutex lock.
     * @return @c true on success.
     */
    bool sendNextPacketLocked();

    /** State. */
    State m_state;

    /** Socket file descriptor. */
    int m_fd;

    /** Epoll file descriptor. */
    int m_epoll;

    /** Connection ID. */
    uint64_t m_id;

    /** Server end point. */
    EndPoint m_addr;

    /** Address range associated with current connection. */
    TcpRange m_range;

    /** Packets that should be sent. */
    std::deque<DataBufferOwning> m_sendPackets;

    /** Send critical section. */
    std::mutex m_sendMutex;

    /** Packet that is currently received. */
    std::vector<std::byte> m_recvPacket;

    /** Closing error. */
    ignite_error m_closeErr;
};

} // namespace ignite::network::detail
