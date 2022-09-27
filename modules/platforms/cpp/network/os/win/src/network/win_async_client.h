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

#pragma once

#include "network/sockets.h"

#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>

#include <ignite/network/async_handler.h>
#include <ignite/network/codec.h>
#include <ignite/network/end_point.h>
#include <ignite/network/tcp_range.h>

namespace ignite::network
{

/**
 * Operation kind.
 */
enum class IoOperationKind
{
    SEND,

    RECEIVE,
};

/**
 * Represents single IO operation.
 * Needed to be able to distinguish one operation from another.
 */
struct IoOperation
{
    /** Overlapped structure that should be passed to every IO operation. */
    WSAOVERLAPPED overlapped;

    /** Operation type. */
    IoOperationKind kind;
};

/**
 * Windows-specific implementation of async network client.
 */
class WinAsyncClient
{
public:
    /**
     * State.
     */
    enum class State
    {
        CONNECTED,

        IN_POOL,

        SHUTDOWN,

        CLOSED,
    };

    /**
     * Constructor.
     *
     * @param socket Socket.
     * @param addr Address.
     * @param range Range.
     * @param m_bufLen Buffer length.
     */
    WinAsyncClient(SOCKET m_socket, EndPoint addr, TcpRange range, int32_t m_bufLen);

    /**
     * Destructor.
     */
    ~WinAsyncClient();

    /**
     * Shutdown client.
     *
     * Can be called from external threads.
     * Can be called from WorkerThread.
     *
     * @param err Error message. Can be null.
     * @return @c true if shutdown performed successfully.
     */
    bool shutdown(std::optional<IgniteError> err);

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
     * Add client to IOCP.
     *
     * @return IOCP handle on success and NULL otherwise.
     */
    HANDLE addToIocp(HANDLE m_iocp);

    /**
     * Send packet using client.
     *
     * @param data Data to send.
     * @return @c true on success.
     */
    bool send(const DataBufferShared& data);

    /**
     * Initiate next receive of data.
     *
     * @return @c true on success.
     */
    bool receive();

    /**
     * Get client ID.
     *
     * @return Client ID.
     */
    [[nodiscard]]
    uint64_t getId() const
    {
        return m_id;
    }

    /**
     * Set ID.
     *
     * @param id ID to set.
     */
    void setId(uint64_t id)
    {
        m_id = id;
    }

    /**
     * Get address.
     *
     * @return Address.
     */
    [[nodiscard]]
    const EndPoint& getAddress() const
    {
        return m_addr;
    }

    /**
     * Get range.
     *
     * @return Range.
     */
    [[nodiscard]]
    const TcpRange& getRange() const
    {
        return m_range;
    }

    /**
     * Check whether client is closed.
     *
     * @return @c true if closed.
     */
    [[nodiscard]]
    bool isClosed() const
    {
        return m_socket == NULL;
    }

    /**
     * Process sent data.
     *
     * @param bytes Bytes.
     * @return @c true on success.
     */
    bool processSent(size_t bytes);

    /**
     * Process received bytes.
     *
     * @param bytes Number of received bytes.
     */
    DataBufferRef processReceived(size_t bytes);

    /**
     * Get closing error for the connection. Can be IGNITE_SUCCESS.
     *
     * @return Connection error.
     */
    [[nodiscard]]
    const IgniteError& getCloseError() const
    {
        return m_closeErr;
    }

private:

    /**
     * Clears client's receive buffer.
     *
     * @return Data received so far.
     */
    void clearReceiveBuffer();

    /**
     * Send next packet in queue.
     *
     * @warning Can only be called when holding m_sendMutex lock.
     * @return @c true on success.
     */
    bool sendNextPacketLocked();

    /** Buffer length. */
    const int32_t m_bufLen;

    /** Client state. */
    State m_state;

    /** Socket. */
    SOCKET m_socket;

    /** Connection ID. */
    uint64_t m_id;

    /** Server end point. */
    EndPoint m_addr;

    /** Address range associated with current connection. */
    TcpRange m_range;

    /** Current send operation. */
    IoOperation m_currentSend{};

    /** Packets that should be sent. */
    std::deque<DataBufferShared> m_sendPackets;

    /** Send critical section. */
    std::mutex m_sendMutex;

    /** Current receive operation. */
    IoOperation m_currentRecv{};

    /** Packet that is currently received. */
    std::vector<std::byte> m_recvPacket;

    /** Closing error. */
    IgniteError m_closeErr;
};

} // namespace ignite::network
