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

#include "ignite/common/end_point.h"
#include "ignite/network/async_handler.h"
#include "ignite/network/codec.h"
#include "ignite/network/tcp_range.h"

#include <cstdint>
#include <deque>
#include <mutex>

namespace ignite::network::detail {

/**
 * Operation kind.
 */
enum class io_operation_kind {
    SEND,

    RECEIVE,
};

/**
 * Represents a single IO operation.
 * Needed to be able to distinguish one operation from another.
 */
struct io_operation {
    /** Overlapped structure that should be passed to every IO operation. */
    WSAOVERLAPPED overlapped;

    /** Operation type. */
    io_operation_kind kind;
};

/**
 * Windows-specific implementation of an async network client.
 */
class win_async_client {
public:
    /**
     * state.
     */
    enum class state {
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
    win_async_client(SOCKET socket, end_point addr, tcp_range range, int32_t m_bufLen);

    /**
     * Destructor.
     */
    ~win_async_client();

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
     * Add client to IOCP.
     *
     * @return IOCP handle on success and NULL otherwise.
     */
    HANDLE add_to_iocp(HANDLE iocp);

    /**
     * Send a packet using the client.
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
    bool receive();

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
    void set_id(uint64_t id) { m_id = id; }

    /**
     * Get address.
     *
     * @return Address.
     */
    [[nodiscard]] const end_point &address() const { return m_addr; }

    /**
     * Get range.
     *
     * @return Range.
     */
    [[nodiscard]] const tcp_range &get_range() const { return m_range; }

    /**
     * Check whether the client is closed.
     *
     * @return @c true if closed.
     */
    [[nodiscard]] bool is_closed() const { return m_socket == NULL; }

    /**
     * Process sent data.
     *
     * @param bytes Bytes.
     * @return @c true on success.
     */
    bool process_sent(size_t bytes);

    /**
     * Process received bytes.
     *
     * @param bytes Number of received bytes.
     */
    bytes_view process_received(size_t bytes);

    /**
     * Get closing error for the connection. Can be IGNITE_SUCCESS.
     *
     * @return Connection error.
     */
    [[nodiscard]] const ignite_error &get_close_error() const { return m_close_err; }

private:
    /**
     * Clears the client's receive buffer.
     *
     * @return Data received so far.
     */
    void clear_receive_buffer();

    /**
     * Send the next packet in queue.
     *
     * @warning Can only be called when holding m_send_mutex lock.
     * @return @c true on success.
     */
    bool send_next_packet_locked();

    /** Buffer length. */
    const int32_t m_bufLen;

    /** Client state. */
    state m_state;

    /** Socket. */
    SOCKET m_socket;

    /** Connection ID. */
    uint64_t m_id;

    /** Server end point. */
    end_point m_addr;

    /** Address range associated with current connection. */
    tcp_range m_range;

    /** The current send operation. */
    io_operation m_current_send{};

    /** Packets that should be sent. */
    std::deque<data_buffer_owning> m_send_packets;

    /** A send critical section. */
    std::mutex m_send_mutex;

    /** The current receive operation. */
    io_operation m_current_recv{};

    /** Packet that is currently received. */
    std::vector<std::byte> m_recv_packet;

    /** Closing error. */
    ignite_error m_close_err;
};

} // namespace ignite::network::detail
