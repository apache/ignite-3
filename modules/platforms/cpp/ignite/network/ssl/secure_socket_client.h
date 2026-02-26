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

#include <ignite/network/network.h>
#include <ignite/network/socket_client.h>
#include <ignite/network/ssl/secure_configuration.h>
#include <ignite/network/ssl/ssl_connection.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

namespace ignite::network
{

/**
 * Secure socket client.
 */
class secure_socket_client : public socket_client
{
public:
    /**
     * Constructor.
     *
     * @param cfg Secure configuration.
     */
    explicit secure_socket_client(secure_configuration  cfg)
        : m_cfg(std::move(cfg))
        , m_socket_client(make_tcp_socket_client())
        , m_recv_packet(BUFFER_SIZE)
    {}

    /**
     * Destructor.
     */
    ~secure_socket_client() override;

    /**
     * Establish connection with the host.
     *
     * @param hostname Host name or address.
     * @param port TCP port.
     * @param timeout Timeout in seconds.
     * @return @c true on success and @c false on timeout.
     */
    bool connect(const char* hostname, std::uint16_t port, std::int32_t timeout) override;

    /**
     * Close the connection.
     */
    void close() override;

    /**
     * Send data using connection.
     * @param data Data to send.
     * @param size Number of bytes to send.
     * @param timeout Timeout in seconds.
     * @return Number of bytes that have been sent on success,
     *     WaitResult::TIMEOUT on timeout and -errno on failure.
     */
    int send(const std::byte* data, std::size_t size, std::int32_t timeout) override;

    /**
     * Receive data from established connection.
     *
     * @param buffer Pointer to data buffer.
     * @param size Size of the buffer in bytes.
     * @param timeout Timeout in seconds.
     * @return Number of bytes that have been received on success,
     *     WaitResult::TIMEOUT on timeout and -errno on failure.
     */
    int receive(std::byte* buffer, std::size_t size, std::int32_t timeout) override;

    /**
     * Check if the socket is blocking or not.
     * @return @c true if the socket is blocking and false otherwise.
     */
    [[nodiscard]] bool is_blocking() const override {
        return m_socket_client->is_blocking();
    }

private:
    static constexpr size_t BUFFER_SIZE = 0x10000;

    /**
     * Close the connection. Internal call.
     */
    void close_internal();

    /**
     * Drive the TLS handshake to completion, exchanging data with the peer
     * over the socket.
     *
     * @param timeout Timeout in seconds.
     * @return @c true on success and @c false on timeout.
     */
    bool complete_connect_internal(std::int32_t timeout);

    /**
     * Drain all pending ciphertext from the SSL output BIO and send it to the peer.
     *
     * @param timeout Timeout in seconds.
     * @return WaitResult::SUCCESS on success, WaitResult::TIMEOUT on timeout, negative on error.
     */
    int flush_bio_out(std::int32_t timeout);

    /**
     * Read data from the socket and feed it into the SSL input BIO.
     *
     * @param timeout Timeout in seconds.
     * @return Bytes fed on success, WaitResult::TIMEOUT on timeout, negative on error.
     */
    int fill_bio_in(std::int32_t timeout);

    /** Secure configuration. */
    secure_configuration m_cfg;

    /** SSL context (SSL_CTX*). */
    void* m_context{nullptr};

    /** TLS state machine (SSL instance + memory BIOs). */
    std::unique_ptr<ssl_connection> m_ssl_conn;

    std::unique_ptr<socket_client> m_socket_client;

    /** Buffer for incoming data. */
    std::vector<std::byte> m_recv_packet;
};

} // namespace ignite::network
