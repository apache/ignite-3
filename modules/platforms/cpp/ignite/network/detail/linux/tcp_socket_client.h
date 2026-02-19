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

#include "ignite/network/detail/linux/sockets.h"
#include "ignite/network/detail/utils.h"
#include "ignite/network/socket_client.h"

#include <cstdint>
#include <sstream>

#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#ifndef MSG_NOSIGNAL
# define MSG_NOSIGNAL 0
#endif

namespace ignite::network {

/**
 * Socket client implementation.
 */
class tcp_socket_client : public socket_client {
public:
    // Delete
    tcp_socket_client(tcp_socket_client &&) = delete;
    tcp_socket_client(const tcp_socket_client &) = delete;
    tcp_socket_client &operator=(tcp_socket_client &&) = delete;
    tcp_socket_client &operator=(const tcp_socket_client &) = delete;

    /** Buffers size */
    enum { BUFFER_SIZE = 0x10000 };

    // Default
    tcp_socket_client() = default;

    /**
     * Destructor.
     */
    ~tcp_socket_client() override { internal_close(); }

    /**
     * Establish connection with remote TCP service.
     *
     * @param hostname Remote host name.
     * @param port TCP service port.
     * @param timeout Timeout.
     * @return True on success.
     */
    bool connect(const char *hostname, std::uint16_t port, std::int32_t timeout) override {
        internal_close();

        addrinfo hints{};

        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_protocol = IPPROTO_TCP;

        std::stringstream converter;
        converter << port;
        std::string str_port = converter.str();

        // Resolve the server address and port
        addrinfo *result{NULL}; // NOLINT(modernize-use-nullptr)
        int res = getaddrinfo(hostname, str_port.c_str(), &hints, &result);

        if (res != 0)
            throw ignite_error(error::code::CONNECTION,
                "Can not resolve host: " + std::string(hostname) + ":" + str_port
                    + ", error_code=" + std::to_string(res));

        std::vector<addrinfo *> shuffled = detail::shuffle_addresses(result);

        std::string last_err_msg = "Failed to resolve host";
        bool is_timeout = false;

        // Attempt to connect to an address until one succeeds
        for (auto *addr : shuffled) {
            last_err_msg = "Failed to establish connection with the host";
            is_timeout = false;

            // Create a SOCKET for connecting to server
            m_socket_handle = socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);

            if (m_socket_handle == SOCKET_ERROR)
                throw ignite_error(
                    error::code::CONNECTION, "Socket creation failed: " + detail::get_last_socket_error_message());

            detail::try_set_socket_options(m_socket_handle, BUFFER_SIZE, true, true, true);

            m_blocking = !detail::set_non_blocking_mode(m_socket_handle, true);

            // Connect to server.
            res = ::connect(m_socket_handle, addr->ai_addr, static_cast<int>(addr->ai_addrlen));
            if (SOCKET_ERROR == res) {
                int last_error = errno;

                if (last_error != EWOULDBLOCK && last_error != EINPROGRESS) {
                    last_err_msg.append(": ").append(detail::get_socket_error_message(last_error));
                    close();

                    continue;
                }

                res = wait_on_socket(timeout, false);

                if (res < 0 || res == wait_result::TIMEOUT) {
                    is_timeout = true;
                    close();

                    continue;
                }
            }

            break;
        }

        freeaddrinfo(result);

        if (m_socket_handle == SOCKET_ERROR) {
            if (is_timeout)
                return false;

            throw ignite_error(error::code::CONNECTION, last_err_msg);
        }

        return true;
    }

    /**
     * Close established connection.
     */
    void close() override { internal_close(); }

    /**
     * Send data by established connection.
     *
     * @param data Pointer to data to be sent.
     * @param size Size of the data in bytes.
     * @param timeout Timeout.
     * @return Number of bytes that have been sent on success,
     *     wait_result::TIMEOUT on timeout and -errno on failure.
     */
    int send(const std::byte *data, std::size_t size, std::int32_t timeout) override {
        if (!m_blocking) {
            int res = wait_on_socket(timeout, false);

            if (res < 0 || res == wait_result::TIMEOUT)
                return res;
        }

        return int(::send(m_socket_handle, reinterpret_cast<const char *>(data), static_cast<int>(size), MSG_NOSIGNAL));
    }

    /**
     * Receive data from established connection.
     *
     * @param buffer Pointer to data buffer.
     * @param size Size of the buffer in bytes.
     * @param timeout Timeout.
     * @return Number of bytes that have been received on success,
     *     wait_result::TIMEOUT on timeout and -errno on failure.
     */
    int receive(std::byte *buffer, std::size_t size, std::int32_t timeout) override {
        if (!m_blocking) {
            int res = wait_on_socket(timeout, true);

            if (res < 0 || res == wait_result::TIMEOUT)
                return res;
        }

        return int(::recv(m_socket_handle, reinterpret_cast<char *>(buffer), static_cast<int>(size), 0));
    }

    /**
     * Check if the socket is m_blocking or not.
     * @return @c true if the socket is m_blocking and false otherwise.
     */
    [[nodiscard]] bool is_blocking() const override { return m_blocking; }

private:
    /**
     * Close established connection.
     */
    void internal_close() {
        if (m_socket_handle != SOCKET_ERROR) {
            ::close(m_socket_handle);
            m_socket_handle = SOCKET_ERROR;
        }
    }

    /**
     * Wait on the socket for any event for specified time.
     * This function uses poll to achieve timeout functionality for every separate socket operation.
     *
     * @param timeout Timeout.
     * @param rd Wait for read if @c true, or for write if @c false.
     * @return -errno on error, wait_result::TIMEOUT on timeout and wait_result::SUCCESS on success.
     */
    int wait_on_socket(std::int32_t timeout, bool rd) // NOLINT(readability-make-member-function-const)
    {
        return detail::wait_on_socket(m_socket_handle, timeout, rd);
    }

    /** Handle. */
    int m_socket_handle{SOCKET_ERROR};

    /** Blocking flag. */
    bool m_blocking{true};
};

} // namespace ignite::network
