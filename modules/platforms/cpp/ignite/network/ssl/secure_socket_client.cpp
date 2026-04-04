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

#include "ignite/common/detail/defer.h"
#include "ignite/network/network.h"
#include "ignite/network/detail/sockets.h"
#include "ignite/network/ssl/secure_socket_client.h"
#include "ignite/network/ssl/secure_utils.h"

namespace ignite::network {

int secure_socket_client::fill_bio_in(std::int32_t timeout)
{
    auto received = m_socket_client->receive(m_recv_packet.data(), m_recv_packet.size(), timeout);
    if (received <= 0)
        return received;

    m_ssl_conn->feed_input(m_recv_packet.data(), received);
    return received;
}

secure_socket_client::~secure_socket_client()
{
    close_internal();

    if (m_context)
        free_context(reinterpret_cast<SSL_CTX*>(m_context));
}

bool secure_socket_client::connect(const char* hostname, std::uint16_t port, std::int32_t timeout)
{
    ensure_ssl_loaded();

    close_internal();

    if (!m_context)
    {
        m_context = make_context(m_cfg);

        if (!m_context)
            throw_last_secure_error("Can not create SSL context", "Aborting connect");
    }

    if (!m_socket_client->connect(hostname, port, timeout))
        return false;

    auto ssl_cleanup = ::ignite::detail::defer([&] { close_internal(); });

    m_ssl_conn = std::make_unique<ssl_connection>(m_context, hostname);

    bool connected = complete_connect_internal(timeout);
    if (!connected)
        return false;

    m_ssl_conn->verify_peer();

    ssl_cleanup.release();

    return true;
}

void secure_socket_client::close()
{
    close_internal();
}

int secure_socket_client::send(const std::byte* data, std::size_t size, std::int32_t timeout)
{
    if (!m_ssl_conn)
        throw ignite_error(error::code::CONNECTION, "Trying to send data using closed connection");

    int ssl_res;
    do
    {
        ssl_res = m_ssl_conn->write(data, static_cast<int>(size));
        if (ssl_res <= 0)
        {
            if (m_ssl_conn->is_fatal_error(ssl_res))
                return ssl_res;

            int flush_res = flush_bio_out(timeout);
            if (flush_res < 0 || flush_res == wait_result::TIMEOUT)
                return flush_res;

            if (m_ssl_conn->wants_read_input())
            {
                int fill_res = fill_bio_in(timeout);
                if (fill_res < 0 || fill_res == wait_result::TIMEOUT)
                    return fill_res;
            }
        }
    } while (ssl_res <= 0);

    int flush_res = flush_bio_out(timeout);
    if (flush_res < 0 || flush_res == wait_result::TIMEOUT)
        return flush_res;

    return ssl_res;
}

int secure_socket_client::receive(std::byte* buffer, std::size_t size, std::int32_t timeout)
{
    if (!m_ssl_conn)
        throw ignite_error(error::code::CONNECTION, "Trying to receive data using closed connection");

    if (!m_socket_client->is_blocking() && m_ssl_conn->pending_decrypted() == 0)
    {
        int fill_res = fill_bio_in(timeout);
        if (fill_res < 0 || fill_res == wait_result::TIMEOUT)
            return fill_res;
    }

    int ssl_res;
    do
    {
        ssl_res = m_ssl_conn->read(buffer, static_cast<int>(size));
        if (ssl_res <= 0)
        {
            if (m_ssl_conn->is_fatal_error(ssl_res))
                return ssl_res;

            int fill_res = fill_bio_in(timeout);
            if (fill_res < 0 || fill_res == wait_result::TIMEOUT)
                return fill_res;

            int flush_res = flush_bio_out(timeout);
            if (flush_res < 0 || flush_res == wait_result::TIMEOUT)
                return flush_res;
        }
    } while (ssl_res <= 0);

    return ssl_res;
}

void secure_socket_client::close_internal()
{
    m_ssl_conn.reset();
    m_socket_client->close();
}

bool secure_socket_client::complete_connect_internal(std::int32_t timeout)
{
    while (true)
    {
        bool done = m_ssl_conn->do_handshake();

        int flush_res = flush_bio_out(timeout);
        if (flush_res == wait_result::TIMEOUT)
            return false;
        if (flush_res < 0)
            throw_last_secure_error("Error while establishing secure connection");

        if (done)
            break;

        int fill_res = fill_bio_in(timeout);
        if (fill_res == wait_result::TIMEOUT)
            return false;
        if (fill_res < 0)
            throw_last_secure_error("Error while establishing secure connection");
    }

    if (std::string("TLSv1.3") == m_ssl_conn->version())
    {
        // Workaround. Need to get SSL into read state to avoid a deadlock.
        // See https://github.com/openssl/openssl/issues/7967 for details.
        m_ssl_conn->read(nullptr, 0);

        if (wait_result::TIMEOUT == flush_bio_out(timeout))
            return false;

        if (wait_result::TIMEOUT == fill_bio_in(timeout))
            return false;
    }

    return true;
}

int secure_socket_client::flush_bio_out(std::int32_t timeout)
{
    auto data = m_ssl_conn->drain_output();
    if (data.empty())
        return wait_result::SUCCESS;

    std::size_t sent = 0;
    while (sent < data.size())
    {
        int n = m_socket_client->send(data.data() + sent, data.size() - sent, timeout);
        if (n < 0 || n == wait_result::TIMEOUT)
            return n;

        sent += static_cast<std::size_t>(n);
    }

    return wait_result::SUCCESS;
}

} // namespace ignite::network