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

#include <ignite/network/ssl/secure_data_filter.h>
#include <ignite/network/ssl/ssl_gateway.h>
#include <ignite/network/ssl/secure_utils.h>
#include <ignite/network/network.h>

#include <iostream>
#include <sstream>
#include <utility>

namespace ignite::network
{

secure_data_filter::secure_data_filter(const secure_configuration &cfg)
{
    ensure_ssl_loaded();

    m_ssl_context = make_context(cfg);
}

secure_data_filter::~secure_data_filter()
{
    free_context(static_cast<SSL_CTX*>(m_ssl_context));
}

bool secure_data_filter::send(std::uint64_t id, std::vector<std::byte> &&data)
{
    auto context = find_context(id);
    if (!context)
        return false;

    return context->send(data);
}

void secure_data_filter::on_connection_success(const end_point &addr, std::uint64_t id)
{
    auto context = std::make_shared<secure_connection_context>(id, addr, *this);

    {
        std::lock_guard<std::mutex> lock(m_context_mutex);

        m_contexts.emplace(std::make_pair(id, context));
    }

    context->do_connect();
}

void secure_data_filter::on_connection_closed(std::uint64_t id, std::optional<ignite_error> err)
{
    auto context = find_context(id);
    if (!context)
        return;

    if (context->is_connected())
        data_filter_adapter::on_connection_closed(id, err);
    else
    {
        std::stringstream ssl_err_msg;
        ssl_err_msg << "Connection closed during SSL/TLS handshake";
        if (err)
            ssl_err_msg << ". Details: " << err->what_str();

        std::string ssl_err_msg_str = ssl_err_msg.str();

        ignite_error err0(ssl_err_msg_str);

        data_filter_adapter::on_connection_error(context->get_address(), err0);
    }

    {
        std::lock_guard<std::mutex> lock(m_context_mutex);

        m_contexts.erase(id);
    }
}

void secure_data_filter::on_message_received(std::uint64_t id, bytes_view msg)
{
    auto context = find_context(id);
    if (!context)
        return;

    data_buffer_ref in(msg);

    while (!in.empty())
    {
        bool connection_happened = context->process_data(in);

        if (connection_happened)
            data_filter_adapter::on_connection_success(context->get_address(), id);

        if (context->is_connected())
        {
            auto data = context->get_pending_decrypted_data();
            while (!data.empty())
            {
                data_filter_adapter::on_message_received(id, data.get_bytes_view());
                data = context->get_pending_decrypted_data();
            }
        }
    }
}

std::shared_ptr<secure_data_filter::secure_connection_context> secure_data_filter::find_context(std::uint64_t id)
{
    std::lock_guard<std::mutex> lock(m_context_mutex);

    auto it = m_contexts.find(id);
    if (it == m_contexts.end())
        return {};

    return it->second;
}

bool secure_data_filter::send_internal(std::uint64_t id, std::vector<std::byte> data)
{
    return data_filter_adapter::send(id, std::move(data));
}

secure_data_filter::secure_connection_context::secure_connection_context(std::uint64_t id, end_point addr,
    secure_data_filter& filter)
    : m_id(id)
    , m_addr(std::move(addr))
    , m_filter(filter)
{
    ssl_gateway &gateway = ssl_gateway::get_instance();

    m_ssl = gateway.SSL_new_(static_cast<SSL_CTX*>(filter.m_ssl_context));
    if (!m_ssl)
        throw_last_secure_error("Can not create secure connection");

    m_bio_in = gateway.BIO_new_(gateway.BIO_s_mem_());
    if (!m_bio_in)
        throw_last_secure_error("Can not create input BIO");

    m_bio_out = gateway.BIO_new_(gateway.BIO_s_mem_());
    if (!m_bio_out)
        throw_last_secure_error("Can not create output BIO");

    gateway.SSL_set_bio_(static_cast<SSL*>(m_ssl), static_cast<BIO*>(m_bio_in), static_cast<BIO*>(m_bio_out));
    gateway.SSL_set_connect_state_(static_cast<SSL*>(m_ssl));
}

secure_data_filter::secure_connection_context::~secure_connection_context()
{
    ssl_gateway &gateway = ssl_gateway::get_instance();

    if (m_ssl)
        gateway.SSL_free_(static_cast<SSL*>(m_ssl));
    else
    {
        if (m_bio_in)
            gateway.BIO_free_all_(static_cast<BIO*>(m_bio_in));

        if (m_bio_out)
            gateway.BIO_free_all_(static_cast<BIO*>(m_bio_out));
    }
}

bool secure_data_filter::secure_connection_context::do_connect()
{
    ssl_gateway &gateway = ssl_gateway::get_instance();

    SSL* ssl0 = static_cast<SSL*>(m_ssl);
    int res = gateway.SSL_connect_(ssl0);

    if (res != SSL_OPERATION_SUCCESS)
    {
        int sslError = gateway.SSL_get_error_(ssl0, res);
        if (is_actual_error(sslError))
            throw_last_secure_error("Can not establish secure connection");
    }

    send_pending_data();

    return res == SSL_OPERATION_SUCCESS;
}

bool secure_data_filter::secure_connection_context::send_pending_data()
{
    auto data = get_pending_data(m_bio_out);

    if (data.empty())
        return false;

    return m_filter.send_internal(m_id, data);
}

bool secure_data_filter::secure_connection_context::send(const std::vector<std::byte> &data)
{
    if (!m_connected)
        return false;

    ssl_gateway &gateway = ssl_gateway::get_instance();

    int res = gateway.SSL_write_(static_cast<SSL*>(m_ssl), data.data(), int(data.size()));
    if (res <= 0)
        return false;

    return send_pending_data();
}

bool secure_data_filter::secure_connection_context::process_data(data_buffer_ref &data)
{
    ssl_gateway &gateway = ssl_gateway::get_instance();
    auto buf = data.get_bytes_view();
    int res = gateway.BIO_write_(static_cast<BIO*>(m_bio_in), buf.data(), int(buf.size()));
    if (res <= 0)
        throw_last_secure_error("Failed to process SSL data");

    data.skip(res);

    send_pending_data();

    if (m_connected)
        return false;

    m_connected = do_connect();

    send_pending_data();

    if (!m_connected)
        return false;

    return true;
}

std::vector<std::byte> secure_data_filter::secure_connection_context::get_pending_data(void* bio)
{
    ssl_gateway &gateway = ssl_gateway::get_instance();

    BIO *bio0 = static_cast<BIO*>(bio);
    int available = gateway.BIO_pending_(bio0);
    if (available <= 0)
        return {};

    std::vector<std::byte> buffer(available, {});
    int res = gateway.BIO_read_(bio0, buffer.data(), int(buffer.size()));
    if (res <= 0)
        return {};

    return std::move(buffer);
}

data_buffer_ref secure_data_filter::secure_connection_context::get_pending_decrypted_data()
{
    ssl_gateway &gateway = ssl_gateway::get_instance();

    SSL *ssl0 = static_cast<SSL*>(m_ssl);
    int res = gateway.SSL_read_(ssl0, m_recv_buffer.data(), int(m_recv_buffer.size()));
    if (res <= 0)
        return {};

    return data_buffer_ref{m_recv_buffer, 0, std::size_t(res)};
}

}
