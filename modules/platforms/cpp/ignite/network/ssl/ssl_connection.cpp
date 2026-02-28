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

#include <ignite/network/ssl/ssl_connection.h>
#include <ignite/network/ssl/ssl_gateway.h>
#include <ignite/network/ssl/secure_utils.h>

#include <ignite/common/detail/defer.h>

namespace ignite::network
{

ssl_connection::ssl_connection(void* ctx, const char* hostname)
{
    ssl_gateway& gw = ssl_gateway::get_instance();

    m_ssl = gw.SSL_new_(static_cast<SSL_CTX*>(ctx));
    if (!m_ssl)
        throw_last_secure_error("Can not create secure connection");

    auto ssl_guard = detail::defer([&] {
        gw.SSL_free_(static_cast<SSL*>(m_ssl));
        m_ssl = nullptr;
    });

    m_bio_in = gw.BIO_new_(gw.BIO_s_mem_());
    if (!m_bio_in)
        throw_last_secure_error("Can not create input BIO");

    auto bio_in_guard = detail::defer([&] {
        gw.BIO_free_all_(static_cast<BIO*>(m_bio_in));
        m_bio_in = nullptr;
    });

    m_bio_out = gw.BIO_new_(gw.BIO_s_mem_());
    if (!m_bio_out)
        throw_last_secure_error("Can not create output BIO");

    // SSL now owns both BIOs; ssl_free will free them.
    gw.SSL_set_bio_(static_cast<SSL*>(m_ssl), static_cast<BIO*>(m_bio_in), static_cast<BIO*>(m_bio_out));
    bio_in_guard.release();

    if (hostname)
    {
        int res = gw.SSL_set_tlsext_host_name_(static_cast<SSL*>(m_ssl), hostname);
        if (res != SSL_OPERATION_SUCCESS)
            throw_last_secure_error("Can not set host name for secure connection");
    }

    gw.SSL_set_connect_state_(static_cast<SSL*>(m_ssl));

    ssl_guard.release();
}

ssl_connection::~ssl_connection()
{
    ssl_gateway& gw = ssl_gateway::get_instance();
    if (gw.is_loaded() && m_ssl)
        gw.SSL_free_(static_cast<SSL*>(m_ssl)); // also frees bio_in and bio_out
}

bool ssl_connection::do_handshake()
{
    ssl_gateway& gw = ssl_gateway::get_instance();
    SSL* ssl = static_cast<SSL*>(m_ssl);

    int res = gw.SSL_connect_(ssl);
    if (res == SSL_OPERATION_SUCCESS)
        return true;

    if (is_actual_error(gw.SSL_get_error_(ssl, res)))
        throw_last_secure_error("Can not establish secure connection");

    return false;
}

int ssl_connection::write(const std::byte* data, int len)
{
    ssl_gateway& gw = ssl_gateway::get_instance();
    return gw.SSL_write_(static_cast<SSL*>(m_ssl), data, len);
}

int ssl_connection::read(std::byte* buf, int len)
{
    ssl_gateway& gw = ssl_gateway::get_instance();
    return gw.SSL_read_(static_cast<SSL*>(m_ssl), buf, len);
}

int ssl_connection::pending_decrypted() const
{
    ssl_gateway& gw = ssl_gateway::get_instance();
    return gw.SSL_pending_(static_cast<const SSL*>(m_ssl));
}

void ssl_connection::feed_input(const void* data, int len)
{
    ssl_gateway& gw = ssl_gateway::get_instance();
    if (gw.BIO_write_(static_cast<BIO*>(m_bio_in), data, len) <= 0)
        throw_last_secure_error("Failed to process SSL data");
}

std::vector<std::byte> ssl_connection::drain_output()
{
    ssl_gateway& gw = ssl_gateway::get_instance();
    BIO* bio_out = static_cast<BIO*>(m_bio_out);

    int available = gw.BIO_pending_(bio_out);
    if (available <= 0)
        return {};

    std::vector<std::byte> buf(available);
    int res = gw.BIO_read_(bio_out, buf.data(), available);
    if (res <= 0)
        return {};

    buf.resize(res);
    return buf;
}

const char* ssl_connection::version() const
{
    ssl_gateway& gw = ssl_gateway::get_instance();
    return gw.SSL_get_version_(static_cast<const SSL*>(m_ssl));
}

bool ssl_connection::is_fatal_error(int ssl_op_result) const
{
    ssl_gateway& gw = ssl_gateway::get_instance();
    return is_actual_error(gw.SSL_get_error_(static_cast<SSL*>(m_ssl), ssl_op_result));
}

bool ssl_connection::wants_read_input() const
{
    ssl_gateway& gw = ssl_gateway::get_instance();
    return gw.SSL_want_(static_cast<SSL*>(m_ssl)) == SSL_READING;
}

void ssl_connection::verify_peer() const
{
    ssl_gateway& gw = ssl_gateway::get_instance();
    SSL* ssl = static_cast<SSL*>(m_ssl);

    X509* cert = gw.SSL_get_peer_certificate_(ssl);
    if (cert)
        gw.X509_free_(cert);
    else
        throw_last_secure_error("Remote host did not provide certificate");

    long res = gw.SSL_get_verify_result_(ssl);
    if (X509_V_OK != res)
        throw_last_secure_error("Certificate chain verification failed");
}

} // namespace ignite::network
