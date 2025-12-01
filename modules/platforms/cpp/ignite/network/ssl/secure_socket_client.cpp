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

#include "ignite/network/detail/sockets.h"
#include "ignite/network/detail/utils.h"
#include "ignite/network/ssl/secure_socket_client.h"
#include "ignite/network/ssl/secure_utils.h"
#include "ignite/network/ssl/ssl_gateway.h"

#include "ignite/common/detail/defer.h"

#include <sstream>
#include <cassert>

namespace {

using namespace ignite::network;

void free_bio(BIO* bio)
{
    ssl_gateway &gateway = ssl_gateway::get_instance();

    assert(gateway.is_loaded());

    gateway.BIO_free_all_(bio);
}

SSL* ssl_from_bio_no_check(void* bio) {
    ssl_gateway &gateway = ssl_gateway::get_instance();
    SSL* ssl = nullptr;
    gateway.BIO_get_ssl_(reinterpret_cast<BIO*>(bio), &ssl);
    return ssl;
}

SSL* ssl_from_bio(void* bio, ignite::error::code on_fail_code, const std::string& on_fail_msg) {
    SSL* ssl = ssl_from_bio_no_check(bio);
    if (!ssl)
        throw ignite::ignite_error(on_fail_code, on_fail_msg);

    return ssl;
}

SSL* ssl_from_bio(void* bio, const std::string& on_fail_msg) {
    SSL* ssl = ssl_from_bio_no_check(bio);
    if (!ssl)
        throw_last_secure_error(on_fail_msg);

    return ssl;
}

} // anonymous namespace

namespace ignite::network
{

secure_socket_client::~secure_socket_client()
{
    close_internal();

    if (m_context)
        free_context(reinterpret_cast<SSL_CTX*>(m_context));
}

bool secure_socket_client::connect(const char* hostname, std::uint16_t port, std::int32_t timeout)
{
    ssl_gateway &gateway = ssl_gateway::get_instance();

    assert(gateway.is_loaded());

    close_internal();

    if (!m_context)
    {
        m_context = make_context(m_cfg);

        if (!m_context)
            throw_last_secure_error("Can not create SSL context", "Aborting connect");
    }

    m_bio = make_ssl(m_context, hostname, port, m_blocking);

    auto cleanup = ::ignite::detail::defer([&] { close_internal(); });

    SSL* ssl = ssl_from_bio(m_bio, "Can not get SSL instance from BIO");

    int res = gateway.SSL_set_tlsext_host_name_(ssl, hostname);

    if (res != SSL_OPERATION_SUCCESS)
        throw_last_secure_error("Can not set host name for secure connection");

    gateway.SSL_set_connect_state_(ssl);

    bool connected = complete_connect_internal(ssl, timeout);

    if (!connected)
        return false;

    // Verify a server certificate was presented during the negotiation
    X509* cert = gateway.SSL_get_peer_certificate_(ssl);
    if (cert)
        gateway.X509_free_(cert);
    else
        throw_last_secure_error("Remote host did not provide certificate");

    // Verify the result of chain verification.
    // Verification performed according to RFC 4158.
    res = gateway.SSL_get_verify_result_(ssl);
    if (X509_V_OK != res)
        throw_last_secure_error("Certificate chain verification failed");

    res = wait_on_socket(ssl, timeout, false);

    if (res == wait_result::TIMEOUT)
        return false;

    if (res != wait_result::SUCCESS)
        throw_last_secure_error("Error while establishing secure connection");

    cleanup.release();

    return true;
}

void secure_socket_client::close()
{
    close_internal();
}

int secure_socket_client::send(const std::byte* data, std::size_t size, std::int32_t timeout)
{
    ssl_gateway &gateway = ssl_gateway::get_instance();

    assert(gateway.is_loaded());

    SSL* ssl = ssl_from_bio(m_bio, error::code::CONNECTION, "Trying to send data using closed connection");

    int res = wait_on_socket(ssl, timeout, false);

    if (res == wait_result::TIMEOUT)
        return res;

    do {
        res = gateway.SSL_write_(ssl, data, static_cast<int>(size));

        int waitRes = wait_on_socket_if_needed(res, ssl, timeout);
        if (waitRes <= 0)
            return waitRes;
    } while (res <= 0);

    return res;
}

int secure_socket_client::receive(std::byte* buffer, std::size_t size, std::int32_t timeout)
{
    ssl_gateway &gateway = ssl_gateway::get_instance();

    assert(gateway.is_loaded());

    SSL* ssl = ssl_from_bio(m_bio, error::code::CONNECTION, "Trying to receive data using closed connection");

    int res = 0;
    if (!m_blocking && gateway.SSL_pending_(ssl) == 0)
    {
        res = wait_on_socket(ssl, timeout, true);

        if (res < 0 || res == wait_result::TIMEOUT)
            return res;
    }

    do {
        res = gateway.SSL_read_(ssl, buffer, static_cast<int>(size));

        int waitRes = wait_on_socket_if_needed(res, ssl, timeout);
        if (waitRes <= 0)
            return waitRes;
    } while (res <= 0);

    return res;
}

void* secure_socket_client::make_ssl(void* context, const char* hostname, std::uint16_t port, bool& blocking)
{
    ssl_gateway &gateway = ssl_gateway::get_instance();

    assert(gateway.is_loaded());

    BIO* bio = gateway.BIO_new_ssl_connect_(reinterpret_cast<SSL_CTX*>(context));
    if (!bio)
        throw_last_secure_error("Can not create SSL connection");

    auto cleanup = ::ignite::detail::defer([&] { free_bio(bio); });

    blocking = gateway.BIO_set_nbio_(bio, 1) != SSL_OPERATION_SUCCESS;

    std::stringstream stream;
    stream << hostname << ":" << port;

    std::string address = stream.str();

    long res = gateway.BIO_set_conn_hostname_(bio, address.c_str());
    if (res != SSL_OPERATION_SUCCESS)
        throw_last_secure_error("Can not set SSL connection hostname");

    cleanup.release();

    return bio;
}

bool secure_socket_client::complete_connect_internal(void* m_ssl, int timeout)
{
    ssl_gateway &gateway = ssl_gateway::get_instance();

    assert(gateway.is_loaded());

    SSL* ssl0 = reinterpret_cast<SSL*>(m_ssl);

    while (true)
    {
        int res = gateway.SSL_connect_(ssl0);

        if (res == SSL_OPERATION_SUCCESS)
            break;

        int sslError = gateway.SSL_get_error_(ssl0, res);

        if (is_actual_error(sslError))
            throw_last_secure_error("Can not establish secure connection");

        int want = gateway.SSL_want_(ssl0);

        res = wait_on_socket(m_ssl, timeout, want == SSL_READING);

        if (res == wait_result::TIMEOUT)
            return false;

        if (res != wait_result::SUCCESS)
            throw_last_secure_error("Error while establishing secure connection");
    }

    if (std::string("TLSv1.3") == gateway.SSL_get_version_(ssl0))
    {
        // Workaround. Need to get SSL into read state to avoid a deadlock.
        // See https://github.com/openssl/openssl/issues/7967 for details.
        gateway.SSL_read_(ssl0, nullptr, 0);
        int res = wait_on_socket(m_ssl, timeout, true);

        if (res == wait_result::TIMEOUT)
            return false;
    }

    return true;
}

void secure_socket_client::close_internal()
{
    ssl_gateway &gateway = ssl_gateway::get_instance();

    if (gateway.is_loaded() && m_bio)
    {
        gateway.BIO_free_all_(reinterpret_cast<BIO*>(m_bio));

        m_bio = nullptr;
    }
}

int secure_socket_client::wait_on_socket(void* ssl, std::int32_t timeout, bool rd)
{
    ssl_gateway &gateway = ssl_gateway::get_instance();

    assert(gateway.is_loaded());

    SSL* ssl0 = reinterpret_cast<SSL*>(ssl);
    int fd = gateway.SSL_get_fd_(ssl0);

    if (fd < 0)
    {
        std::stringstream ss;
        ss << "Can not get file descriptor from the SSL socket, fd=" << fd;

        throw_last_secure_error(ss.str());
    }

    return detail::wait_on_socket(fd, timeout, rd);
}

int secure_socket_client::wait_on_socket_if_needed(int res, void* ssl, int timeout)
{
    ssl_gateway &gateway = ssl_gateway::get_instance();

    assert(gateway.is_loaded());

    SSL* ssl0 = reinterpret_cast<SSL*>(ssl);

    if (res <= 0)
    {
        int err = gateway.SSL_get_error_(ssl0, res);
        if (is_actual_error(err))
            return res;

        int want = gateway.SSL_want_(ssl0);
        int wait_res = wait_on_socket(ssl, timeout, want == SSL_READING);
        if (wait_res < 0 || wait_res == wait_result::TIMEOUT)
            return wait_res;
    }

    return wait_result::SUCCESS;
}

} // namespace ignite::network
