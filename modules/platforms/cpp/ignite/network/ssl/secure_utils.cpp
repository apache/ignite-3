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

#ifdef _WIN32
#include <wincrypt.h>
#endif

#include <ignite/network/ssl/secure_utils.h>
#include <ignite/network/network.h>

#include <ignite/common/ignite_error.h>
#include <ignite/common/detail/config.h>
#include <ignite/common/detail/defer.h>

#include <sstream>

namespace
{
using namespace ignite::network;


std::string format_error_message(const std::string &description, const std::string &details,
    const std::string &advice)
{
    std::stringstream message_builder;
    message_builder << description;
    if (!details.empty())
        message_builder << ": " << details;

    if (!advice.empty())
        message_builder << ". " << advice;

    return message_builder.str();
}

#ifdef _WIN32
std::string get_last_system_error()
{
    auto error_code = IGNITE_SWITCH_WIN_OTHER(GetLastError(), errno);
    
    std::string error_details;
    if (error_code != IGNITE_SWITCH_WIN_OTHER(ERROR_SUCCESS, 0))
    {
        char err_buf[1024] = { 0 };
        const char* res = err_buf;

#ifdef _WIN32
        FormatMessageA(
            FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, NULL, error_code,
            MAKELANGID(LANG_ENGLISH, SUBLANG_ENGLISH_US), err_buf, sizeof(err_buf), NULL);
#else //  _WIN32
        res = strerror_r(error_code, err_buf, sizeof(err_buf));
#endif // _WIN32

        error_details.assign(res);
    }

    return error_details;
}

std::string get_last_system_error(const std::string& description, const std::string& advice)
{
    return format_error_message(description, get_last_system_error(), advice);
}
#endif

void load_default_ca(SSL_CTX* ssl_context)
{
    assert(ssl_context != nullptr);
    auto& gateway = ssl_gateway::get_instance();

#ifndef _WIN32
    long res = gateway.SSL_CTX_set_default_verify_paths_(ssl_context);
    if (res != SSL_OPERATION_SUCCESS) {
        throw_last_secure_error(
            "Can not set default Certificate Authority for secure connection", "Try setting custom CA");
    }
#else
    auto *ssl_store = gateway.X509_STORE_new_();
    if (!ssl_store)
        throw_last_secure_error("Can not create X509_STORE certificate store", "Try setting custom CA");

    HCERTSTORE sys_store = CertOpenSystemStoreA(NULL, "ROOT");
    if (!sys_store)
        throw_secure_error(get_last_system_error("Can not open System Certificate store for secure connection",
            "Try setting custom CA"));

    PCCERT_CONTEXT cert_iter = CertEnumCertificatesInStore(sys_store, nullptr);
    while (cert_iter)
    {
        const unsigned char *current_cert = cert_iter->pbCertEncoded;
        X509* x509 = gateway.d2i_X509_(nullptr, &current_cert, static_cast<long>(cert_iter->cbCertEncoded));
        if (x509)
        {
            gateway.X509_STORE_add_cert_(ssl_store, x509);

            gateway.X509_free_(x509);
        }
        cert_iter = CertEnumCertificatesInStore(sys_store, cert_iter);
    }

    CertCloseStore(sys_store, 0);

    gateway.SSL_CTX_set_cert_store_(ssl_context, ssl_store);
#endif
}

} // anonymous namespace

namespace ignite::network
{

SSL_CTX* make_context(const secure_configuration& cfg)
{
    ensure_ssl_loaded();

    ssl_gateway &gateway = ssl_gateway::get_instance();

    const SSL_METHOD* method = gateway.SSLv23_client_method_();
    if (!method)
        throw_last_secure_error("Can not get SSL method");

    SSL_CTX* ssl_context = gateway.SSL_CTX_new_(method);
    if (!ssl_context)
        throw_last_secure_error("Can not create new SSL context");

    auto cleanup = ::ignite::detail::defer([&] { free_context(ssl_context); });

    gateway.SSL_CTX_set_verify_(ssl_context, SSL_VERIFY_PEER, nullptr);

    gateway.SSL_CTX_set_verify_depth_(ssl_context, 8);

    gateway.SSL_CTX_set_options_(ssl_context, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_NO_COMPRESSION);

    if (!cfg.ca_path.empty())
    {
        long res = gateway.SSL_CTX_load_verify_locations_(ssl_context, cfg.ca_path.c_str(), nullptr);
        if (res != SSL_OPERATION_SUCCESS)
            throw_last_secure_error("Can not set Certificate Authority path for secure connection, path=" +
                cfg.ca_path);
    }
    else
        load_default_ca(ssl_context);

    if (!cfg.cert_path.empty())
    {
        long res = gateway.SSL_CTX_use_certificate_chain_file_(ssl_context, cfg.cert_path.c_str());
        if (res != SSL_OPERATION_SUCCESS)
            throw_last_secure_error("Can not set client certificate file for secure connection, path=" +
                cfg.cert_path);
    }

    if (!cfg.key_path.empty())
    {
        long res = gateway.SSL_CTX_use_RSAPrivateKey_file_(ssl_context, cfg.key_path.c_str(), SSL_FILETYPE_PEM);
        if (res != SSL_OPERATION_SUCCESS)
            throw_last_secure_error("Can not set private key file for secure connection, path=" + cfg.key_path);
    }

    const char* const PREFERRED_CIPHERS = "HIGH:!aNULL:!kRSA:!PSK:!SRP:!MD5:!RC4";
    long res = gateway.SSL_CTX_set_cipher_list_(ssl_context, PREFERRED_CIPHERS);
    if (res != SSL_OPERATION_SUCCESS)
        throw_last_secure_error("Can not set ciphers list for secure connection");

    cleanup.release();

    return ssl_context;
}

void free_context(SSL_CTX* ctx)
{
    ssl_gateway &gateway = ssl_gateway::get_instance();

    assert(gateway.is_loaded());

    gateway.SSL_CTX_free_(ctx);
}

bool is_actual_error(int err)
{
    switch (err)
    {
        case SSL_ERROR_NONE:
        case SSL_ERROR_WANT_READ:
        case SSL_ERROR_WANT_WRITE:
        case SSL_ERROR_WANT_X509_LOOKUP:
        case SSL_ERROR_WANT_CONNECT:
        case SSL_ERROR_WANT_ACCEPT:
            return false;

        default:
            return true;
    }
}

void throw_secure_error(std::string err)
{
    throw ignite_error(error::code::GENERIC, std::move(err));
}

std::string get_last_secure_error()
{
    ssl_gateway &gateway = ssl_gateway::get_instance();

    assert(gateway.is_loaded());

    unsigned long error_code = gateway.ERR_get_error_();

    std::string error_details;
    if (error_code != 0)
    {
        char err_buf[1024] = { 0 };

        gateway.ERR_error_string_n_(error_code, err_buf, sizeof(err_buf));

        error_details.assign(err_buf);
    }

    return error_details;
}

void throw_last_secure_error(const std::string& description, const std::string& advice)
{
    throw_secure_error(format_error_message(description, get_last_secure_error(), advice));
}

void throw_last_secure_error(const std::string& description)
{
    throw_last_secure_error(description, "");
}

} // namespace ignite::network

