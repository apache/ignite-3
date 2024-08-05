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
# include "ignite/network/detail/win/dynamic_module.h"
#else
# include "ignite/network/detail/linux/dynamic_module.h"
#endif

#include "ignite/network/ssl/ssl_gateway.h"

#include "ignite/common/ignite_error.h"
#include "ignite/common/detail/defer.h"
#include "ignite/common/detail/config.h"
#include "ignite/common/detail/utils.h"

#include <sstream>
#include <filesystem>
#include <cassert>

#ifndef ADDITIONAL_OPENSSL_HOME_ENV
#   define ADDITIONAL_OPENSSL_HOME_ENV "OPEN_SSL_HOME"
#endif // ADDITIONAL_OPENSSL_HOME_ENV

#ifndef SSL_CTRL_OPTIONS
#   define SSL_CTRL_OPTIONS 32
#endif // SSL_CTRL_OPTIONS

#ifndef OPENSSL_INIT_LOAD_SSL_STRINGS
#   define OPENSSL_INIT_LOAD_SSL_STRINGS 0x00200000L
#endif // OPENSSL_INIT_LOAD_SSL_STRINGS

#ifndef OPENSSL_INIT_LOAD_CRYPTO_STRINGS
#   define OPENSSL_INIT_LOAD_CRYPTO_STRINGS 0x00000002L
#endif // OPENSSL_INIT_LOAD_CRYPTO_STRINGS


namespace ignite::network
{

ssl_gateway::ssl_gateway()
{
    memset(&m_functions, 0, sizeof(m_functions));
}

void ssl_gateway::unload_all()
{
    if (m_libeay32)
        m_libeay32->unload();

    if (m_ssleay32)
        m_ssleay32->unload();

    if (m_libssl)
        m_libssl->unload();

    if (m_libcrypto)
        m_libcrypto->unload();

    memset(&m_functions, 0, sizeof(m_functions));
}

std::unique_ptr<dynamic_module> ssl_gateway::load_ssl_library(const std::string& name, const std::string& home_dir)
{
    constexpr const char* lib_file_extension = IGNITE_SWITCH_WIN_OTHER(".dll", ".so");
    constexpr const char* bin_sub_dir = IGNITE_SWITCH_WIN_OTHER("bin", "lib");

    std::string full_name = name + lib_file_extension;

    if (!home_dir.empty())
    {
        auto path = std::filesystem::path{home_dir} / bin_sub_dir / full_name;

        return std::make_unique<dynamic_module>(load_module(path));
    }

    return std::make_unique<dynamic_module>(load_module(full_name));
}

void ssl_gateway::load_ssl_libraries()
{
    auto home = ignite::detail::get_env(ADDITIONAL_OPENSSL_HOME_ENV);
    if (!home)
        home = ignite::detail::get_env("OPENSSL_HOME");

    bool is_loaded = false;

    if (home)
        is_loaded = try_load_ssl_libraries(*home);

    // Try to load from a system path.
    if (!is_loaded)
        is_loaded = try_load_ssl_libraries("");

    if (!is_loaded)
    {
#ifdef _WIN32
        std::stringstream ss;

        ss << "Can not load necessary OpenSSL libraries:";

        if (!m_libssl->is_loaded() || !m_libcrypto->is_loaded())
        {
            if (!m_libssl->is_loaded())
                ss << " libssl";

            if (!m_libcrypto->is_loaded())
                ss << " libcrypto";
        }
        else
        {
            if (!m_libeay32->is_loaded())
                ss << " libeay32";

            if (!m_ssleay32->is_loaded())
                ss << " ssleay32";
        }

        throw ignite_error(ss.str());
#else
        if (!m_libssl->is_loaded())
            throw ignite_error("Can not load necessary OpenSSL library: libssl");
#endif
    }
}

bool ssl_gateway::try_load_ssl_libraries(const std::string& home_dir)
{
#ifdef _WIN32
#ifdef _WIN64
#define SSL_LIB_PLATFORM_POSTFIX "-x64"
#else
#define SSL_LIB_PLATFORM_POSTFIX ""
#endif
    m_libcrypto = load_ssl_library("libcrypto-3" SSL_LIB_PLATFORM_POSTFIX, home_dir);
    m_libssl = load_ssl_library("libssl-3" SSL_LIB_PLATFORM_POSTFIX, home_dir);

    if (!m_libssl->is_loaded() || !m_libcrypto->is_loaded())
    {
        m_libcrypto = load_ssl_library("libcrypto-1_1" SSL_LIB_PLATFORM_POSTFIX, home_dir);
        m_libssl = load_ssl_library("libssl-1_1" SSL_LIB_PLATFORM_POSTFIX, home_dir);
    }

    if (!m_libssl->is_loaded() || !m_libcrypto->is_loaded())
    {
        m_libeay32 = load_ssl_library("libeay32", home_dir);
        m_ssleay32 = load_ssl_library("ssleay32", home_dir);
    }

    return (m_libssl->is_loaded() && m_libcrypto->is_loaded()) || (m_libeay32->is_loaded() && m_ssleay32->is_loaded());
#else
    m_libssl = load_ssl_library("libssl", home_dir);

    return m_libssl->is_loaded();
#endif
}

void ssl_gateway::load_mandatory_methods()
{
    m_functions.fpSSLeay_version = try_load_ssl_method("SSLeay_version");

    if (!m_functions.fpSSLeay_version)
        m_functions.fpOpenSSL_version = load_ssl_method("OpenSSL_version");

    m_functions.fpSSL_library_init = try_load_ssl_method("SSL_library_init");
    m_functions.fpSSL_load_error_strings = try_load_ssl_method("SSL_load_error_strings");

    if (!m_functions.fpSSL_library_init || !m_functions.fpSSL_load_error_strings)
        m_functions.fpOPENSSL_init_ssl = load_ssl_method("OPENSSL_init_ssl");

    m_functions.fpSSLv23_client_method = try_load_ssl_method("SSLv23_client_method");

    if (!m_functions.fpSSLv23_client_method)
        m_functions.fpTLS_client_method = load_ssl_method("TLS_client_method");

    m_functions.fpSSL_CTX_new = load_ssl_method("SSL_CTX_new");
    m_functions.fpSSL_CTX_free = load_ssl_method("SSL_CTX_free");
    m_functions.fpSSL_CTX_set_verify = load_ssl_method("SSL_CTX_set_verify");
    m_functions.fpSSL_CTX_set_verify_depth = load_ssl_method("SSL_CTX_set_verify_depth");
    m_functions.fpSSL_CTX_set_cert_store = load_ssl_method("SSL_CTX_set_cert_store");
    m_functions.fpSSL_CTX_set_default_verify_paths = load_ssl_method("SSL_CTX_set_default_verify_paths");
    m_functions.fpSSL_CTX_load_verify_locations = load_ssl_method("SSL_CTX_load_verify_locations");
    m_functions.fpSSL_CTX_use_certificate_chain_file = load_ssl_method("SSL_CTX_use_certificate_chain_file");
    m_functions.fpSSL_CTX_use_RSAPrivateKey_file = load_ssl_method("SSL_CTX_use_RSAPrivateKey_file");
    m_functions.fpSSL_CTX_set_cipher_list = load_ssl_method("SSL_CTX_set_cipher_list");

    m_functions.fpSSL_get_verify_result = load_ssl_method("SSL_get_verify_result");

    m_functions.fpSSL_get_peer_certificate = try_load_ssl_method("SSL_get_peer_certificate");
    // OpenSSL >= 3.0.0
    if (!m_functions.fpSSL_get_peer_certificate)
        m_functions.fpSSL_get_peer_certificate = load_ssl_method("SSL_get1_peer_certificate");

    m_functions.fpSSL_ctrl = load_ssl_method("SSL_ctrl");
    m_functions.fpSSL_CTX_ctrl = load_ssl_method("SSL_CTX_ctrl");

    m_functions.fpSSL_set_connect_state = load_ssl_method("SSL_set_connect_state");
    m_functions.fpSSL_connect = load_ssl_method("SSL_connect");
    m_functions.fpSSL_set_bio = load_ssl_method("SSL_set_bio");
    m_functions.fpSSL_get_error = load_ssl_method("SSL_get_error");
    m_functions.fpSSL_want = load_ssl_method("SSL_want");
    m_functions.fpSSL_write = load_ssl_method("SSL_write");
    m_functions.fpSSL_read = load_ssl_method("SSL_read");
    m_functions.fpSSL_pending = load_ssl_method("SSL_pending");
    m_functions.fpSSL_get_version = load_ssl_method("SSL_get_version");

    m_functions.fpSSL_get_fd = load_ssl_method("SSL_get_fd");
    m_functions.fpSSL_new = load_ssl_method("SSL_new");
    m_functions.fpSSL_free = load_ssl_method("SSL_free");
    m_functions.fpBIO_new = load_ssl_method("BIO_new");
    m_functions.fpBIO_new_ssl_connect = load_ssl_method("BIO_new_ssl_connect");
    m_functions.fpBIO_s_mem = load_ssl_method("BIO_s_mem");
    m_functions.fpBIO_read = load_ssl_method("BIO_read");
    m_functions.fpBIO_write = load_ssl_method("BIO_write");


    m_functions.fpOPENSSL_config = load_ssl_method("OPENSSL_config");
    m_functions.fpX509_STORE_new = load_ssl_method("X509_STORE_new");
    m_functions.fpX509_STORE_add_cert = load_ssl_method("X509_STORE_add_cert");
    m_functions.fpd2i_X509 = load_ssl_method("d2i_X509");
    m_functions.fpX509_free = load_ssl_method("X509_free");

    m_functions.fpBIO_free_all = load_ssl_method("BIO_free_all");
    m_functions.fpBIO_ctrl = load_ssl_method("BIO_ctrl");

    m_functions.fpERR_get_error = load_ssl_method("ERR_get_error");
    m_functions.fpERR_error_string_n = load_ssl_method("ERR_error_string_n");
}

ssl_gateway& ssl_gateway::get_instance()
{
    static ssl_gateway self;

    return self;
}

void ssl_gateway::load_all()
{
    if (m_inited)
        return;

    std::lock_guard<std::mutex> lock(m_init_mutex);

    if (m_inited)
        return;

    auto cleanup = ignite::detail::defer([&] { unload_all(); });

    load_ssl_libraries();

    load_mandatory_methods();

    m_functions.fpSSL_CTX_set_options = try_load_ssl_method("SSL_CTX_set_options");

    UNUSED_VALUE SSL_library_init_();

    SSL_load_error_strings_();

    OPENSSL_config_(nullptr);

    cleanup.release();

    m_inited = true;
}

void* ssl_gateway::try_load_ssl_method(const char* name)
{
    void* fp = nullptr;
    if (m_libeay32)
        fp = m_libeay32->find_symbol(name);

    if (!fp && m_ssleay32)
        fp = m_ssleay32->find_symbol(name);

    if (!fp && m_libcrypto)
        fp = m_libcrypto->find_symbol(name);

    if (!fp && m_libssl)
        fp = m_libssl->find_symbol(name);

    return fp;
}

void* ssl_gateway::load_ssl_method(const char* name)
{
    void* fp = try_load_ssl_method(name);

    if (!fp) {
        throw ignite_error(std::string("Can not load function ") + name);
    }

    return fp;
}

[[maybe_unused]] char* ssl_gateway::OpenSSL_version_(int type)
{
    typedef char* (func_type)(int);

    func_type* fp;

    if (m_functions.fpSSLeay_version)
        fp = reinterpret_cast<func_type*>(m_functions.fpSSLeay_version);
    else
        fp = reinterpret_cast<func_type*>(m_functions.fpOpenSSL_version);

    assert(fp != nullptr);

    return fp(type);
}

int ssl_gateway::OPENSSL_init_ssl_(std::uint64_t opts, const void* settings)
{
    assert(m_functions.fpOPENSSL_init_ssl != nullptr);

    typedef int (func_type)(std::uint64_t, const void*);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpOPENSSL_init_ssl);

    return fp(opts, settings);
}

long ssl_gateway::SSL_CTX_set_options_(SSL_CTX* ctx, long options)
{
    if (m_functions.fpSSL_CTX_set_options)
    {
        typedef long (func_type)(SSL_CTX*, long);

        auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_CTX_set_options);

        return fp(ctx, options);
    }

    return SSL_CTX_ctrl_(ctx, SSL_CTRL_OPTIONS, options, nullptr);
}

long ssl_gateway::SSL_CTX_ctrl_(SSL_CTX* ctx, int cmd, long larg, void* parg)
{
    assert(m_functions.fpSSL_CTX_ctrl != nullptr);

    typedef long (func_type)(SSL_CTX*, int, long, void*);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_CTX_ctrl);

    return fp(ctx, cmd, larg, parg);
}

SSL_CTX* ssl_gateway::SSL_CTX_new_(const SSL_METHOD* meth)
{
    assert(m_functions.fpSSL_CTX_new != nullptr);

    typedef SSL_CTX*(func_type)(const SSL_METHOD*);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_CTX_new);

    return fp(meth);
}

void ssl_gateway::SSL_CTX_free_(SSL_CTX* ctx)
{
    assert(m_functions.fpSSL_CTX_free != nullptr);

    typedef void (func_type)(SSL_CTX*);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_CTX_free);

    fp(ctx);
}

void ssl_gateway::SSL_CTX_set_verify_(SSL_CTX* ctx, int mode, int (* callback)(int, X509_STORE_CTX*))
{
    assert(m_functions.fpSSL_CTX_set_verify != nullptr);

    typedef void (func_type)(SSL_CTX*, int, int (*)(int, X509_STORE_CTX*));

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_CTX_set_verify);

    fp(ctx, mode, callback);
}

void ssl_gateway::SSL_CTX_set_verify_depth_(SSL_CTX* ctx, int depth)
{
    assert(m_functions.fpSSL_CTX_set_verify_depth != nullptr);

    typedef void (func_type)(SSL_CTX*, int);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_CTX_set_verify_depth);

    fp(ctx, depth);
}

void ssl_gateway::SSL_CTX_set_cert_store_(SSL_CTX* ctx, X509_STORE* store)
{
    assert(m_functions.fpSSL_CTX_set_cert_store != nullptr);

    typedef int (func_type)(SSL_CTX*, X509_STORE*);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_CTX_set_cert_store);

    fp(ctx, store);
}

int ssl_gateway::SSL_CTX_set_default_verify_paths_(SSL_CTX* ctx)
{
    assert(m_functions.fpSSL_CTX_set_default_verify_paths != nullptr);

    typedef int (func_type)(SSL_CTX*);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_CTX_set_default_verify_paths);

    return fp(ctx);
}

int ssl_gateway::SSL_CTX_load_verify_locations_(SSL_CTX* ctx, const char* cAfile, const char* cApath)
{
    assert(m_functions.fpSSL_CTX_load_verify_locations != nullptr);

    typedef int (func_type)(SSL_CTX*, const char*, const char*);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_CTX_load_verify_locations);

    return fp(ctx, cAfile, cApath);
}

int ssl_gateway::SSL_CTX_use_certificate_chain_file_(SSL_CTX* ctx, const char* file)
{
    assert(m_functions.fpSSL_CTX_use_certificate_chain_file != nullptr);

    typedef int (func_type)(SSL_CTX*, const char*);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_CTX_use_certificate_chain_file);

    return fp(ctx, file);
}

int ssl_gateway::SSL_CTX_use_RSAPrivateKey_file_(SSL_CTX* ctx, const char* file, int type)
{
    assert(m_functions.fpSSL_CTX_use_RSAPrivateKey_file != nullptr);

    typedef int (func_type)(SSL_CTX*, const char*, int);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_CTX_use_RSAPrivateKey_file);

    return fp(ctx, file, type);
}

int ssl_gateway::SSL_CTX_set_cipher_list_(SSL_CTX* ctx, const char* str)
{
    assert(m_functions.fpSSL_CTX_set_cipher_list != nullptr);

    typedef int (func_type)(SSL_CTX*, const char*);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_CTX_set_cipher_list);

    return fp(ctx, str);
}

long ssl_gateway::SSL_get_verify_result_(const SSL* s)
{
    assert(m_functions.fpSSL_get_verify_result != nullptr);

    typedef long (func_type)(const SSL*);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_get_verify_result);

    return fp(s);
}

int ssl_gateway::SSL_library_init_()
{
    typedef int (func_type)();

    if (m_functions.fpSSL_library_init)
    {
        auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_library_init);

        return fp();
    }

    return OPENSSL_init_ssl_(0, nullptr);
}

void ssl_gateway::SSL_load_error_strings_()
{
    typedef void (func_type)();

    if (m_functions.fpSSL_load_error_strings)
    {
        auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_load_error_strings);

        fp();

        return;
    }

    OPENSSL_init_ssl_(OPENSSL_INIT_LOAD_SSL_STRINGS | OPENSSL_INIT_LOAD_CRYPTO_STRINGS, nullptr);
}

X509* ssl_gateway::SSL_get_peer_certificate_(const SSL* s)
{
    assert(m_functions.fpSSL_get_peer_certificate != nullptr);

    typedef X509*(func_type)(const SSL*);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_get_peer_certificate);

    return fp(s);
}

long ssl_gateway::SSL_ctrl_(SSL* s, int cmd, long larg, void* parg)
{
    assert(m_functions.fpSSL_ctrl != nullptr);

    typedef long (func_type)(SSL*, int, long, void*);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_ctrl);

    return fp(s, cmd, larg, parg);
}

long ssl_gateway::SSL_set_tlsext_host_name_(SSL* s, const char* name)
{
    return SSL_ctrl_(s, SSL_CTRL_SET_TLSEXT_HOSTNAME, TLSEXT_NAMETYPE_host_name, const_cast<char*>(name));
}

void ssl_gateway::SSL_set_connect_state_(SSL* s)
{
    assert(m_functions.fpSSL_set_connect_state != nullptr);

    typedef void (func_type)(SSL*);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_set_connect_state);

    return fp(s);
}

int ssl_gateway::SSL_connect_(SSL* s)
{
    assert(m_functions.fpSSL_connect != nullptr);

    typedef int (func_type)(SSL*);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_connect);

    return fp(s);
}

void ssl_gateway::SSL_set_bio_(SSL* s, BIO* rbio, BIO* wbio)
{
    assert(m_functions.fpSSL_set_bio != nullptr);

    typedef void (func_type)(SSL*, BIO*, BIO*);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_set_bio);

    fp(s, rbio, wbio);
}

int ssl_gateway::SSL_get_error_(const SSL* s, int ret)
{
    assert(m_functions.fpSSL_get_error != nullptr);

    typedef int (func_type)(const SSL*, int);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_get_error);

    return fp(s, ret);
}

int ssl_gateway::SSL_want_(const SSL* s)
{
    assert(m_functions.fpSSL_want != nullptr);

    typedef int (func_type)(const SSL*);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_want);

    return fp(s);
}

int ssl_gateway::SSL_write_(SSL* s, const void* buf, int num)
{
    assert(m_functions.fpSSL_write != nullptr);

    typedef int (func_type)(SSL*, const void*, int);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_write);

    return fp(s, buf, num);
}

int ssl_gateway::SSL_read_(SSL* s, void* buf, int num)
{
    assert(m_functions.fpSSL_read != nullptr);

    typedef int (func_type)(SSL*, void*, int);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_read);

    return fp(s, buf, num);
}

int ssl_gateway::SSL_pending_(const SSL* ssl)
{
    assert(m_functions.fpSSL_pending != nullptr);

    typedef int (func_type)(const SSL*);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_pending);

    return fp(ssl);
}

const char* ssl_gateway::SSL_get_version_(const SSL* ssl)
{
    assert(m_functions.fpSSL_get_version != nullptr);

    typedef const char* (func_type)(const SSL*);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_get_version);

    return fp(ssl);
}

int ssl_gateway::SSL_get_fd_(const SSL* ssl)
{
    assert(m_functions.fpSSL_get_fd != nullptr);

    typedef int (func_type)(const SSL*);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_get_fd);

    return fp(ssl);
}

SSL* ssl_gateway::SSL_new_(SSL_CTX* ctx)
{
    assert(m_functions.fpSSL_new != nullptr);

    typedef SSL* (func_type)(SSL_CTX*);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_new);

    return fp(ctx);
}

void ssl_gateway::SSL_free_(SSL* ssl)
{
    assert(m_functions.fpSSL_free != nullptr);

    typedef void (func_type)(SSL*);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSL_free);

    fp(ssl);
}

const SSL_METHOD* ssl_gateway::SSLv23_client_method_()
{
    if (m_functions.fpSSLv23_client_method)
    {
        typedef const SSL_METHOD*(func_type)();

        auto* fp = reinterpret_cast<func_type*>(m_functions.fpSSLv23_client_method);

        return fp();
    }

    return TLS_client_method_();
}

const SSL_METHOD* ssl_gateway::TLS_client_method_()
{
    assert(m_functions.fpTLS_client_method != nullptr);

    typedef const SSL_METHOD*(func_type)();

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpTLS_client_method);

    return fp();
}

void ssl_gateway::OPENSSL_config_(const char* configName)
{
    assert(m_functions.fpOPENSSL_config != nullptr);

    typedef void (func_type)(const char*);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpOPENSSL_config);

    fp(configName);
}

X509_STORE* ssl_gateway::X509_STORE_new_()
{
    assert(m_functions.fpX509_STORE_new != nullptr);

    typedef X509_STORE*(func_type)();

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpX509_STORE_new);

    return fp();
}

int ssl_gateway::X509_STORE_add_cert_(X509_STORE* ctx, X509* cert)
{
    assert(m_functions.fpX509_STORE_add_cert != nullptr);

    typedef int(func_type)(X509_STORE*, X509*);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpX509_STORE_add_cert);

    return fp(ctx, cert);
}

X509* ssl_gateway::d2i_X509_(X509** cert, const unsigned char** ppin, long length)
{
    assert(m_functions.fpd2i_X509 != nullptr);

    typedef X509*(func_type)(X509**, const unsigned char**, long);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpd2i_X509);

    return fp(cert, ppin, length);
}

void ssl_gateway::X509_free_(X509* cert)
{
    assert(m_functions.fpX509_free != nullptr);

    typedef void(func_type)(X509*);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpX509_free);

    fp(cert);
}

BIO* ssl_gateway::BIO_new_(const BIO_METHOD* method)
{
    assert(m_functions.fpBIO_new != nullptr);

    typedef BIO*(func_type)(const BIO_METHOD*);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpBIO_new);

    return fp(method);
}

BIO* ssl_gateway::BIO_new_ssl_connect_(SSL_CTX* ctx)
{
    assert(m_functions.fpBIO_new_ssl_connect != nullptr);

    typedef BIO*(func_type)(SSL_CTX*);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpBIO_new_ssl_connect);

    return fp(ctx);
}

void ssl_gateway::BIO_free_all_(BIO* a)
{
    assert(m_functions.fpBIO_free_all != nullptr);

    typedef void (func_type)(BIO*);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpBIO_free_all);

    fp(a);
}

const BIO_METHOD* ssl_gateway::BIO_s_mem_()
{
    assert(m_functions.fpBIO_s_mem != nullptr);

    typedef const BIO_METHOD* (func_type)();

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpBIO_s_mem);

    return fp();
}

int ssl_gateway::BIO_read_(BIO* b, void* data, int len)
{
    assert(m_functions.fpBIO_read != nullptr);

    typedef int (func_type)(BIO*, void*, int);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpBIO_read);

    return fp(b, data, len);
}

int ssl_gateway::BIO_write_(BIO* b, const void *data, int len)
{
    assert(m_functions.fpBIO_write != nullptr);

    typedef int (func_type)(BIO*, const void*, int);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpBIO_write);

    return fp(b, data, len);
}

int ssl_gateway::BIO_pending_(BIO* b)
{
    return BIO_ctrl_(b, BIO_CTRL_PENDING, 0, nullptr);
}

long ssl_gateway::BIO_ctrl_(BIO* bp, int cmd, long larg, void* parg)
{
    assert(m_functions.fpBIO_ctrl != nullptr);

    typedef long (func_type)(BIO*, int, long, void*);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpBIO_ctrl);

    return fp(bp, cmd, larg, parg);
}

long ssl_gateway::BIO_get_ssl_(BIO* bp, SSL** ssl)
{
    return BIO_ctrl_(bp, BIO_C_GET_SSL, 0, reinterpret_cast<void*>(ssl));
}

long ssl_gateway::BIO_set_nbio_(BIO* bp, long n)
{
    return BIO_ctrl_(bp, BIO_C_SET_NBIO, n, nullptr);
}

long ssl_gateway::BIO_set_conn_hostname_(BIO* bp, const char* name)
{
    return BIO_ctrl_(bp, BIO_C_SET_CONNECT, 0, const_cast<char*>(name));
}

unsigned long ssl_gateway::ERR_get_error_()
{
    assert(m_functions.fpERR_get_error != nullptr);

    typedef unsigned long (func_type)();

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpERR_get_error);

    return fp();
}

void ssl_gateway::ERR_error_string_n_(unsigned long e, char* buf, size_t len)
{
    assert(m_functions.fpERR_error_string_n != nullptr);

    typedef void (func_type)(unsigned long, char*, size_t);

    auto* fp = reinterpret_cast<func_type*>(m_functions.fpERR_error_string_n);

    fp(e, buf, len);
}

} // namespace ignite
