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

#include "ignite_runner_suite.h"
#include "tests/test-common/test_utils.h"

#include "ignite/client/ignite_client.h"
#include "ignite/client/ignite_client_configuration.h"

#include <gtest/gtest.h>
#include <gmock/gmock-matchers.h>

using namespace ignite;

/**
 * Test suite.
 */
class ssl_test : public ignite_runner_suite { };

/**
 * Get a path to a SSL file.
 * @param file
 * @return
 */
std::string get_ssl_file(const std::string& file)
{
    auto test_dir = resolve_test_dir();
    auto ssl_files_dir = test_dir / "client-test" / "ssl";
    if (!std::filesystem::is_directory(ssl_files_dir))
        throw ignite_error("Can not find an 'ssl' directory in the current 'tests' directory: " + ssl_files_dir.string());

    return (ssl_files_dir / file).string();
}

TEST_F(ssl_test, ssl_connection_success)
{
    ignite_client_configuration cfg{get_ssl_node_addrs()};
    cfg.set_logger(get_logger());

    cfg.set_ssl_mode(ssl_mode::REQUIRE);
    cfg.set_ssl_cert_file(get_ssl_file("client.pem"));
    cfg.set_ssl_key_file(get_ssl_file("client.pem"));
    cfg.set_ssl_ca_file(get_ssl_file("ca.pem"));

    auto client = ignite_client::start(cfg, std::chrono::seconds(500));
}

TEST_F(ssl_test, ssl_connection_reject)
{
    ignite_client_configuration cfg{get_ssl_node_addrs()};
    cfg.set_logger(get_logger());

    cfg.set_ssl_mode(ssl_mode::REQUIRE);
    cfg.set_ssl_cert_file(get_ssl_file("client_unknown.pem"));
    cfg.set_ssl_key_file(get_ssl_file("client_unknown.pem"));
    cfg.set_ssl_ca_file(get_ssl_file("ca.pem"));

    EXPECT_THROW(
        {
            try {
                (void) ignite_client::start(cfg, std::chrono::seconds(30));
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), testing::HasSubstr("Can not establish secure connection"));
                throw;
            }
        },
        ignite_error);
}

TEST_F(ssl_test, ssl_connection_reject_2)
{
    ignite_client_configuration cfg{get_ssl_node_addrs()};
    cfg.set_logger(get_logger());

    cfg.set_ssl_mode(ssl_mode::DISABLE);

    EXPECT_THROW(
        {
            try {
                (void) ignite_client::start(cfg, std::chrono::seconds(5));
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), testing::HasSubstr("Can not establish connection within timeout"));
                throw;
            }
        },
        ignite_error);
}

TEST_F(ssl_test, ssl_connection_rejected_3)
{
    ignite_client_configuration cfg{get_ssl_node_addrs()};
    cfg.set_logger(get_logger());

    cfg.set_ssl_mode(ssl_mode::REQUIRE);
    cfg.set_ssl_cert_file(get_ssl_file("client_full.pem"));
    cfg.set_ssl_key_file(get_ssl_file("client_full.pem"));
    cfg.set_ssl_ca_file(get_ssl_file("ca.pem"));

    EXPECT_THROW(
        {
            try {
                (void) ignite_client::start(cfg, std::chrono::seconds(30));
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), testing::HasSubstr("Can not establish secure connection"));
                throw;
            }
        },
        ignite_error);
}

//TEST_F(ssl_test, ssl_cache_client_put_all_get_all)
//{
//    StartSslNode("node1");
//    StartSslNode("node2");
//
//    IgniteClientConfiguration cfg;
//    cfg.SetEndPoints("127.0.0.1:11110,127.0.0.1:11110");
//
//    cfg.SetSslMode(SslMode::REQUIRE);
//    cfg.SetSslCertFile(GetConfigFile("client_full.pem"));
//    cfg.SetSslKeyFile(GetConfigFile("client_full.pem"));
//    cfg.SetSslCaFile(GetConfigFile("ca.pem"));
//
//    IgniteClient client = IgniteClient::Start(cfg);
//
//    cache::CacheClient<int32_t, std::string> cache =
//            client.CreateCache<int32_t, std::string>("test");
//
//    enum { BATCH_SIZE = 20000 };
//
//    std::map<int32_t, std::string> values;
//    std::set<int32_t> keys;
//
//    for (int32_t j = 0; j < BATCH_SIZE; ++j)
//    {
//        int32_t key = BATCH_SIZE + j;
//
//        values[key] = "value_" + ignite::common::LexicalCast<std::string>(key);
//        keys.insert(key);
//    }
//
//    cache.PutAll(values);
//
//    std::map<int32_t, std::string> retrieved;
//    cache.GetAll(keys, retrieved);
//
//    BOOST_REQUIRE(values == retrieved);
//}
//
//TEST_F(ssl_test, ssl_cache_client_put_get)
//{
//    StartSslNode("node1");
//    StartSslNode("node2");
//
//    IgniteClientConfiguration cfg;
//    cfg.SetEndPoints("127.0.0.1:11110,127.0.0.1:11110");
//
//    cfg.SetSslMode(SslMode::REQUIRE);
//    cfg.SetSslCertFile(GetConfigFile("client_full.pem"));
//    cfg.SetSslKeyFile(GetConfigFile("client_full.pem"));
//    cfg.SetSslCaFile(GetConfigFile("ca.pem"));
//
//    IgniteClient client = IgniteClient::Start(cfg);
//
//    cache::CacheClient<int32_t, std::string> cache =
//            client.CreateCache<int32_t, std::string>("test");
//
//    enum { OPS_NUM = 100 };
//    for (int32_t j = 0; j < OPS_NUM; ++j)
//    {
//        int32_t key = OPS_NUM + j;
//        std::string value = "value_" + ignite::common::LexicalCast<std::string>(key);
//
//        cache.Put(key, value);
//        std::string retrieved = cache.Get(key);
//
//        BOOST_REQUIRE_EQUAL(value, retrieved);
//    }
//}
//
//TEST_F(ssl_test, ssl_connection_no_certs)
//{
//    StartSslNoClientAuthNode();
//
//    IgniteClientConfiguration cfg;
//    cfg.SetEndPoints("127.0.0.1:11110");
//
//    cfg.SetSslMode(SslMode::REQUIRE);
//    cfg.SetSslCaFile(GetConfigFile("ca.pem"));
//
//    IgniteClient client = IgniteClient::Start(cfg);
//}
//
///**
// * Check whether error is "file not exists".
// *
// * @param err Error to check
// * @return @true is Error is of expected kind.
// */
//bool IsNonExisting(const ignite::IgniteError& err)
//{
//    if (err.GetCode() != ignite::IgniteError::IGNITE_ERR_SECURE_CONNECTION_FAILURE)
//        return false;
//
//    std::string msg(err.GetText());
//
//    if (msg.find("error:02001002") == std::string::npos &&
//        msg.find("error:10000080") == std::string::npos &&
//        msg.find("error:2006D080") == std::string::npos &&
//        msg.find("error:80000002") == std::string::npos)
//        return false;
//
//    if (msg.find("No such file or directory") == std::string::npos &&
//        msg.find("no such file") == std::string::npos)
//        return false;
//
//    return true;
//}
//
///**
// * Check whether error is "CA file not exists".
// *
// * @param err Error to check
// * @return @true is Error is of expected kind.
// */
//bool IsNonExistingCa(const ignite::IgniteError& err)
//{
//    if (!IsNonExisting(err))
//        return false;
//
//    std::string msg(err.GetText());
//    return msg.find("Can not set Certificate Authority path for secure connection") != std::string::npos;
//}
//
//TEST_F(ssl_test, ssl_connection_error_non_existing_ca)
//{
//    StartSslNoClientAuthNode();
//
//    IgniteClientConfiguration cfg;
//    cfg.SetEndPoints("127.0.0.1:11110");
//
//    cfg.SetSslMode(SslMode::REQUIRE);
//    cfg.SetSslCaFile(GetConfigFile("non_existing_ca.pem"));
//
//    BOOST_CHECK_EXCEPTION(IgniteClient::Start(cfg), ignite::IgniteError, IsNonExistingCa);
//}
//
///**
// * Check whether error is "private key file not exists".
// *
// * @param err Error to check
// * @return @true is Error is of expected kind.
// */
//bool IsNonExistingKey(const ignite::IgniteError& err)
//{
//    if (!IsNonExisting(err))
//        return false;
//
//    std::string msg(err.GetText());
//    return msg.find("Can not set private key file for secure connection") != std::string::npos;
//}
//
//TEST_F(ssl_test, ssl_connection_error_non_existing_key)
//{
//    StartSslNoClientAuthNode();
//
//    IgniteClientConfiguration cfg;
//    cfg.SetEndPoints("127.0.0.1:11110");
//
//    cfg.SetSslMode(SslMode::REQUIRE);
//    cfg.SetSslKeyFile(GetConfigFile("non_existing_key.pem"));
//
//    BOOST_CHECK_EXCEPTION(IgniteClient::Start(cfg), ignite::IgniteError, IsNonExistingKey);
//}
//
//
///**
// * Check whether error is "certificate file not exists".
// *
// * @param err Error to check
// * @return @true is Error is of expected kind.
// */
//bool IsNonExistingCert(const ignite::IgniteError& err)
//{
//    if (!IsNonExisting(err))
//        return false;
//
//    std::string msg(err.GetText());
//    return msg.find("Can not set client certificate file for secure connection") != std::string::npos;
//}
//
//TEST_F(ssl_test, ssl_connection_error_non_existing_cert)
//{
//    StartSslNoClientAuthNode();
//
//    IgniteClientConfiguration cfg;
//    cfg.SetEndPoints("127.0.0.1:11110");
//
//    cfg.SetSslMode(SslMode::REQUIRE);
//    cfg.SetSslCertFile(GetConfigFile("non_existing_Cert.pem"));
//
//    BOOST_CHECK_EXCEPTION(IgniteClient::Start(cfg), ignite::IgniteError, IsNonExistingCert);
//}
