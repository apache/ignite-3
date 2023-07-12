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

#include <ignite/client/basic_authenticator.h>
#include <ignite/client/ignite_client.h>
#include <ignite/client/ignite_client_configuration.h>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <chrono>
#include <thread>

using namespace ignite;

/**
 * Test suite.
 */
class basic_authenticator_test : public ignite_runner_suite
{
public:
    /** Correct username */
    inline static const std::string CORRECT_USERNAME{"user-1"};

    /** Correct password */
    inline static const std::string CORRECT_PASSWORD{"password-1"};

    /**
     * Tear down.
     */
    static void TearDownTestSuite() {
        set_authentication_enabled(false);
    }

    /**
     * Get default configuration.
     *
     * @return Configuration.
     */
    static ignite_client_configuration get_configuration() {
        ignite_client_configuration cfg{get_node_addrs()};
        cfg.set_logger(get_logger());

        return cfg;
    }

    /**
     * Get configuration with basic auth enabled.
     *
     * @param user Username to use.
     * @param password Password to use.
     * @return Configuration.
     */
    static ignite_client_configuration get_configuration(std::string user, std::string password) {
        ignite_client_configuration cfg{get_configuration()};

        auto authenticator = std::make_shared<basic_authenticator>(std::move(user), std::move(password));
        cfg.set_authenticator(authenticator);

        return cfg;
    }

    /**
     * Get configuration with correct credentials.
     *
     * @return Configuration with correct credentials.
     */
    static ignite_client_configuration get_configuration_correct() {
        return get_configuration(CORRECT_USERNAME, CORRECT_PASSWORD);
    }

    /**
     * Change cluster authentication state.
     *
     * @param enable Authentication enabled.
     */
    static void set_authentication_enabled(bool enable) {
        if (m_auth_enabled == enable)
            return;

        ignite_client_configuration cfg = m_auth_enabled ? get_configuration_correct() : get_configuration();

        try {
            auto client = ignite_client::start(cfg, std::chrono::seconds(30));
            auto nodes = client.get_cluster_nodes();
            client.get_compute().execute(nodes, {}, ENABLE_AUTHN_JOB, {enable ? 1 : 0});
        } catch (const ignite_error&) {
            // Ignore.
            // As a result of this call, the client may be disconnected from the server due to authn config change.
        }

        // Wait for the server to apply the configuration change and drop the client connection.
        std::this_thread::sleep_for(std::chrono::seconds(3));

        m_auth_enabled = enable;
    }

private:
    /** Authentication enabled. */
    inline static bool m_auth_enabled{false};
};

TEST_F(basic_authenticator_test, disabled_on_server) {
    set_authentication_enabled(false);
    auto client = ignite_client::start(get_configuration_correct(), std::chrono::seconds(30));
    (void) client.get_cluster_nodes();
}

TEST_F(basic_authenticator_test, disabled_on_client) {
    set_authentication_enabled(true);
    EXPECT_THROW(
        {
            try {
                (void) ignite_client::start(get_configuration(), std::chrono::seconds(30));
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), testing::HasSubstr("Authentication failed"));
                throw;
            }
        },
        ignite_error);
}

TEST_F(basic_authenticator_test, success) {
    set_authentication_enabled(true);
    auto client = ignite_client::start(get_configuration_correct(), std::chrono::seconds(30));
    (void) client.get_cluster_nodes();
}

TEST_F(basic_authenticator_test, wrong_username) {
    set_authentication_enabled(true);
    EXPECT_THROW(
        {
            try {
                (void) ignite_client::start(get_configuration("Lorem Ipsum", "bla"), std::chrono::seconds(30));
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), testing::HasSubstr("Authentication failed"));
                throw;
            }
        },
        ignite_error);
}

TEST_F(basic_authenticator_test, wrong_password) {
    set_authentication_enabled(true);
    EXPECT_THROW(
        {
            try {
                (void) ignite_client::start(get_configuration(CORRECT_USERNAME, "wrong"), std::chrono::seconds(30));
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), testing::HasSubstr("Authentication failed"));
                throw;
            }
        },
        ignite_error);
}
