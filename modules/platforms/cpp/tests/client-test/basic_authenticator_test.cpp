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

#include <gtest/gtest.h>

#include <chrono>

using namespace ignite;

/**
 * Test suite.
 */
class client_test : public ignite_runner_suite
{
public:
    /** Correct username */
    inline static const std::string CORRECT_USERNAME{"user-1"};

    /** Correct password */
    inline static const std::string CORRECT_PASSWORD{"password-1"};

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

        std::shared_ptr<basic_authenticator> authenticator;
        authenticator->set_identity(std::move(user));
        authenticator->set_secret(std::move(password));
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
};

TEST_F(client_test, basic_authentication_success) {
    auto client = ignite_client::start(get_configuration_correct(), std::chrono::seconds(30));
}

TEST_F(client_test, basic_authentication_wrong_username) {
    EXPECT_THROW(
        {
            try {
                (void) ignite_client::start(get_configuration("Lorem Ipsum", "bla"), std::chrono::seconds(30));
            } catch (const ignite_error &e) {
                EXPECT_STREQ("Key tuple can not be empty", e.what());
                throw;
            }
        },
        ignite_error);
}

TEST_F(client_test, basic_authentication_wrong_password) {
    EXPECT_THROW(
        {
            try {
                (void) ignite_client::start(get_configuration(CORRECT_USERNAME, "wrong"), std::chrono::seconds(30));
            } catch (const ignite_error &e) {
                EXPECT_STREQ("Key tuple can not be empty", e.what());
                throw;
            }
        },
        ignite_error);
}
