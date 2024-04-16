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

#include "tests/test-common/basic_auth_test_suite.h"

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

using namespace ignite;

struct DISABLED_basic_authenticator_test : public basic_auth_test_suite {
    /**
     * Tear down.
     */
    static void TearDownTestSuite() { set_authentication_enabled(false); }
};

TEST_F(DISABLED_basic_authenticator_test, disabled_on_server) {
    set_authentication_enabled(false);
    auto client = ignite_client::start(get_configuration_correct(), std::chrono::seconds(30));
    (void) client.get_cluster_nodes();
}

TEST_F(DISABLED_basic_authenticator_test, disabled_on_client) {
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

TEST_F(DISABLED_basic_authenticator_test, success) {
    set_authentication_enabled(true);
    auto client = ignite_client::start(get_configuration_correct(), std::chrono::seconds(30));
    (void) client.get_cluster_nodes();
}

TEST_F(DISABLED_basic_authenticator_test, wrong_username) {
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

TEST_F(DISABLED_basic_authenticator_test, wrong_password) {
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
