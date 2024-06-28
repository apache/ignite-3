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

#pragma once

#include "tests/client-test/ignite_runner_suite.h"

#include <ignite/client/basic_authenticator.h>
#include <ignite/client/ignite_client.h>
#include <ignite/client/ignite_client_configuration.h>

#include <chrono>
#include <thread>

/**
 * Test suite.
 */
class basic_auth_test_suite : public ignite::ignite_runner_suite {
public:
    /** Correct username */
    inline static const std::string CORRECT_USERNAME{"user-1"};

    /** Correct password */
    inline static const std::string CORRECT_PASSWORD{"password-1"};

    /** Correct username */
    inline static const std::string INCORRECT_USERNAME{"root"};

    /** Correct password */
    inline static const std::string INCORRECT_PASSWORD{"123"};

    /**
     * Get default configuration.
     *
     * @return Configuration.
     */
    static ignite::ignite_client_configuration get_configuration() {
        ignite::ignite_client_configuration cfg{get_node_addrs()};
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
    static ignite::ignite_client_configuration get_configuration(std::string user, std::string password) {
        ignite::ignite_client_configuration cfg{get_configuration()};

        auto authenticator = std::make_shared<ignite::basic_authenticator>(std::move(user), std::move(password));
        cfg.set_authenticator(authenticator);

        return cfg;
    }

    /**
     * Get configuration with correct credentials.
     *
     * @return Configuration with correct credentials.
     */
    static ignite::ignite_client_configuration get_configuration_correct() {
        return get_configuration(CORRECT_USERNAME, CORRECT_PASSWORD);
    }

    /**
     * Change cluster authentication state.
     *
     * @param enable Authentication enabled.
     */
    static void set_authentication_enabled(bool enable) {
        using namespace ignite;

        if (m_auth_enabled == enable)
            return;

        try {
            ignite_client_configuration cfg = m_auth_enabled ? get_configuration_correct() : get_configuration();

            auto client = ignite_client::start(cfg, std::chrono::seconds(30));

            auto nodes = client.get_cluster_nodes();
            auto descriptor = job_descriptor::builder(ENABLE_AUTHN_JOB).build();

            client.get_compute().submit(nodes, descriptor, {enable ? 1 : 0}).get_result();
        } catch (const ignite_error &) {
            // Ignore.
            // As a result of this call, the client may be disconnected from the server due to authn config change.
        }

        // Wait for the server to apply the configuration change and drop the client connection.
        std::this_thread::sleep_for(std::chrono::seconds(10));

        // TODO: IGNITE-18885 C++: Thin 3.0: Add TLS support
#ifdef MUTED
        // Check that configuration has been applied to all nodes.
        auto applied = wait_for_condition(std::chrono::seconds(10), [&] () -> bool {
            for (const auto &addr : ignite_runner::NODE_ADDRS) {
                ignite_client_configuration cfg{addr};
                cfg.set_logger(get_logger());

                if (enable) {
                   auto auth = std::make_shared<ignite::basic_authenticator>(CORRECT_USERNAME, CORRECT_PASSWORD);
                   cfg.set_authenticator(auth);
                }

                auto client = ignite_client::start(cfg, std::chrono::seconds(30));
            }
            return true;
        });
        if (!applied)
            throw ignite_error("Auth configuration was not applied within timeout");
#endif

        m_auth_enabled = enable;
    }

private:
    /** Authentication enabled. */
    inline static bool m_auth_enabled{false};
};
