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

#include "cmd_process.h"
#include "test_utils.h"

#include "ignite/common/detail/utils.h"

#include <chrono>
#include <string_view>

namespace ignite {

/**
 * Represents ignite_runner process.
 *
 * ignite_runner is started from the command line. It is recommended to re-use
 * the same ignite_runner as much as possible to make tests as quick as possible.
 */
class ignite_runner {
public:
    static std::vector<std::string> SINGLE_NODE_ADDR;
    static std::vector<std::string> NODE_ADDRS;
    static std::vector<std::string> SSL_NODE_ADDRS;
    static std::vector<std::string> SSL_NODE_CA_ADDRS;
    static std::vector<std::string> COMPATIBILITY_NODE_ADDRS;
    inline static bool COMPATIBILITY_MODE = false;
    inline static std::string COMPATIBILITY_VERSION;

    ignite_runner() = default;

    /**
     *
     * @param version If present then should start server of that particular version otherwise current version.
     */
    ignite_runner(std::string_view version);

    /**
     * Destructor.
     */
    ~ignite_runner() { stop(); }

    /**
     * Start node.
     */
    void start();

    /**
     * Stop node.
     */
    void stop();

    /**
     * Join a node process.
     *
     * @param timeout Timeout.
     */
    void join(std::chrono::milliseconds timeout);

    /**
     * Check whether tests run in single node mode.
     *
     * @return @c true if tests run in single node mode.
     */
    static bool single_node_mode() {
        auto env = ignite::detail::get_env("IGNITE_CPP_TESTS_USE_SINGLE_NODE");

        return env.has_value() ? *env == "true" : false;
    }

    /**
     * Get node addresses to use for tests.
     *
     * @return Addresses.
     */
    static std::vector<std::string> get_node_addrs() {
        if (COMPATIBILITY_MODE) {
            return COMPATIBILITY_NODE_ADDRS;
        }

        if (single_node_mode())
            return SINGLE_NODE_ADDR;

        return NODE_ADDRS;
    }

    /**
     * Get node addresses to use for tests.
     *
     * @return Addresses.
     */
    static std::vector<std::string> get_ssl_node_addrs() { return SSL_NODE_ADDRS; }

    /**
     * Get node addresses to use for tests.
     *
     * @return Addresses.
     */
    static std::vector<std::string> get_ssl_node_ca_addrs() { return SSL_NODE_CA_ADDRS; }

private:
    /** Underlying process. */
    std::unique_ptr<CmdProcess> m_process;
    std::optional<std::string> m_version = std::nullopt;
};

} // namespace ignite
