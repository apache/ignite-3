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

#include "ignite_runner.h"

#include "ignite/client/ignite_client_configuration.h"
#include "ignite_type.h"
#include "test_utils.h"

#include "ignite/common/detail/config.h"

#include <algorithm>
#include <filesystem>
#include <sstream>
#include <stdexcept>

#ifdef WIN32
# include <Windows.h>
#endif

namespace {

void set_environment_variable(const char* name, const char* value) {
#ifdef WIN32
    SetEnvironmentVariable(name, value);
#else
    setenv(name, value, 1);
#endif
}

/**
 * System shell command string.
 */
constexpr std::string_view SYSTEM_SHELL = IGNITE_SWITCH_WIN_OTHER("cmd.exe", "/bin/sh");
constexpr std::string_view SYSTEM_SHELL_ARG_0 = IGNITE_SWITCH_WIN_OTHER("/c ", "-c");
constexpr std::string_view GRADLEW_SCRIPT = IGNITE_SWITCH_WIN_OTHER("gradlew.bat", "./gradlew");

const char* IGNITE_CLUSTER_HOST = getenv("IGNITE_CLUSTER_HOST");
const std::string SERVER_ADDRESS = IGNITE_CLUSTER_HOST ? IGNITE_CLUSTER_HOST : "127.0.0.1";

const std::string ADDITIONAL_JVM_OPTIONS_ENV = "CPP_ADDITIONAL_JVM_OPTIONS";

} // anonymous namespace

namespace ignite {

static int calc_node_port_offset(std::string_view version);

std::vector<std::string> ignite_runner::SINGLE_NODE_ADDR = {SERVER_ADDRESS + ":10942"};
std::vector<std::string> ignite_runner::NODE_ADDRS = {SERVER_ADDRESS + ":10942", SERVER_ADDRESS + ":10943"};
std::vector<std::string> ignite_runner::SSL_NODE_ADDRS = {SERVER_ADDRESS + ":10944"};
std::vector<std::string> ignite_runner::SSL_NODE_CA_ADDRS = {SERVER_ADDRESS + ":10945"};
std::vector<std::string> ignite_runner::COMPATIBILITY_NODE_ADDRS = {};

ignite_runner::ignite_runner(std::string_view version) : m_version(version) {
    COMPATIBILITY_MODE = true;
    COMPATIBILITY_NODE_ADDRS = {SERVER_ADDRESS + ":"
        + std::to_string(ignite_client_configuration::DEFAULT_PORT + calc_node_port_offset(version))};
    COMPATIBILITY_VERSION = version;
}

void ignite_runner::start() {
    std::string home = resolve_ignite_home();

    std::string work_dir;
    if (m_version) {
        work_dir = resolve_temp_dir("IgniteCompatibility", *m_version).string();
    } else {
        work_dir = home;
    }

    if (home.empty())
        throw std::runtime_error("Can not resolve Ignite home directory. Try setting IGNITE_HOME explicitly");

    std::vector<std::string> args;
    args.emplace_back(SYSTEM_SHELL_ARG_0);

    std::string command{GRADLEW_SCRIPT};

    if (m_version) {
        command += " :ignite-compatibility-tests:runnerPlatformCompatibilityTest --parallel";
    } else {
        command += " :ignite-runner:runnerPlatformTest"
                   " --no-daemon"
                   " -x compileJava"
                   " -x compileTestFixturesJava"
                   " -x compileIntegrationTestJava"
                   " -x compileTestJava";
    }

    if (auto additional_opts = detail::get_env(ADDITIONAL_JVM_OPTIONS_ENV)) {
        command += " " + *additional_opts;
    }

    args.emplace_back(command);
    if (m_version) {
        set_environment_variable("IGNITE_OLD_SERVER_VERSION", m_version->c_str());
        set_environment_variable("IGNITE_OLD_SERVER_WORK_DIR", work_dir.c_str());

        int node_port_offset = calc_node_port_offset(*m_version);

        std::string port = std::to_string(3344 + 20000 + node_port_offset);
        std::string http_port = std::to_string(10300 + node_port_offset);
        std::string client_port = std::to_string(ignite_client_configuration::DEFAULT_PORT + node_port_offset);

        set_environment_variable("IGNITE_OLD_SERVER_PORT", port.c_str());
        set_environment_variable("IGNITE_OLD_SERVER_HTTP_PORT", http_port.c_str());
        set_environment_variable("IGNITE_OLD_SERVER_CLIENT_PORT", client_port.c_str());
    }

    m_process = CmdProcess::make(std::string(SYSTEM_SHELL), args, home);
    if (!m_process->start()) {
        m_process.reset();

        std::stringstream argsStr;
        for (auto &arg : args)
            argsStr << arg << " ";

        throw std::runtime_error("Failed to invoke Ignite command: '" + argsStr.str() + "'");
    }
}

void ignite_runner::stop() {
    if (m_process)
        m_process->kill();
}

void ignite_runner::join(std::chrono::milliseconds timeout) {
    if (m_process)
        m_process->join(timeout);
}

static int calc_node_port_offset(std::string_view version) {
    std::string port_shift;

    std::copy_if(version.begin(), version.end(), std::back_inserter(port_shift), [](auto c) { return c >= '0' && c <= '9'; });

    return stoi(port_shift);
}

} // namespace ignite