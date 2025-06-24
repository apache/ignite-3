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
#include "test_utils.h"

#include "ignite/common/detail/config.h"

#include <filesystem>
#include <iostream>
#include <sstream>
#include <stdexcept>

namespace {

/**
 * System shell command string.
 */
constexpr std::string_view SYSTEM_SHELL = IGNITE_SWITCH_WIN_OTHER("cmd.exe", "/bin/sh");
constexpr std::string_view SYSTEM_SHELL_ARG_0 = IGNITE_SWITCH_WIN_OTHER("/c ", "-c");
constexpr std::string_view GRADLEW_SCRIPT = IGNITE_SWITCH_WIN_OTHER("gradlew.bat", "./gradlew");

const std::string SERVER_ADDRESS = "127.0.0.1";

} // anonymous namespace

namespace ignite {

std::vector<std::string> ignite_runner::SINGLE_NODE_ADDR = {SERVER_ADDRESS + ":10942"};
std::vector<std::string> ignite_runner::NODE_ADDRS = {SERVER_ADDRESS + ":10942", SERVER_ADDRESS + ":10943"};
std::vector<std::string> ignite_runner::SSL_NODE_ADDRS = {SERVER_ADDRESS + ":10944"};
std::vector<std::string> ignite_runner::SSL_NODE_CA_ADDRS = {SERVER_ADDRESS + ":10945"};

void ignite_runner::start() {
    std::string home = resolve_ignite_home();
    if (home.empty())
        throw std::runtime_error("Can not resolve Ignite home directory. Try setting IGNITE_HOME explicitly");

    std::vector<std::string> args;
    args.emplace_back(SYSTEM_SHELL_ARG_0);

    std::string command{GRADLEW_SCRIPT};
    command += " :ignite-runner:runnerPlatformTest"
               " --no-daemon"
               " -x compileJava"
               " -x compileTestFixturesJava"
               " -x compileIntegrationTestJava"
               " -x compileTestJava";

    args.emplace_back(command);

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

} // namespace ignite