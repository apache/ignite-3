/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <filesystem>
#include <stdexcept>
#include <iostream>

#include "common/Platform.h"

#include "ignite_node.h"
#include "test_utils.h"

namespace
{

/**
 * System shell command string.
 */
constexpr std::string_view SYSTEM_SHELL = SWITCH_WIN_OTHER("cmd.exe /c ", "/bin/bash -c ");

} // anonymous namespace

namespace ignite
{

void IgniteNode::start(bool dryRun)
{
    std::string home = resolveIgniteHome();
    if (home.empty())
        throw std::runtime_error(
                "Can not resolve Ignite home directory. Try setting IGNITE_HOME explicitly");

    std::string command = std::string(SYSTEM_SHELL) + getMavenPath() + " exec:java@platform-test-node-runner";

    if (dryRun)
        command += " -Dexec.args=dry-run";

    auto workDir = std::filesystem::path(home) / "modules" / "runner";

    std::cout << "IGNITE_HOME=" << home << std::endl;
    std::cout << "working dir=" << workDir << std::endl;
    std::cout << "command=" << command << std::endl;

    m_process = CmdProcess::make(command, workDir.string());
    if (!m_process->start())
    {
        m_process.reset();

        throw std::runtime_error("Failed to invoke Ignite command: '" + command + "'");
    }
}

void IgniteNode::stop()
{
    if (m_process)
        m_process->kill();
}

void IgniteNode::join(std::chrono::milliseconds timeout)
{
    if (m_process)
        m_process->join(timeout);
}

} // namespace ignite