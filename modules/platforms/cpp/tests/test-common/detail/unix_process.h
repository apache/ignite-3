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

// It's OK that this code is entirely in header as it only supposed to be included from a single file.

#include "cmd_process.h"

#include <chrono>
#include <iostream>
#include <string>
#include <vector>

#ifdef __APPLE__
# include <csignal>
#endif
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

namespace ignite::detail {

/**
 * Implementation of CmdProcess for UNIX and UNIX-like systems.
 */
class UnixProcess : public ignite::CmdProcess {
public:
    /**
     * Constructor.
     *
     * @param command Command.
     * @param args Arguments.
     * @param workDir Working directory.
     */
    UnixProcess(std::string command, std::vector<std::string> args, std::string workDir)
        : m_running(false)
        , m_command(std::move(command))
        , m_args(std::move(args))
        , m_workDir(std::move(workDir)) {}

    /**
     * Destructor.
     */
    ~UnixProcess() override { kill(); }

    /**
     * Start process.
     */
    bool start() final {
        if (m_running)
            return false;

        m_pid = fork();

        if (!m_pid) {
            // Setting the group ID to be killed easily.
            int res = setpgid(0, 0);
            if (res) {
                std::cout << "Failed set group ID of the forked process: " + std::to_string(res) << std::endl;
                exit(1);
            }

            // Route for the forked process.
            res = chdir(m_workDir.c_str());
            if (res) {
                std::cout << "Failed to change directory of the forked process: " + std::to_string(res) << std::endl;
                exit(1);
            }

            std::vector<const char *> args;
            args.reserve(m_args.size() + 2);
            args.push_back(m_command.c_str());

            for (auto &arg : m_args) {
                args.push_back(arg.c_str());
            }

            args.push_back(nullptr);

            res = execvp(m_command.c_str(), const_cast<char *const *>(args.data()));

            // On success this code should never be reached because the process get replaced by a new one.
            std::cout << "Failed to execute process: " + std::to_string(res) << std::endl;
            exit(1);
        }

        m_running = true;
        return true;
    }

    /**
     * Kill the process.
     */
    void kill() final {
        if (!m_running)
            return;

        ::kill(-m_pid, SIGTERM);
    }

    /**
     * Join process.
     *
     * @param timeout Timeout.
     */
    void join(std::chrono::milliseconds) final {
        // Ignoring timeout in Linux...
        ::waitpid(m_pid, nullptr, 0);
    }

private:
    /** Running flag. */
    bool m_running;

    /** Process ID. */
    int m_pid{0};

    /** Command. */
    const std::string m_command;

    /** Arguments. */
    const std::vector<std::string> m_args;

    /** Working directory. */
    const std::string m_workDir;
};

} // namespace ignite::detail
