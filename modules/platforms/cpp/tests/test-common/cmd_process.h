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

#include <chrono>
#include <memory>
#include <string>
#include <vector>

namespace ignite {

/**
 * Represents system process launched using commandline instruction.
 */
class CmdProcess {
public:
    /**
     * Destructor.
     */
    virtual ~CmdProcess() = default;

    /**
     * Make new process instance.
     *
     * @param command Command.
     * @param args Arguments.
     * @param workDir Working directory.
     * @return CmdProcess.
     */
    static std::unique_ptr<CmdProcess> make(std::string command, std::vector<std::string> args, std::string workDir);

    /**
     * Start process.
     */
    virtual bool start() = 0;

    /**
     * Kill the process.
     */
    virtual void kill() = 0;

    /**
     * Join process.
     *
     * @param timeout Timeout.
     */
    virtual void join(std::chrono::milliseconds timeout) = 0;

protected:
    /**
     * Constructor.
     */
    CmdProcess() = default;
};

} // namespace ignite
