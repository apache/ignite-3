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

#pragma once

#include <cstdio>

#include <string>

namespace ignite
{
    /**
     * Resolve IGNITE_HOME directory. Resolution is performed in several steps:
     * 1) Check for path provided as argument.
     * 2) Check for environment variable.
     * 3) Check for current working directory.
     * Result of these checks are evaluated based on existence of certain predefined folders inside possible Ignite
     * home. If they are found, IGNITE_HOME is considered resolved.
     *
     * @param path Optional path to check.
     * @return Resolved Ignite home.
     */
    std::string resolveIgniteHome(const std::string& path = "");

    /**
     * Get path to maven executable.
     */
    std::string getMavenPath();

    /**
     * Open process.
     *
     * @param command System shell command line instruction.
     * @param type Mode of the returned process output stream. Can be one of the following:
     *  "r" - The calling process can read the spawned command's standard output using the returned stream.
     *  "w" - The calling process can write to the spawned command's standard input using the returned stream.
     * @return File stream for the process.
     */
    FILE* processOpen(const char *command, const char *type);

    /**
     * Waits for the associated process to terminate and returns the exit status of the command.
     *
     * @param stream Return value from the previous call to processOpen().
     * @return Returns the exit status of the terminating command processor, or -1 if an error occurs.
     */
    int processClose(FILE* stream);
} // namespace ignite