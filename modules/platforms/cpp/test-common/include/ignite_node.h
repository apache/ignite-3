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

#include <chrono>

#include "process.h"

namespace ignite
{

/**
 * Represents Ignite server node process.
 *
 * Ignite node is started from command line. It is recommended to re-use
 * a single Ignite node as much as possible to make tests as quick as possible.
 */
class IgniteNode
{
public:
    /**
     * Constructor.
     */
    IgniteNode() = default;

    /**
     * Destructor.
     */
    ~IgniteNode() = default;

    /**
     * Start node.
     *
     * @param dryRun Perform a dry run. Mostly used to ensure that code is compiled and all artifacts are downloaded.
     */
    void start(bool dryRun = false);

    /**
     * Stop node.
     */
    void stop();

    /**
     * Join node process.
     *
     * @param timeout Timeout.
     */
    void join(std::chrono::milliseconds timeout);

private:
    /** Underlying process. */
    std::unique_ptr<Process> process;
};

} // namespace ignite
