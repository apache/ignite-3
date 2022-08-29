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

#include "process.h"

namespace ignite
{
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

//        /**
//         * Check whether node is still running.
//         *
//         * @return @c true if the node is running.
//         */
//        bool isRunning();
//
//        /**
//         * Get current node output.
//         *
//         * @param max Max bytes to get.
//         * @return Output.
//         */
//        std::string getOutput(int max = 1024);

    private:
        /** Underlying process. */
        std::unique_ptr<Process> process;
    };
} // namespace ignite
