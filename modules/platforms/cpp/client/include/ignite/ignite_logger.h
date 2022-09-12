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

#include <string_view>

namespace ignite
{

/**
 * Ignite logger interface.
 *
 * User can implement this class to use preferred logger with Ignite client.
 */
class IgniteLogger
{
public:
    // Default
    IgniteLogger() = default;
    ~IgniteLogger() = default;
    IgniteLogger(IgniteLogger&&) = default;
    IgniteLogger(const IgniteLogger&) = default;
    IgniteLogger& operator=(IgniteLogger&&) = default;
    IgniteLogger& operator=(const IgniteLogger&) = default;

    /**
     * Used to log error messages.
     *
     * @param message Error message.
     */
    virtual void logError(std::string_view message) = 0;

    /**
     * Used to log warning messages.
     *
     * @param message Warning message.
     */
    virtual void logWarning(std::string_view message) = 0;

    /**
     * Used to log info messages.
     *
     * @param message Info message.
     */
    virtual void logInfo(std::string_view message) = 0;

    /**
     * Used to log debug messages.
     *
     * It is recommended to disable debug logging by default for the sake of performance.
     *
     * @param message Debug message.
     */
    virtual void logDebug(std::string_view message) = 0;
};

} // namespace ignite
