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

#include <memory>
#include <string>
#include <sstream>

#include <gtest/gtest.h>

namespace ignite
{

/**
 * Test logger.
 */
class GtestLogger : public IgniteLogger
{
public:
    /**
     * Construct.
     *
     * @param includeTs Include timestamps.
     * @param debug Enable debug.
     */
    GtestLogger(bool includeTs, bool debug) :
        m_includeTs(includeTs),
        m_debug(debug) { }

    void logError(std::string_view message) override {
        std::cout << "[          ] [ ERROR ]   " + std::string(message) + '\n' << std::flush;
    }

    void logWarning(std::string_view message) override {
        std::cout << "[          ] [ WARNING ] " + std::string(message) + '\n' << std::flush;
    }

    void logInfo(std::string_view message) override {
        std::cout << "[          ] [ INFO ]    " + std::string(message) + '\n' << std::flush;
    }

    void logDebug(std::string_view message) override {
        if (m_debug)
            std::cout << "[          ] [ DEBUG ]   " + std::string(message) + '\n' << std::flush;
    }

private:
    /**
     * Get timestamp in string format.
     *
     * @return Timestamp string.
     */
    [[nodiscard]]
    std::string getTimestamp() const
    {
        if (!m_includeTs)
            return {};

        using clock = std::chrono::system_clock;

        auto now = clock::now();
        auto cTime = clock::to_time_t(now);

        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());

        std::stringstream ss;
        ss << std::put_time(std::localtime(&cTime), "%H:%M:%S.") << std::setw(3) << std::setfill('0') << (ms.count() % 1000) << " ";
        return ss.str();
    }

    /** Include timestamps. */
    bool m_includeTs;

    /** Include debug messages. */
    bool m_debug;
};

} // namespace ignite
