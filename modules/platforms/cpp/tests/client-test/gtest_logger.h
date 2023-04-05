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

#include <ignite/client/ignite_logger.h>

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <sstream>
#include <string>

namespace ignite {

/**
 * Test logger.
 */
class gtest_logger : public ignite_logger {
public:
    /**
     * Construct.
     *
     * @param includeTs Include timestamps.
     * @param debug Enable debug.
     */
    gtest_logger(bool includeTs, bool debug)
        : m_includeTs(includeTs)
        , m_debug(debug) {}

    void log_error(std::string_view message) override {
        std::cout << "[          ] [ ERROR ]   " + get_timestamp() + std::string(message) + '\n' << std::flush;
    }

    void log_warning(std::string_view message) override {
        std::cout << "[          ] [ WARNING ] " + get_timestamp() + std::string(message) + '\n' << std::flush;
    }

    void log_info(std::string_view message) override {
        std::cout << "[          ] [ INFO ]    " + get_timestamp() + std::string(message) + '\n' << std::flush;
    }

    void log_debug(std::string_view message) override {
        if (m_debug)
            std::cout << "[          ] [ DEBUG ]   " + get_timestamp() + std::string(message) + '\n' << std::flush;
    }

    [[nodiscard]] bool is_debug_enabled() const override { return true; }

private:
    /**
     * Get timestamp in string format.
     *
     * @return Timestamp string.
     */
    [[nodiscard]] std::string get_timestamp() const {
        if (!m_includeTs)
            return {};

        using clock = std::chrono::system_clock;

        auto now = clock::now();
        auto cTime = clock::to_time_t(now);

        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());

        std::stringstream ss;
        ss << std::put_time(std::localtime(&cTime), "%H:%M:%S.") << std::setw(3) << std::setfill('0')
           << (ms.count() % 1000) << " ";
        return ss.str();
    }

    /** Include timestamps. */
    bool m_includeTs;

    /** Include debug messages. */
    bool m_debug;
};

} // namespace ignite
