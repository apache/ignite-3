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

#include "ignite/client/ignite_logger.h"

#include <memory>

namespace ignite::detail {

/** Protocol version. */
class logger_wrapper : public ignite_logger {
public:
    /**
     * Constructor.
     *
     * @param logger Logger.
     */
    logger_wrapper(std::shared_ptr<ignite_logger> logger)
        : m_logger(std::move(logger)) {}

    /**
     * Used to log error messages.
     *
     * @param message Error message.
     */
    void log_error(std::string_view message) override {
        if (m_logger)
            m_logger->log_error(message);
    }

    /**
     * Used to log warning messages.
     *
     * @param message Warning message.
     */
    void log_warning(std::string_view message) override {
        if (m_logger)
            m_logger->log_warning(message);
    }

    /**
     * Used to log info messages.
     *
     * @param message Info message.
     */
    void log_info(std::string_view message) override {
        if (m_logger)
            m_logger->log_info(message);
    }

    /**
     * Used to log debug messages.
     *
     * It is recommended to disable debug logging by default for the sake of performance.
     *
     * @param message Debug message.
     */
    void log_debug(std::string_view message) override {
        if (m_logger)
            m_logger->log_debug(message);
    }

    /**
     * Check whether debug is enabled.
     * @return
     */
    [[nodiscard]] bool is_debug_enabled() const override { return m_logger && m_logger->is_debug_enabled(); }

private:
    /** Logger. */
    std::shared_ptr<ignite_logger> m_logger;
};

} // namespace ignite::detail
