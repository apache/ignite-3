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

#include <cstdint>
#include <exception>
#include <string>

namespace ignite {

/**
 * Status code.
 */
enum class status_code : std::int32_t {
    SUCCESS = 0,

    GENERIC,

    UNKNOWN,

    NETWORK,

    OS,
};

/**
 * Ignite Error.
 */
class ignite_error : public std::exception {
public:
    // Default
    ignite_error() = default;

    /**
     * Constructor.
     *
     * @param message Message.
     */
    explicit ignite_error(std::string message)
        : m_status_code(status_code::GENERIC)
        , m_message(std::move(message))
        , m_cause() {} // NOLINT(bugprone-throw-keyword-missing)

    /**
     * Constructor.
     *
     * @param statusCode Status code.
     * @param message Message.
     */
    explicit ignite_error(status_code statusCode, std::string message)
        : m_status_code(statusCode)
        , m_message(std::move(message))
        , m_cause() {} // NOLINT(bugprone-throw-keyword-missing)

    /**
     * Constructor.
     *
     * @param statusCode Status code.
     * @param message Message.
     * @param cause Error cause.
     */
    explicit ignite_error(status_code statusCode, std::string message, const std::exception_ptr &cause)
        : m_status_code(statusCode)
        , m_message(std::move(message))
        , m_cause(cause) {} // NOLINT(bugprone-throw-keyword-missing)

    /**
     * Get error message.
     */
    [[nodiscard]] char const *what() const noexcept override { return m_message.c_str(); }

    /**
     * Get error message as std::string.
     */
    [[nodiscard]] const std::string &what_str() const { return m_message; }

    /**
     * Get status code.
     *
     * @return Status code.
     */
    [[nodiscard]] status_code get_status_code() const { return m_status_code; }

    /**
     * Get error cause.
     *
     * @return Error cause. Can be empty.
     */
    [[nodiscard]] std::exception_ptr get_cause() { return m_cause; }

private:
    /** Status code. */
    status_code m_status_code{status_code::SUCCESS};

    /** Message. */
    std::string m_message;

    /** Cause. */
    std::exception_ptr m_cause;
};

} // namespace ignite
