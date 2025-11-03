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

#include "error_codes.h"
#include "uuid.h"

#include <cstdint>
#include <exception>
#include <optional>
#include <string>
#include <any>
#include <map>

namespace ignite {

/**
 * @brief Basic exception type.
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
    explicit ignite_error(std::string message) noexcept
        : m_message(std::move(message)) {} // NOLINT(bugprone-throw-keyword-missing)

    /**
     * Constructor.
     *
     * @param message Message.
     */
    explicit ignite_error(std::string message, std::int32_t flags) noexcept
        : m_message(std::move(message))
        , m_flags(flags) {} // NOLINT(bugprone-throw-keyword-missing)

    /**
     * Constructor.
     *
     * @param code Status code.
     * @param message Message.
     */
    explicit ignite_error(error::code code, std::string message) noexcept
        : m_status_code(code)
        , m_message(std::move(message)) {} // NOLINT(bugprone-throw-keyword-missing)

    /**
     * Constructor.
     *
     * @param code Status code.
     * @param message Message.
     * @param cause Error cause.
     */
    explicit ignite_error(error::code code, std::string message, std::exception_ptr cause) noexcept
        : m_status_code(code)
        , m_message(std::move(message))
        , m_cause(std::move(cause)) {} // NOLINT(bugprone-throw-keyword-missing)

    /**
     * Constructor.
     *
     * @param code Status code.
     * @param message Message.
     * @param trace_id Trace ID.
     * @param java_st Java stack trace.
     */
    explicit ignite_error(error::code code, std::string message, uuid trace_id,
        std::optional<std::string> java_st) noexcept
        : m_status_code(code)
        , m_message(std::move(message))
        , m_trace_id(trace_id)
        , m_java_stack_trace(java_st) {} // NOLINT(bugprone-throw-keyword-missing)

    /**
     * Get an error message.
     */
    [[nodiscard]] char const *what() const noexcept override { return m_message.c_str(); }

    /**
     * Get the error message as std::string.
     */
    [[nodiscard]] const std::string &what_str() const noexcept { return m_message; }

    /**
     * Get status code.
     *
     * @return Status code.
     */
    [[nodiscard]] error::code get_status_code() const noexcept { return m_status_code; }

    /**
     * Get trace ID.
     * Trace ID can be used to track the same error in different part of product and different logs (e.g., server and
     * client)
     *
     * @return Trace ID.
     */
    [[nodiscard]] uuid get_trace_id() const noexcept { return m_trace_id; }

    /**
     * Get Java side stack trace.
     * Can be empty if the error generated on the client side or if the server side option for sending stack traces
     * to the client is disabled.
     *
     * @return Java side stack trace.
     */
    [[nodiscard]] const std::optional<std::string> &get_java_stack_trace() const noexcept { return m_java_stack_trace; }

    /**
     * Get error cause.
     *
     * @return Error cause. Can be empty.
     */
    [[nodiscard]] std::exception_ptr get_cause() const noexcept { return m_cause; }

    /**
     * Get flags.
     * Internal method.
     *
     * @return Flags.
     */
    [[nodiscard]] std::int32_t get_flags() const noexcept { return m_flags; }

    /**
     * Add extra information.
     *
     * @tparam T Extra type.
     * @param key Key.
     * @param value value.
     */
    template<typename T>
    void add_extra(std::string key, T value) {
        m_extras.emplace(std::pair{std::move(key), std::any{std::move(value)}});
    }

    /**
     * Get extra information by the key.
     *
     * @return Extra.
     */
    template<typename T>
    [[nodiscard]] std::optional<T> get_extra(const std::string &key) const noexcept {
        auto it = m_extras.find(key);
        if (it == m_extras.end())
            return {};

        return std::any_cast<T>(it->second);
    }

private:
    /** Status code. */
    error::code m_status_code{error::code::INTERNAL};

    /** Message. */
    std::string m_message;

    /** Trace ID. */
    uuid m_trace_id{uuid::random()};

    /** Java side stack trace. */
    std::optional<std::string> m_java_stack_trace;

    /** Cause. */
    std::exception_ptr m_cause;

    /** Flags. */
    std::int32_t m_flags{0};

    /** Extras. */
    std::map<std::string, std::any> m_extras;
};

} // namespace ignite
