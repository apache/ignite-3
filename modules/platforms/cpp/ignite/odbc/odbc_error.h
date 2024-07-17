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

#include <exception>
#include <string>
#include <utility>

#include "common_types.h"
#include "ignite/common/ignite_error.h"

namespace ignite {

/**
 * ODBC error.
 */
class odbc_error : public std::exception {
public:
    // Default
    odbc_error() = default;

    /**
     * Constructor.
     *
     * @param state SQL state.
     * @param message Error message.
     */
    odbc_error(sql_state state, std::string message) noexcept
        : m_state(state)
        , m_message(std::move(message)) {}

    /**
     * Constructor.
     *
     * @param err Ignite error.
     */
    explicit odbc_error(ignite_error err) noexcept
        : m_state(error_code_to_sql_state(err.get_status_code()))
        , m_message(err.what_str())
        , m_cause(std::move(err)) {}

    /**
     * Get state.
     *
     * @return State.
     */
    [[nodiscard]] sql_state get_state() const { return m_state; }

    /**
     * Get error message.
     *
     * @return Error message.
     */
    [[nodiscard]] const std::string &get_error_message() const { return m_message; }

    /**
     * Get error message.
     */
    [[nodiscard]] char const *what() const noexcept override { return m_message.c_str(); }

    /**
     * Get cause.
     *
     * @return Cause.
     */
    [[nodiscard]] const std::optional<ignite_error>& get_cause() const { return m_cause; }

private:
    /** Status. */
    sql_state m_state{sql_state::UNKNOWN};

    /** Error message. */
    std::string m_message;

    /** Cause. */
    std::optional<ignite_error> m_cause;
};

} // namespace ignite
