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

#include <sstream>
#include <string>
#include <string_view>

#include "common/ignite_error.h"

namespace ignite::network {

/**
 * Fibonacci sequence iterator.
 *
 * @tparam S Sequence length. Should be >= 2.
 */
template <size_t S, typename T = size_t>
class FibonacciSequence {
public:
    /** Size. */
    static constexpr size_t size = S > 2 ? S : 2;

    /**
     * Constructor.
     */
    constexpr FibonacciSequence() {
        for (size_t i = 2; i < size; ++i)
            sequence[i] = sequence[i - 1] + sequence[i - 2];
    }

    /**
     * Get n-th or max member of sequence.
     *
     * @param n Member position.
     * @return N-th member of sequence if n < size, or max member.
     */
    [[nodiscard]] constexpr T getValue(size_t n) const { return n < size ? sequence[n] : sequence[size - 1]; }

private:
    /** Sequence of fibonacci numbers */
    T sequence[size]{0, 1};
};

/**
 * Format error message.
 *
 * @param description Error description.
 * @param description Error details.
 * @param advice User advice.
 */
inline std::string formatErrorMessage(std::string_view description, std::string_view details, std::string_view advice) {
    std::stringstream messageBuilder;
    messageBuilder << description;
    if (!details.empty())
        messageBuilder << ": " << details;

    if (!advice.empty())
        messageBuilder << ". " << advice;

    return messageBuilder.str();
}

/**
 * Try extract from system error stack, and return platform-specific error.
 *
 * @return Error in human-readable format.
 */
std::string getLastSystemError();

/**
 * Try extract from system error stack, format and return platform-specific error.
 *
 * @param description Error description.
 * @return Error in human-readable format.
 */
inline std::string getLastSystemError(std::string_view description, std::string_view advice) {
    return formatErrorMessage(description, getLastSystemError(), advice);
}

/**
 * Try extract from system error stack and throw platform-specific error.
 *
 * @param description Error description.
 * @param advice User advice.
 */
inline void throwLastSystemError(std::string_view description, std::string_view advice = {}) {
    throw IgniteError(StatusCode::OS, getLastSystemError(description, advice));
}

} // namespace ignite::network
