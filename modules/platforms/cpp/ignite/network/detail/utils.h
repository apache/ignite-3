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

#include <ignite/common/ignite_error.h>

#include <algorithm>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

namespace ignite::network::detail {

/**
 * Fibonacci sequence iterator.
 *
 * @tparam S Sequence length. Should be >= 2.
 */
template<size_t S, typename T = size_t>
class fibonacci_sequence {
public:
    /** Size. */
    static constexpr size_t size = S > 2 ? S : 2;

    /**
     * Constructor.
     */
    constexpr fibonacci_sequence() {
        for (size_t i = 2; i < size; ++i)
            sequence[i] = sequence[i - 1] + sequence[i - 2];
    }

    /**
     * Get n-th or max member of sequence.
     *
     * @param n Member position.
     * @return N-th member of sequence if n < size, or max member.
     */
    [[nodiscard]] constexpr T get_value(size_t n) const { return n < size ? sequence[n] : sequence[size - 1]; }

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
inline std::string format_error_message(
    std::string_view description, std::string_view details, std::string_view advice) {
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
std::string get_last_system_error();

/**
 * Try extract from system error stack, format and return platform-specific error.
 *
 * @param description Error description.
 * @return Error in human-readable format.
 */
inline std::string get_last_system_error(std::string_view description, std::string_view advice) {
    return format_error_message(description, get_last_system_error(), advice);
}

/**
 * Try extract from system error stack and throw platform-specific error.
 *
 * @param description Error description.
 * @param advice User advice.
 */
inline void throw_last_system_error(std::string_view description, std::string_view advice = {}) {
    throw ignite_error(error::code::INTERNAL, get_last_system_error(description, advice));
}

/**
 * Shuffle addresses randomly.
 *
 * @param addrsIn Addresses.
 * @return Randomly shuffled addresses.
 */
template<typename Addrinfo>
std::vector<Addrinfo *> shuffle_addresses(Addrinfo *addrsIn) {
    std::vector<Addrinfo *> res;

    for (Addrinfo *it = addrsIn; it != NULL; it = it->ai_next)
        res.push_back(it);

    std::random_device device;
    std::mt19937 generator(device());
    std::shuffle(res.begin(), res.end(), generator);

    return res;
}

} // namespace ignite::network::detail
