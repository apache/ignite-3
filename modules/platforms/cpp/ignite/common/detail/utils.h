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

#include <ignite_error.h>

#include <future>
#include <memory>
#include <utility>
#include <optional>
#include <string>

namespace ignite::detail {

/**
 * Make future error.
 *
 * @tparam T Value type.
 * @param err Error.
 * @return Failed future with the specified error.
 */
template<typename T>
std::future<T> make_future_error(ignite_error err) {
    std::promise<T> promise;
    promise.set_exception(std::make_exception_ptr(std::move(err)));

    return promise.get_future();
}

/**
 * Make future value.
 *
 * @tparam T Value type.
 * @param value Value.
 * @return Failed future with the specified error.
 */
template<typename T>
std::future<T> make_future_value(T value) {
    std::promise<T> promise;
    promise.set_value(std::move(value));

    return promise.get_future();
}

/**
 * Get environment variable.
 *
 * @param name Variable name.
 * @return Variable value if it is set, or @c std::nullopt otherwise.
 */
inline std::optional<std::string> get_env(const std::string &name) {
    const char *env = std::getenv(name.c_str());
    if (!env)
        return {};

    return env;
}

/**
 * Normalizes nanosecond portion of temporal type according to chosen precision.
 *
 * @param nanos Nanoseconds.
 * @param precision Precision, 0 means one-second precision 9 means nanosecond precision.
 * @return Truncated nanoseconds.
 */
inline std::int_least32_t normalize_nanos(std::int32_t nanos, std::int32_t precision) {
    switch (precision) {
        case 0:
            return 0;
        case 1:
            return nanos / 100'000'000 * 100'000'000; // 100ms precision
        case 2:
            return nanos / 10'000'000 * 10'000'000; // 10ms precision
        case 3:
            return nanos / 1'000'000 * 1'000'000; // 1ms precision
        case 4:
            return nanos / 100'000 * 100'000; // 100us precision
        case 5:
            return nanos / 10'000 * 10'000; // 10us precision
        case 6:
            return nanos / 1'000 * 1'000; // 1us precision
        case 7:
            return nanos / 100 * 100; // 100ns precision
        case 8:
            return nanos / 10 * 10; // 10ns precision
        case 9:
        default:
            return nanos; // 1ns precision
    }
}

} // namespace ignite::detail
