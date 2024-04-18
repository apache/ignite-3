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

#include <cstdint>
#include <functional>
#include <future>
#include <optional>
#include <string>
#include <variant>

namespace ignite {

/**
 * Ignite Result.
 */
template<typename T>
class ignite_result {
public:
    // Default
    ignite_result() = default;

    /**
     * Constructor.
     *
     * @param value Value.
     */
    ignite_result(T &&value) // NOLINT(google-explicit-constructor)
        : m_value(std::move(value)) {}

    /**
     * Constructor.
     *
     * @param message Message.
     */
    ignite_result(ignite_error &&error) // NOLINT(google-explicit-constructor)
        : m_value(std::move(error)) {}

    /**
     * Has value.
     *
     * @return @c true if the result has value.
     */
    [[nodiscard]] bool has_value() const noexcept { return !has_error(); }

    /**
     * Has error.
     *
     * @return @c true if the result has error.
     */
    [[nodiscard]] bool has_error() const noexcept { return std::holds_alternative<ignite_error>(m_value); }

    /**
     * Get value.
     *
     * @return Value.
     */
    [[nodiscard]] T &&value() && {
        if (!has_value())
            throw ignite_error("No value is present in result");

        return std::get<T>(std::move(m_value));
    }

    /**
     * Get value.
     *
     * @return Value.
     */
    [[nodiscard]] const T &value() const & {
        if (!has_value())
            throw ignite_error("No value is present in result");

        return std::get<T>(m_value);
    }

    /**
     * Get value.
     *
     * @return Value.
     */
    [[nodiscard]] T &value() & {
        if (!has_value())
            throw ignite_error("No value is present in result");

        return std::get<T>(m_value);
    }

    /**
     * Get error.
     *
     * @return Error.
     */
    [[nodiscard]] ignite_error &&error() && {
        if (!has_error())
            throw ignite_error("No error is present in result");

        return std::move(std::get<ignite_error>(m_value));
    }

    /**
     * Get error.
     *
     * @return Error.
     */
    [[nodiscard]] const ignite_error &error() const & {
        if (!has_error())
            throw ignite_error("No error is present in result");

        return std::get<ignite_error>(m_value);
    }

    /**
     * Get error.
     *
     * @return Error.
     */
    [[nodiscard]] ignite_error &error() & {
        if (!has_error())
            throw ignite_error("No error is present in result");

        return std::get<ignite_error>(m_value);
    }

    /**
     * Bool operator.
     * Can be used to check result for an error.
     *
     * @return @c true if result does not contain error.
     */
    explicit operator bool() const noexcept { return !has_error(); }

private:
    /** Value. */
    std::variant<ignite_error, T> m_value;
};

/**
 * Ignite Result.
 */
template<>
class ignite_result<void> {
public:
    /**
     * Constructor.
     */
    ignite_result()
        : m_error(std::nullopt) {}

    /**
     * Constructor.
     *
     * @param message Message.
     */
    ignite_result(ignite_error &&error) // NOLINT(google-explicit-constructor)
        : m_error(std::move(error)) {}

    /**
     * Has error.
     *
     * @return @c true if the result has error.
     */
    [[nodiscard]] bool has_error() const noexcept { return m_error.has_value(); }

    /**
     * Get error.
     *
     * @return Error.
     */
    [[nodiscard]] ignite_error &&error() {
        if (!has_error())
            throw ignite_error("No error is present in result");

        return std::move(m_error.value());
    }

    /**
     * Get error.
     *
     * @return Error.
     */
    [[nodiscard]] const ignite_error &error() const {
        if (!has_error())
            throw ignite_error("No error is present in result");

        return m_error.value();
    }

    /**
     * Bool operator.
     * Can be used to check result for an error.
     *
     * @return @c true if result does not contain error.
     */
    explicit operator bool() const noexcept { return !has_error(); }

private:
    /** Error. */
    std::optional<ignite_error> m_error;
};

template<typename T>
using ignite_callback = std::function<void(ignite_result<T> &&)>;

/**
 * Wrap operation result in ignite_result.
 *
 * @param operation Operation to wrap.
 * @return ignite_result
 */
template<typename T>
ignite_result<T> result_of_operation(const std::function<T()> &operation) noexcept {
    try {
        if constexpr (std::is_same<decltype(operation()), void>::value) {
            operation();
            return {};
        } else {
            return {operation()};
        }
    } catch (const ignite_error &err) {
        return {ignite_error(err)};
    } catch (const std::exception &err) {
        std::string msg("Standard library exception is thrown: ");
        msg += err.what();
        return {ignite_error(error::code::GENERIC, msg, std::current_exception())};
    } catch (...) {
        return {ignite_error(error::code::INTERNAL, "Unknown error is encountered when processing network event",
            std::current_exception())};
    }
}

/**
 * Set promise from result.
 *
 * @param pr Promise to set.
 * @param res Result to use.
 */
template<typename T>
void result_set_promise(std::promise<T> &pr, ignite_result<T> &&res) {
    if (!res) {
        pr.set_exception(std::make_exception_ptr(std::move(res).error()));
    } else {
        if constexpr (std::is_same<T, void>::value) {
            pr.set_value();
        } else {
            pr.set_value(std::move(res).value());
        }
    }
}

/**
 * Get promise setter for a promise to be used with ignite result.
 *
 * @param pr Promise.
 * @return Promise setter.
 */
template<typename T>
std::function<void(ignite_result<T>)> result_promise_setter(std::shared_ptr<std::promise<T>> pr) {
    return [pr = std::move(pr)](ignite_result<T> &&res) mutable { result_set_promise<T>(*pr, std::move(res)); };
}

/**
 * Synchronously calls async function.
 *
 * @param pr Promise.
 * @return Promise setter.
 */
template<typename T>
T sync(std::function<void(ignite_callback<T>)> func) {
    auto promise = std::make_shared<std::promise<T>>();
    func(result_promise_setter(promise));
    return promise->get_future().get();
}

} // namespace ignite
