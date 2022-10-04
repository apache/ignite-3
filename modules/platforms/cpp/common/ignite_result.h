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
#include <functional>
#include <future>
#include <optional>
#include <string>

#include "common/ignite_error.h"

namespace ignite {

/**
 * Ignite Result.
 */
template <typename T>
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
        : m_value(std::move(value))
        , m_error(std::nullopt) { }

    /**
     * Constructor.
     *
     * @param message Message.
     */
    ignite_result(ignite_error &&error) // NOLINT(google-explicit-constructor)
        : m_value(std::nullopt)
        , m_error(std::move(error)) { }

    /**
     * Has value.
     *
     * @return @c true if the result has value.
     */
    [[nodiscard]] bool has_value() const noexcept { return m_value.has_value(); }

    /**
     * Has error.
     *
     * @return @c true if the result has error.
     */
    [[nodiscard]] bool has_error() const noexcept { return m_error.has_value(); }

    /**
     * Get value.
     *
     * @return Value.
     */
    [[nodiscard]] T &&value() && {
        if (!has_value())
            throw ignite_error("No value is present in result");

        return std::move(m_value.value());
    }

    /**
     * Get value.
     *
     * @return Value.
     */
    [[nodiscard]] const T &value() const & {
        if (!has_value())
            throw ignite_error("No value is present in result");

        return m_value.value();
    }

    /**
     * Get value.
     *
     * @return Value.
     */
    [[nodiscard]] T &value() & {
        if (!has_value())
            throw ignite_error("No value is present in result");

        return m_value.value();
    }

    /**
     * Get error.
     *
     * @return Error.
     */
    [[nodiscard]] ignite_error &&error() && {
        if (!has_error())
            throw ignite_error("No error is present in result");

        return std::move(m_error.value());
    }

    /**
     * Get error.
     *
     * @return Error.
     */
    [[nodiscard]] const ignite_error &error() const & {
        if (!has_error())
            throw ignite_error("No error is present in result");

        return m_error.value();
    }

    /**
     * Get error.
     *
     * @return Error.
     */
    [[nodiscard]] ignite_error &error() & {
        if (!has_error())
            throw ignite_error("No error is present in result");

        return m_error.value();
    }

    /**
     * Bool operator. Can be used to check result for an error.
     *
     * @return @c true if result does not contain error.
     */
    explicit operator bool() const noexcept { return !has_error(); }

    /**
     * Wrap operation result in ignite_result.
     * @param operation Operation to wrap.
     * @return ignite_result
     */
    static ignite_result of_operation(const std::function<T()> &operation) noexcept {
        // TODO: IGNITE-17760 Move to common once it's re-factored
        try {
            return {operation()};
        } catch (const ignite_error &err) {
            return {ignite_error(err)};
        } catch (const std::exception &err) {
            std::string msg("Standard library exception is thrown: ");
            msg += err.what();
            return {ignite_error(status_code::GENERIC, msg, std::current_exception())};
        } catch (...) {
            return {ignite_error(status_code::UNKNOWN, "Unknown error is encountered when processing network event",
                std::current_exception())};
        }
    }

    /**
     * Return promise setter.
     *
     * @param pr Promise.
     * @return Promise setter.
     */
    static std::function<void(ignite_result<T>)> promise_setter(std::shared_ptr<std::promise<T>> pr) {
        // TODO: IGNITE-17760 Move to common once it's re-factored
        return [pr = std::move(pr)](
                   ignite_result<T> &&res) mutable { ignite_result<T>::set_promise(*pr, std::move(res)); };
    }

private:
    /**
     * Set promise from result.
     *
     * @param pr Promise to set.
     */
    static void set_promise(std::promise<T> &pr, ignite_result res) {
        if (res.has_error()) {
            pr.set_exception(std::make_exception_ptr(std::move(res.m_error.value())));
        } else {
            pr.set_value(std::move(res.m_value.value()));
        }
    }

    /** Value. */
    std::optional<T> m_value;

    /** Error. */
    std::optional<ignite_error> m_error;
};

/**
 * Ignite Result.
 */
template <>
class ignite_result<void> {
public:
    /**
     * Constructor.
     */
    ignite_result()
        : m_error(std::nullopt) { }

    /**
     * Constructor.
     *
     * @param message Message.
     */
    ignite_result(ignite_error &&error)
        : // NOLINT(google-explicit-constructor)
        m_error(std::move(error)) { }

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
     * Bool operator. Can be used to check result for an error.
     *
     * @return @c true if result does not contain error.
     */
    explicit operator bool() const noexcept { return !has_error(); }

    /**
     * Wrap operation result in ignite_result.
     * @param operation Operation to wrap.
     * @return ignite_result
     */
    static ignite_result of_operation(const std::function<void()> &operation) noexcept {
        // TODO: IGNITE-17760 Move to common once it's re-factored
        try {
            operation();
            return {};
        } catch (const ignite_error &err) {
            return {ignite_error(err)};
        } catch (const std::exception &err) {
            std::string msg("Standard library exception is thrown: ");
            msg += err.what();
            return {ignite_error(status_code::GENERIC, msg, std::current_exception())};
        } catch (...) {
            return {ignite_error(status_code::UNKNOWN, "Unknown error is encountered when processing network event",
                std::current_exception())};
        }
    }

    /**
     * Return promise setter.
     *
     * @param pr Promise.
     * @return Promise setter.
     */
    static std::function<void(ignite_result<void>)> promise_setter(std::shared_ptr<std::promise<void>> pr) {
        // TODO: IGNITE-17760 Move to common once it's re-factored
        return [pr = std::move(pr)](
                   ignite_result<void> &&res) mutable { ignite_result<void>::set_promise(*pr, std::move(res)); };
    }

private:
    /**
     * Set promise from result.
     *
     * @param pr Promise to set.
     */
    static void set_promise(std::promise<void> &pr, ignite_result res) {
        if (res.has_error()) {
            pr.set_exception(std::make_exception_ptr(std::move(res.m_error.value())));
        } else {
            pr.set_value();
        }
    }

    /** Error. */
    std::optional<ignite_error> m_error;
};

template <typename T>
using ignite_callback = std::function<void(ignite_result<T> &&)>;

} // namespace ignite
