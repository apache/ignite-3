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
#include <string>
#include <optional>
#include <future>
#include <functional>

#include "common/ignite_error.h"

namespace ignite
{

/**
 * Ignite Result.
 */
template<typename T>
class IgniteResult
{
public:
    // Default
    IgniteResult() = default;
    ~IgniteResult() = default;
    IgniteResult(IgniteResult&&) noexcept = default;
    IgniteResult(const IgniteResult&) = default;
    IgniteResult& operator=(IgniteResult&&) noexcept = default;
    IgniteResult& operator=(const IgniteResult&) = default;

    /**
     * Constructor.
     *
     * @param value Value.
     */
    IgniteResult(T&& value) : // NOLINT(google-explicit-constructor)
        m_value(std::move(value)),
        m_error(std::nullopt) { }

    /**
     * Constructor.
     *
     * @param message Message.
     */
    IgniteResult(IgniteError&& error) : // NOLINT(google-explicit-constructor)
        m_value(std::nullopt),
        m_error(std::move(error)) { }

    /**
     * Has value.
     *
     * @return @c true if the result has value.
     */
    [[nodiscard]]
    bool hasValue() const noexcept
    {
        return m_value.has_value();
    }

    /**
     * Has error.
     *
     * @return @c true if the result has error.
     */
    [[nodiscard]]
    bool hasError() const noexcept
    {
        return m_error.has_value();
    }

    /**
     * Get value.
     *
     * @return Value.
     */
    [[nodiscard]]
    T&& getValue() &&
    {
        if (!hasValue())
            throw IgniteError("No value is present in result");

        return std::move(m_value.value());
    }

    /**
     * Get value.
     *
     * @return Value.
     */
    [[nodiscard]]
    const T& getValue() const &
    {
        if (!hasValue())
            throw IgniteError("No value is present in result");

        return m_value.value();
    }

    /**
     * Get value.
     *
     * @return Value.
     */
    [[nodiscard]]
    T& getValue() &
    {
        if (!hasValue())
            throw IgniteError("No value is present in result");

        return m_value.value();
    }

    /**
     * Get error.
     *
     * @return Error.
     */
    [[nodiscard]]
    IgniteError&& getError() &&
    {
        if (!hasError())
            throw IgniteError("No error is present in result");

        return std::move(m_error.value());
    }

    /**
     * Get error.
     *
     * @return Error.
     */
    [[nodiscard]]
    const IgniteError& getError() const &
    {
        if (!hasError())
            throw IgniteError("No error is present in result");

        return m_error.value();
    }

    /**
     * Get error.
     *
     * @return Error.
     */
    [[nodiscard]]
    IgniteError& getError() &
    {
        if (!hasError())
            throw IgniteError("No error is present in result");

        return m_error.value();
    }

    /**
     * Bool operator. Can be used to check result for an error.
     *
     * @return @c true if result does not contain error.
     */
    explicit operator bool() const noexcept {
        return !hasError();
    }

    /**
     * Wrap operation result in IgniteResult.
     * @param operation Operation to wrap.
     * @return IgniteResult
     */
    static IgniteResult ofOperation(const std::function<T()>& operation) noexcept {
        // TODO: IGNITE-17760 Move to common once it's re-factored
        try {
            return {operation()};
        } catch (const IgniteError& err) {
            return {IgniteError(err)};
        } catch (const std::exception& err) {
            std::string msg("Standard library exception is thrown: ");
            msg += err.what();
            return {IgniteError(StatusCode::GENERIC, msg, std::current_exception())};
        } catch (...)
        {
            return {IgniteError(StatusCode::UNKNOWN, "Unknown error is encountered when processing network event",
                                std::current_exception())};
        }
    }

    /**
     * Return promise setter.
     *
     * @param pr Promise.
     * @return Promise setter.
     */
    static std::function<void(IgniteResult<T>)> promiseSetter(std::shared_ptr<std::promise<T>> pr) {
        // TODO: IGNITE-17760 Move to common once it's re-factored
        return [pr = std::move(pr)] (IgniteResult<T>&& res) mutable {
            IgniteResult<T>::setPromise(*pr, std::move(res));
        };
    }

private:
    /**
     * Set promise from result.
     *
     * @param pr Promise to set.
     */
    static void setPromise(std::promise<T>& pr, IgniteResult res)
    {
        if (res.hasError()) {
            pr.set_exception(std::make_exception_ptr(std::move(res.m_error.value())));
        } else {
            pr.set_value(std::move(res.m_value.value()));
        }
    }

    /** Value. */
    std::optional<T> m_value;

    /** Error. */
    std::optional<IgniteError> m_error;
};

/**
 * Ignite Result.
 */
template<>
class IgniteResult<void>
{
public:
    // Default
    ~IgniteResult() = default;
    IgniteResult(IgniteResult&&) noexcept = default;
    IgniteResult(const IgniteResult&) = default;
    IgniteResult& operator=(IgniteResult&&) noexcept = default;
    IgniteResult& operator=(const IgniteResult&) = default;

    /**
     * Constructor.
     */
    IgniteResult() :
        m_error(std::nullopt) { }

    /**
     * Constructor.
     *
     * @param message Message.
     */
    IgniteResult(IgniteError&& error) : // NOLINT(google-explicit-constructor)
        m_error(std::move(error)) { }

    /**
     * Has error.
     *
     * @return @c true if the result has error.
     */
    [[nodiscard]]
    bool hasError() const noexcept
    {
        return m_error.has_value();
    }

    /**
     * Get error.
     *
     * @return Error.
     */
    [[nodiscard]]
    IgniteError&& getError()
    {
        if (!hasError())
            throw IgniteError("No error is present in result");

        return std::move(m_error.value());
    }

    /**
     * Get error.
     *
     * @return Error.
     */
    [[nodiscard]]
    const IgniteError& getError() const
    {
        if (!hasError())
            throw IgniteError("No error is present in result");

        return m_error.value();
    }

    /**
     * Bool operator. Can be used to check result for an error.
     *
     * @return @c true if result does not contain error.
     */
    explicit operator bool() const noexcept {
        return !hasError();
    }

    /**
     * Wrap operation result in IgniteResult.
     * @param operation Operation to wrap.
     * @return IgniteResult
     */
    static IgniteResult ofOperation(const std::function<void()>& operation) noexcept {
        // TODO: IGNITE-17760 Move to common once it's re-factored
        try {
            operation();
            return {};
        } catch (const IgniteError& err) {
            return {IgniteError(err)};
        } catch (const std::exception& err) {
            std::string msg("Standard library exception is thrown: ");
            msg += err.what();
            return {IgniteError(StatusCode::GENERIC, msg, std::current_exception())};
        } catch (...) {
            return {IgniteError(StatusCode::UNKNOWN, "Unknown error is encountered when processing network event",
                std::current_exception())};
        }
    }

    /**
     * Return promise setter.
     *
     * @param pr Promise.
     * @return Promise setter.
     */
    static std::function<void(IgniteResult<void>)> promiseSetter(std::shared_ptr<std::promise<void>> pr) {
        // TODO: IGNITE-17760 Move to common once it's re-factored
        return [pr = std::move(pr)] (IgniteResult<void>&& res) mutable {
            IgniteResult<void>::setPromise(*pr, std::move(res));
        };
    }

private:
    /**
     * Set promise from result.
     *
     * @param pr Promise to set.
     */
    static void setPromise(std::promise<void>& pr, IgniteResult res)
    {
        if (res.hasError()) {
            pr.set_exception(std::make_exception_ptr(std::move(res.m_error.value())));
        } else {
            pr.set_value();
        }
    }

    /** Error. */
    std::optional<IgniteError> m_error;
};

template<typename T>
using IgniteCallback = std::function<void(IgniteResult<T>&&)>;

} // namespace ignite
