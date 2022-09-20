/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
        m_value(std::forward<T>(value)),
        m_error(std::nullopt) { }

    /**
     * Constructor.
     *
     * @param message Message.
     */
    IgniteResult(IgniteError&& error) : // NOLINT(google-explicit-constructor)
        m_value(std::nullopt),
        m_error(std::forward<IgniteError>(error)) { }

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
    std::optional<T>&& getValue() noexcept
    {
        return std::move(m_value);
    }

    /**
     * Get value.
     *
     * @return Value.
     */
    [[nodiscard]]
    const std::optional<T>& getValue() const noexcept
    {
        return m_value;
    }

    /**
     * Get error.
     *
     * @return Error.
     */
    [[nodiscard]]
    std::optional<IgniteError>&& getError() noexcept
    {
        return std::move(m_error);
    }

    /**
     * Get error.
     *
     * @return Error.
     */
    [[nodiscard]]
    const std::optional<IgniteError>& getError() const noexcept
    {
        return m_error;
    }

    /**
     * Wrap operation result in IgniteResult.
     * @param operation Operation to wrap.
     * @return IgniteResult
     */
    static IgniteResult ofOperation(const std::function<T()>& operation) noexcept {
        try {
            return operation();
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
        return [pr = std::move(pr)] (IgniteResult<T> res) mutable {
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
        m_error(std::forward<IgniteError>(error)) { }

    /**
     * Has value.
     *
     * @return @c true if the result has value.
     */
    [[nodiscard]]
    bool hasValue() const noexcept
    {
        return !hasError();
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
     * Get error.
     *
     * @return Error.
     */
    [[nodiscard]]
    std::optional<IgniteError>&& getError() noexcept
    {
        return std::move(m_error);
    }

    /**
     * Get error.
     *
     * @return Error.
     */
    [[nodiscard]]
    const std::optional<IgniteError>& getError() const noexcept
    {
        return m_error;
    }

    /**
     * Wrap operation result in IgniteResult.
     * @param operation Operation to wrap.
     * @return IgniteResult
     */
    static IgniteResult ofOperation(const std::function<void()>& operation) noexcept {
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
        return [pr = std::move(pr)] (IgniteResult<void> res) mutable {
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

} // namespace ignite
