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

#include <functional>
#include <future>
#include <memory>
#include <tuple>

#include "common/ignite_error.h"
#include "common/ignite_result.h"
#include "ignite/protocol/reader.h"

#pragma once

namespace ignite::detail
{

/**
 * Response handler.
 */
class ResponseHandler
{
public:
    // Default
    ResponseHandler() = default;
    virtual ~ResponseHandler() = default;
    ResponseHandler(ResponseHandler&&) = default;
    ResponseHandler(const ResponseHandler&) = default;
    ResponseHandler& operator=(ResponseHandler&&) = default;
    ResponseHandler& operator=(const ResponseHandler&) = default;

    /**
     * Handle response.
     */
    [[nodiscard]]
    virtual IgniteResult<void> handle(protocol::Reader&) = 0;

    /**
     * Set error.
     */
     [[nodiscard]]
    virtual IgniteResult<void> setError(IgniteError) = 0;
};


/**
 * Response handler implementation for specific type.
 */
template<typename T>
class ResponseHandlerImpl : public ResponseHandler
{
public:
    // Default
    ResponseHandlerImpl() = default;
    ~ResponseHandlerImpl() override = default;
    ResponseHandlerImpl(ResponseHandlerImpl&&) noexcept = default;
    ResponseHandlerImpl& operator=(ResponseHandlerImpl&&) noexcept = default;

    // Delete
    ResponseHandlerImpl(const ResponseHandlerImpl&) = delete;
    ResponseHandlerImpl& operator=(const ResponseHandlerImpl&) = delete;

    /**
     * Constructor.
     *
     * @param func Function.
     */
    explicit ResponseHandlerImpl(std::function<T(protocol::Reader&)> readFunc, IgniteCallback<T> callback) :
        m_readFunc(std::move(readFunc)),
        m_callback(std::move(callback)),
        m_mutex() { }

    /**
     * Handle response.
     *
     * @param reader Reader to be used to read response.
     */
    [[nodiscard]]
    IgniteResult<void> handle(protocol::Reader& reader) override {
        IgniteCallback<T> callback = removeCallback();
        if (!callback)
            return {};

        auto res = IgniteResult<T>::ofOperation([&] () { return m_readFunc(reader); });
        return IgniteResult<void>::ofOperation([&] () { callback(std::move(res)); } );
    }

    /**
     * Set error.
     *
     * @param err Error to set.
     */
    [[nodiscard]]
    IgniteResult<void> setError(IgniteError err) override {
        IgniteCallback<T> callback = removeCallback();
        if (!callback)
            return {};

        return IgniteResult<void>::ofOperation([&] () { callback({std::move(err)}); } );
    }

private:
    /**
     * Remove callback and return it.
     *
     * @return Callback.
     */
    IgniteCallback<T> removeCallback() {
        std::lock_guard<std::mutex> guard(m_mutex);
        IgniteCallback<T> callback = {};
        std::swap(callback, m_callback);
        return callback;
    }

    /** Read function. */
    std::function<T(protocol::Reader&)> m_readFunc;

    /** Promise. */
    IgniteCallback<T> m_callback;

    /** Callback mutex. */
    std::mutex m_mutex;
};

} // namespace ignite::detail
