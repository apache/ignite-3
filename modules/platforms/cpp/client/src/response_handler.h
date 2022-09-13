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
#include "ignite/protocol/reader.h"

#pragma once

namespace ignite::impl
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
    virtual void handle(protocol::Reader&) = 0;

    /**
     * Set error.
     */
    virtual void setError(IgniteError) = 0;
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
    ResponseHandlerImpl(const ResponseHandlerImpl&) = default;
    ResponseHandlerImpl& operator=(ResponseHandlerImpl&&) noexcept = default;
    ResponseHandlerImpl& operator=(const ResponseHandlerImpl&) = default;

    /**
     * Constructor.
     *
     * @param func Function.
     */
    explicit ResponseHandlerImpl(std::function<T(protocol::Reader&)> func) :
        m_func(std::move(func)),
        m_promise() { }

    /**
     * Handle response.
     *
     * @param reader Reader to be used to read response.
     */
    void handle(protocol::Reader& reader) override
    {
        try
        {
            T res = m_func(reader);
            m_promise.set_value(std::move(res));
        }
        catch (...)
        {
            m_promise.set_exception(std::current_exception());
        }
    }

    /**
     * Set error.
     *
     * @param err Error to set.
     */
    void setError(IgniteError err) override
    {
        m_promise.set_exception(std::make_exception_ptr(std::move(err)));
    }

    /**
     * Get future.
     *
     * @return Future.
     */
    [[nodiscard]]
    std::future<T> getFuture()
    {
        return m_promise.get_future();
    }

private:
    /** Handler. */
    std::function<T(protocol::Reader&)> m_func;

    /** Promise. */
    std::promise<T> m_promise;
};

} // namespace ignite::impl
