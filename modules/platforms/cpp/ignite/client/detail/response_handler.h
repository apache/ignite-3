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

#include "ignite/client/detail/node_connection.h"
#include "ignite/common/ignite_error.h"
#include "ignite/common/ignite_result.h"
#include "ignite/protocol/reader.h"

#include <functional>
#include <future>
#include <memory>
#include <tuple>

namespace ignite::detail {

class node_connection;

/**
 * Response handler.
 */
class response_handler {
public:
    // Default
    response_handler() = default;
    virtual ~response_handler() = default;

    // Deleted
    response_handler(response_handler &&) = delete;
    response_handler(const response_handler &) = delete;
    response_handler &operator=(response_handler &&) = delete;
    response_handler &operator=(const response_handler &) = delete;

    /**
     * Handle response.
     */
    [[nodiscard]] virtual ignite_result<void> handle(std::shared_ptr<node_connection>, bytes_view) = 0;

    /**
     * Set error.
     */
    [[nodiscard]] virtual ignite_result<void> set_error(ignite_error) = 0;
};

/**
 * Response handler implementation for bytes.
 */
template<typename T>
class response_handler_bytes final : public response_handler {
public:
    // Default
    response_handler_bytes() = default;

    /**
     * Constructor.
     *
     * @param read_func Read function.
     * @param callback Callback.
     */
    explicit response_handler_bytes(
        std::function<T(std::shared_ptr<node_connection>, bytes_view)> read_func, ignite_callback<T> callback)
        : m_read_func(std::move(read_func))
        , m_callback(std::move(callback))
        , m_mutex() {}

    /**
     * Handle response.
     *
     * @param channel Channel.
     * @param msg Message.
     */
    [[nodiscard]] ignite_result<void> handle(std::shared_ptr<node_connection> channel, bytes_view msg) final {
        ignite_callback<T> callback = remove_callback();
        if (!callback)
            return {};

        auto res = result_of_operation<T>([&]() { return m_read_func(std::move(channel), msg); });
        return result_of_operation<void>([&]() { callback(std::move(res)); });
    }

    /**
     * Set error.
     *
     * @param err Error to set.
     */
    [[nodiscard]] ignite_result<void> set_error(ignite_error err) final {
        ignite_callback<T> callback = remove_callback();
        if (!callback)
            return {};

        return result_of_operation<void>([&]() { callback({std::move(err)}); });
    }

private:
    /**
     * Remove callback and return it.
     *
     * @return Callback.
     */
    ignite_callback<T> remove_callback() {
        std::lock_guard<std::mutex> guard(m_mutex);
        ignite_callback<T> callback = {};
        std::swap(callback, m_callback);
        return callback;
    }

    /** Read function. */
    std::function<T(std::shared_ptr<node_connection>, bytes_view)> m_read_func;

    /** Promise. */
    ignite_callback<T> m_callback;

    /** Callback mutex. */
    std::mutex m_mutex;
};

/**
 * Response handler implementation for reader.
 */
template<typename T>
class response_handler_reader final : public response_handler {
public:
    // Default
    response_handler_reader() = default;

    /**
     * Constructor.
     *
     * @param read_func Read function.
     * @param callback Callback.
     */
    explicit response_handler_reader(std::function<T(protocol::reader &)> read_func, ignite_callback<T> callback)
        : m_read_func(std::move(read_func))
        , m_callback(std::move(callback))
        , m_mutex() {}

    /**
     * Handle response.
     *
     * @param msg Message.
     */
    [[nodiscard]] ignite_result<void> handle(std::shared_ptr<node_connection>, bytes_view msg) final {
        ignite_callback<T> callback = remove_callback();
        if (!callback)
            return {};

        protocol::reader reader(msg);
        auto res = result_of_operation<T>([&]() { return m_read_func(reader); });
        return result_of_operation<void>([&]() { callback(std::move(res)); });
    }

    /**
     * Set error.
     *
     * @param err Error to set.
     */
    [[nodiscard]] ignite_result<void> set_error(ignite_error err) final {
        ignite_callback<T> callback = remove_callback();
        if (!callback)
            return {};

        return result_of_operation<void>([&]() { callback({std::move(err)}); });
    }

private:
    /**
     * Remove callback and return it.
     *
     * @return Callback.
     */
    ignite_callback<T> remove_callback() {
        std::lock_guard<std::mutex> guard(m_mutex);
        ignite_callback<T> callback = {};
        std::swap(callback, m_callback);
        return callback;
    }

    /** Read function. */
    std::function<T(protocol::reader &)> m_read_func;

    /** Promise. */
    ignite_callback<T> m_callback;

    /** Callback mutex. */
    std::mutex m_mutex;
};

/**
 * Response handler implementation for reader.
 */
template<typename T>
class response_handler_reader_connection final : public response_handler {
public:
    // Default
    response_handler_reader_connection() = default;

    /**
     * Constructor.
     *
     * @param read_func Read function.
     * @param callback Callback.
     */
    explicit response_handler_reader_connection(
        std::function<T(protocol::reader &, std::shared_ptr<node_connection>)> read_func, ignite_callback<T> callback)
        : m_read_func(std::move(read_func))
        , m_callback(std::move(callback))
        , m_mutex() {}

    /**
     * Handle response.
     *
     * @param msg Message.
     */
    [[nodiscard]] ignite_result<void> handle(std::shared_ptr<node_connection> conn, bytes_view msg) final {
        ignite_callback<T> callback = remove_callback();
        if (!callback)
            return {};

        protocol::reader reader(msg);
        auto res = result_of_operation<T>([&]() { return m_read_func(reader, conn); });
        return result_of_operation<void>([&]() { callback(std::move(res)); });
    }

    /**
     * Set error.
     *
     * @param err Error to set.
     */
    [[nodiscard]] ignite_result<void> set_error(ignite_error err) final {
        ignite_callback<T> callback = remove_callback();
        if (!callback)
            return {};

        return result_of_operation<void>([&]() { callback({std::move(err)}); });
    }

private:
    /**
     * Remove callback and return it.
     *
     * @return Callback.
     */
    ignite_callback<T> remove_callback() {
        std::lock_guard<std::mutex> guard(m_mutex);
        ignite_callback<T> callback = {};
        std::swap(callback, m_callback);
        return callback;
    }

    /** Read function. */
    std::function<T(protocol::reader &, std::shared_ptr<node_connection>)> m_read_func;

    /** Promise. */
    ignite_callback<T> m_callback;

    /** Callback mutex. */
    std::mutex m_mutex;
};

} // namespace ignite::detail
