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
#include "ignite/protocol/messages.h"
#include "ignite/protocol/reader.h"

#include <functional>
#include <future>
#include <memory>
#include <tuple>

namespace ignite::detail {

class node_connection;

/**
 * response handler.
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
    [[nodiscard]] virtual ignite_result<void> handle(std::shared_ptr<node_connection>, bytes_view, std::int32_t) = 0;

    /**
     * Set error.
     */
    [[nodiscard]] virtual ignite_result<void> set_error(ignite_error) = 0;

    /**
     * Check whether handling is complete.
     *
     * @return @c true if the handling is complete and false otherwise.
     */
    [[nodiscard]] bool is_handling_complete() const { return m_handling_complete; }

protected:
    /** Handling completion flag. */
    bool m_handling_complete{false};
};

/**
 * Response handler raw implementation.
 */
class response_handler_raw final : public response_handler {
public:
    // Default
    response_handler_raw() = default;

    /**
     * Constructor.
     *
     * @param callback Callback.
     */
    explicit response_handler_raw(ignite_callback<bytes_view> callback)
        : m_callback(std::move(callback)) {}

    /**
     * Handle response.
     *
     * @param msg Message.
     */
    [[nodiscard]] ignite_result<void> handle(std::shared_ptr<node_connection>, bytes_view msg, std::int32_t) final {
        auto res = result_of_operation<void>([&]() { return m_callback(bytes_view{msg}); });
        if (res.has_error()) {
            m_callback(std::move(res).error());
        }
        this->m_handling_complete = true;
        return res;
    }

    /**
     * Set error.
     *
     * @param err Error to set.
     */
    [[nodiscard]] ignite_result<void> set_error(ignite_error err) override {
        auto res = result_of_operation<void>([&]() { m_callback({std::move(err)}); });
        m_handling_complete = true;
        return res;
    }

private:
    /** Callback. */
    ignite_callback<bytes_view> m_callback;
};

/**
 * Response handler adapter.
 */
template<typename T>
class response_handler_adapter : public response_handler {
public:
    // Default
    response_handler_adapter() = default;

    /**
     * Constructor.
     *
     * @param callback Callback.
     */
    explicit response_handler_adapter(ignite_callback<T> callback)
        : m_callback(std::move(callback)) {}

    /**
     * Set error.
     *
     * @param err Error to set.
     */
    [[nodiscard]] ignite_result<void> set_error(ignite_error err) override {
        auto res = result_of_operation<void>([&]() { m_callback({std::move(err)}); });
        m_handling_complete = true;
        return res;
    }

protected:
    /** Callback. */
    ignite_callback<T> m_callback;
};

/**
 * Response handler implementation for bytes.
 */
template<typename T>
class response_handler_bytes final : public response_handler_adapter<T> {
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
        : response_handler_adapter<T>(std::move(callback))
        , m_read_func(std::move(read_func)) {}

    /**
     * Handle response.
     *
     * @param channel Channel.
     * @param msg Message.
     */
    [[nodiscard]] ignite_result<void> handle(
        std::shared_ptr<node_connection> channel, bytes_view msg, std::int32_t) final {
        auto read_res = result_of_operation<T>([&]() { return m_read_func(std::move(channel), msg); });
        bool read_error = read_res.has_error();

        auto handle_res = result_of_operation<void>([&]() { this->m_callback(std::move(read_res)); });
        if (!read_error && handle_res.has_error()) {
            handle_res = result_of_operation<void>([&]() { this->m_callback(std::move(handle_res.error())); });
        }

        this->m_handling_complete = true;
        return handle_res;
    }

private:
    /** Read function. */
    std::function<T(std::shared_ptr<node_connection>, bytes_view)> m_read_func;
};

/**
 * Response handler implementation for reader.
 */
template<typename T>
class response_handler_reader final : public response_handler_adapter<T> {
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
        : response_handler_adapter<T>(std::move(callback))
        , m_read_func(std::move(read_func)) {}

    /**
     * Handle response.
     *
     * @param msg Message.
     */
    [[nodiscard]] ignite_result<void> handle(std::shared_ptr<node_connection>, bytes_view msg, std::int32_t) {
        protocol::reader reader(msg);
        auto read_res = result_of_operation<T>([&]() { return m_read_func(reader); });
        bool read_error = read_res.has_error();

        auto handle_res = result_of_operation<void>([&]() { this->m_callback(std::move(read_res)); });
        if (!read_error && handle_res.has_error()) {
            handle_res = result_of_operation<void>([&]() { this->m_callback(std::move(handle_res.error())); });
        }

        this->m_handling_complete = true;
        return handle_res;
    }

private:
    /** Read function. */
    std::function<T(protocol::reader &)> m_read_func;
};

/**
 * Response handler implementation for reader.
 */
template<typename T>
class response_handler_reader_connection final : public response_handler_adapter<T> {
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
        : response_handler_adapter<T>(std::move(callback))
        , m_read_func(std::move(read_func)) {}

    /**
     * Handle response.
     *
     * @param conn Connection.
     * @param msg Message.
     */
    [[nodiscard]] ignite_result<void> handle(
        std::shared_ptr<node_connection> conn, bytes_view msg, std::int32_t) {
        protocol::reader reader(msg);
        auto read_res = result_of_operation<T>([&]() { return m_read_func(reader, conn); });
        bool read_error = read_res.has_error();

        auto handle_res = result_of_operation<void>([&]() { this->m_callback(std::move(read_res)); });
        if (!read_error && handle_res.has_error()) {
            handle_res = result_of_operation<void>([&]() { this->m_callback(std::move(handle_res.error())); });
        }

        this->m_handling_complete = true;
        return handle_res;
    }

private:
    /** Read function. */
    std::function<T(protocol::reader &, std::shared_ptr<node_connection>)> m_read_func;
};

} // namespace ignite::detail
