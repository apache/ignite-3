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

#include <ignite/network/data_filter.h>

#include <optional>

namespace ignite::network {

/**
 * Data filter adapter.
 */
class data_filter_adapter : public data_filter {
public:
    /**
     * Send data to specific established connection.
     *
     * @param id Client ID.
     * @param data Data to be sent.
     * @return @c true if connection is present and @c false otherwise.
     *
     * @throw IgniteError on error.
     */
    bool send(uint64_t id, std::vector<std::byte> &&data) override {
        if (m_sink)
            return m_sink->send(id, std::move(data));

        return false;
    }

    /**
     * Closes specified connection if it's established. Connection to the specified address is planned for
     * re-connect. Error is reported to handler.
     *
     * @param id Client ID.
     */
    void close(uint64_t id, std::optional<ignite_error> err) override {
        if (m_sink)
            m_sink->close(id, std::move(err));
    }

    /**
     * Callback that called on successful connection establishment.
     *
     * @param addr Address of the new connection.
     * @param id Connection ID.
     */
    void on_connection_success(const end_point &addr, uint64_t id) override {
        if (auto handler = m_handler.lock())
            handler->on_connection_success(addr, id);
    }

    /**
     * Callback that called on error during connection establishment.
     *
     * @param addr Connection address.
     * @param err Error.
     */
    void on_connection_error(const end_point &addr, ignite_error err) override {
        if (auto handler = m_handler.lock())
            handler->on_connection_error(addr, std::move(err));
    }

    /**
     * Callback that called on error during connection establishment.
     *
     * @param id Async client ID.
     * @param err Error. Can be null if connection closed without error.
     */
    void on_connection_closed(uint64_t id, std::optional<ignite_error> err) override {
        if (auto handler = m_handler.lock())
            handler->on_connection_closed(id, std::move(err));
    }

    /**
     * Callback that called when new message is received.
     *
     * @param id Async client ID.
     * @param msg Received message.
     */
    void on_message_received(uint64_t id, bytes_view msg) override {
        if (auto handler = m_handler.lock())
            handler->on_message_received(id, msg);
    }

    /**
     * Callback that called when message is sent.
     *
     * @param id Async client ID.
     */
    void on_message_sent(uint64_t id) override {
        if (auto handler = m_handler.lock())
            handler->on_message_sent(id);
    }
};

} // namespace ignite::network
