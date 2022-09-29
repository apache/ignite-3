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

namespace ignite::network {

/**
 * Data filter adapter.
 */
class DataFilterAdapter : public DataFilter {
public:
    // Default
    DataFilterAdapter() = default;

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
        DataSink *sink = m_sink;
        if (sink)
            return sink->send(id, std::move(data));

        return false;
    }

    /**
     * Closes specified connection if it's established. Connection to the specified address is planned for
     * re-connect. Error is reported to handler.
     *
     * @param id Client ID.
     */
    void close(uint64_t id, std::optional<IgniteError> err) override {
        DataSink *sink = m_sink;
        if (sink)
            sink->close(id, std::move(err));
    }

    /**
     * Callback that called on successful connection establishment.
     *
     * @param addr Address of the new connection.
     * @param id Connection ID.
     */
    void onConnectionSuccess(const EndPoint &addr, uint64_t id) override {
        auto handler = m_handler.lock();
        if (handler)
            handler->onConnectionSuccess(addr, id);
    }

    /**
     * Callback that called on error during connection establishment.
     *
     * @param addr Connection address.
     * @param err Error.
     */
    void onConnectionError(const EndPoint &addr, IgniteError err) override {
        auto handler = m_handler.lock();
        if (handler)
            handler->onConnectionError(addr, std::move(err));
    }

    /**
     * Callback that called on error during connection establishment.
     *
     * @param id Async client ID.
     * @param err Error. Can be null if connection closed without error.
     */
    void onConnectionClosed(uint64_t id, std::optional<IgniteError> err) override {
        auto handler = m_handler.lock();
        if (handler)
            handler->onConnectionClosed(id, std::move(err));
    }

    /**
     * Callback that called when new message is received.
     *
     * @param id Async client ID.
     * @param msg Received message.
     */
    void onMessageReceived(uint64_t id, BytesView msg) override {
        auto handler = m_handler.lock();
        if (handler)
            handler->onMessageReceived(id, msg);
    }

    /**
     * Callback that called when message is sent.
     *
     * @param id Async client ID.
     */
    void onMessageSent(uint64_t id) override {
        auto handler = m_handler.lock();
        if (handler)
            handler->onMessageSent(id);
    }
};

} // namespace ignite::network
