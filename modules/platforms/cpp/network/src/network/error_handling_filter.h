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

#include <functional>

#include <ignite/network/data_filter_adapter.h>

namespace ignite::network
{

/**
 * Filter that handles exceptions thrown by upper level handlers.
 */
class ErrorHandlingFilter : public DataFilterAdapter
{
public:
    // Default
    ~ErrorHandlingFilter() override = default;

    /**
     * Callback that called on successful connection establishment.
     *
     * @param addr Address of the new connection.
     * @param id Connection ID.
     */
    void onConnectionSuccess(const EndPoint& addr, uint64_t id) override;

    /**
     * Callback that called on error during connection establishment.
     *
     * @param addr Connection address.
     * @param err Error.
     */
    void onConnectionError(const EndPoint& addr, IgniteError err) override;

    /**
     * Callback that called on error during connection establishment.
     *
     * @param id Async client ID.
     * @param err Error. Can be null if connection closed without error.
     */
    void onConnectionClosed(uint64_t id, std::optional<IgniteError> err) override;

    /**
     * Callback that called when new message is received.
     *
     * @param id Async client ID.
     * @param msg Received message.
     */
    void onMessageReceived(uint64_t id, const DataBuffer &msg) override;

    /**
     * Callback that called when message is sent.
     *
     * @param id Async client ID.
     */
    void onMessageSent(uint64_t id) override;

private:
    /**
     * Execute function and handle all possible exceptions.
     *
     * @param id Async client ID.
     * @param func Function to handle;
     */
    void closeConnectionOnException(uint64_t id, const std::function<void()>& func);
};

} // namespace ignite::network
