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

#include <ignite/common/ignite_error.h>
#include <ignite/network/data_buffer.h>
#include <ignite/network/end_point.h>

#include <cstdint>

namespace ignite::network {

/**
 * Asynchronous events handler.
 */
class AsyncHandler {
public:
    /**
     * Destructor.
     */
    virtual ~AsyncHandler() = default;

    /**
     * Callback that called on successful connection establishment.
     *
     * @param addr Address of the new connection.
     * @param id Connection ID.
     */
    virtual void onConnectionSuccess(const EndPoint &addr, uint64_t id) = 0;

    /**
     * Callback that called on error during connection establishment.
     *
     * @param addr Connection address.
     * @param err Error.
     */
    virtual void onConnectionError(const EndPoint &addr, ignite_error err) = 0;

    /**
     * Callback that called on error during connection establishment.
     *
     * @param id Async client ID.
     * @param err Error. Can be null if connection closed without error.
     */
    virtual void onConnectionClosed(uint64_t id, std::optional<ignite_error> err) = 0;

    /**
     * Callback that called when new message is received.
     *
     * @param id Async client ID.
     * @param msg Received message.
     */
    virtual void onMessageReceived(uint64_t id, bytes_view msg) = 0;

    /**
     * Callback that called when message is sent.
     *
     * @param id Async client ID.
     */
    virtual void onMessageSent(uint64_t id) = 0;
};

} // namespace ignite::network
