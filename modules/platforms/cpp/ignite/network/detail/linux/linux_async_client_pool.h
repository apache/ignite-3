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

#include "linux_async_client.h"
#include "linux_async_worker_thread.h"

#include <ignite/common/ignite_error.h>
#include <ignite/network/async_client_pool.h>
#include <ignite/network/async_handler.h>
#include <ignite/network/tcp_range.h>

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>

namespace ignite::network::detail {

/**
 * Linux-specific implementation of asynchronous client pool.
 */
class linux_async_client_pool : public async_client_pool {
public:
    /**
     * Constructor
     *
     * @param handler Upper level event handler.
     */
    linux_async_client_pool();

    /**
     * Destructor.
     */
    ~linux_async_client_pool() override;

    /**
     * Start internal thread that establishes connections to provided addresses and asynchronously sends and
     * receives messages from them. Function returns either when thread is started and first connection is
     * established or failure happened.
     *
     * @param addrs Addresses to connect to.
     * @param conn_limit Connection upper limit. Zero means limit is disabled.
     *
     * @throw ignite_error on error.
     */
    void start(std::vector<tcp_range> addrs, uint32_t conn_limit) override;

    /**
     * Close all established connections and stops handling thread.
     */
    void stop() override;

    /**
     * Set handler.
     *
     * @param handler Handler to set.
     */
    void set_handler(std::weak_ptr<async_handler> handler) override { m_async_handler = std::move(handler); }

    /**
     * Send data to specific established connection.
     *
     * @param id Client ID.
     * @param data Data to be sent.
     * @return @c true if connection is present and @c false otherwise.
     *
     * @throw ignite_error on error.
     */
    bool send(uint64_t id, std::vector<std::byte> &&data) override;

    /**
     * Closes specified connection if it's established. Connection to the specified address is planned for
     * re-connect. Event is issued to the handler with specified error.
     *
     * @param id Client ID.
     */
    void close(uint64_t id, std::optional<ignite_error> err) override;

    /**
     * Closes and releases memory allocated for client with specified ID.
     * Error is reported to handler.
     *
     * @param id Client ID.
     * @param err Error to report. May be null.
     * @return @c true if connection with specified ID was found.
     */
    void close_and_release(uint64_t id, std::optional<ignite_error> err);

    /**
     * Add client to connection map. Notify user.
     *
     * @param client Client.
     * @return Client ID.
     */
    bool add_client(std::shared_ptr<linux_async_client> client);

    /**
     * Handle error during connection establishment.
     *
     * @param addr Connection address.
     * @param err Error.
     */
    void handle_connection_error(const end_point &addr, ignite_error err);

    /**
     * Handle successful connection establishment.
     *
     * @param addr Address of the new connection.
     * @param id Connection ID.
     */
    void handle_connection_success(const end_point &addr, uint64_t id);

    /**
     * Handle error during connection establishment.
     *
     * @param id Async client ID.
     * @param err Error. Can be null if connection closed without error.
     */
    void handle_connection_closed(uint64_t id, std::optional<ignite_error> err);

    /**
     * Handle new message.
     *
     * @param id Async client ID.
     * @param msg Received message.
     */
    void handle_message_received(uint64_t id, bytes_view msg);

    /**
     * Handle sent message event.
     *
     * @param id Async client ID.
     */
    void handle_message_sent(uint64_t id);

private:
    /**
     * Close all established connections and stops handling threads.
     */
    void internal_stop();

    /**
     * Find client by ID.
     *
     * @param id Client ID.
     * @return Client. Null pointer if is not found.
     */
    std::shared_ptr<linux_async_client> find_client(uint64_t id) const;

    /** Flag indicating that pool is stopping. */
    volatile bool m_stopping;

    /** Event handler. */
    std::weak_ptr<async_handler> m_async_handler;

    /** Worker thread. */
    linux_async_worker_thread m_worker_thread;

    /** ID counter. */
    uint64_t m_id_gen;

    /** Clients critical section. */
    mutable std::mutex m_clients_mutex;

    /** Client mapping ID -> client */
    std::map<uint64_t, std::shared_ptr<linux_async_client>> m_client_id_map;
};

} // namespace ignite::network::detail
