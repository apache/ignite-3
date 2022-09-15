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

#include <netdb.h>

#include <cstdint>
#include <memory>

#include <ignite/network/end_point.h>
#include <ignite/network/tcp_range.h>

#include "network/linux_async_client.h"

namespace ignite::network
{

/**
 * Connecting context.
 */
class ConnectingContext
{
public:
    // Deleted
    ConnectingContext() = delete;
    ConnectingContext(const ConnectingContext&) = delete;
    ConnectingContext& operator=(const ConnectingContext&) = delete;

    // Default
    ConnectingContext(ConnectingContext&&) = default;
    ConnectingContext& operator=(ConnectingContext&&) = default;

    /**
     * Constructor.
     */
    explicit ConnectingContext(TcpRange range);
    

    /**
     * Destructor.
     */
    ~ConnectingContext();

    /**
     * Reset connection context to it's initial state.
     */
    void reset();

    /**
     * Next address in range.
     *
     * @return Next address info for connection.
     */
    addrinfo* next();

    /**
     * Get last address.
     *
     * @return Address.
     */
    EndPoint getAddress() const;

    /**
     * Make client.
     *
     * @param fd Socket file descriptor.
     * @return Client instance from current internal state.
     */
    std::shared_ptr<LinuxAsyncClient> toClient(int fd);

private:
    /** Range. */
    TcpRange m_range;

    /** Next port. */
    uint16_t m_nextPort;

    /** Current address info. */
    addrinfo* m_info;

    /** Address info which is currently used for connection */
    addrinfo* m_currentInfo;
};

} // namespace ignite::network
