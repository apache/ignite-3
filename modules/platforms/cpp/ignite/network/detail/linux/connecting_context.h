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

#include "ignite/common/end_point.h"
#include "ignite/network/detail/linux/linux_async_client.h"
#include "ignite/network/tcp_range.h"

#include <cstdint>
#include <memory>

#include <netdb.h>

namespace ignite::network::detail {

/**
 * Connecting context.
 */
class connecting_context {
public:
    // Default
    connecting_context(connecting_context &&) = default;
    connecting_context &operator=(connecting_context &&) = default;

    /**
     * Constructor.
     */
    explicit connecting_context(tcp_range range);

    /**
     * Destructor.
     */
    ~connecting_context();

    /**
     * reset connection context to it's initial state.
     */
    void reset();

    /**
     * Next address in range.
     *
     * @return Next address info for connection.
     */
    addrinfo *next();

    /**
     * Get last address.
     *
     * @return Address.
     */
    end_point current_address() const;

    /**
     * Make client.
     *
     * @param fd Socket file descriptor.
     * @return Client instance from current internal state.
     */
    std::shared_ptr<linux_async_client> to_client(int fd);

private:
    /** Range. */
    tcp_range m_range;

    /** Next port. */
    uint16_t m_next_port;

    /** Current address info. */
    addrinfo *m_info;

    /** Address info which is currently used for connection */
    addrinfo *m_current_info;
};

} // namespace ignite::network::detail
