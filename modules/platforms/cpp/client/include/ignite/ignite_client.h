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

#include <future>
#include <memory>

#include <ignite/ignite_client_configuration.h>

namespace ignite
{

namespace impl
{
class IgniteClientImpl;
}

/**
 * Ignite client.
 */
class IgniteClient
{
public:
    // Deleted
    IgniteClient() = delete;

    // Default
    ~IgniteClient() = default;
    IgniteClient(IgniteClient&&) = default;
    IgniteClient(const IgniteClient&) = default;
    IgniteClient& operator=(IgniteClient&&) = default;
    IgniteClient& operator=(const IgniteClient&) = default;

    /**
     * Start client asynchronously.
     *
     * Client tries to establish connection to every endpoint. First endpoint is
     * selected randomly. After that round-robin is used to determine the next
     * address to establish connection to.
     *
     * System means are used to resolve endpoint IP addresses. If more than one
     * IP address is returned, client attempts to connect to them in random order.
     *
     * Only one connection establishment can be in process at the same time.
     *
     * Client considered connected to a cluster when there is at least one
     * connection to any node of the cluster. Upon this event, future will be set
     * with a usable IgniteClient instance.
     *
     * @param configuration Client configuration.
     * @return Future with either Ignite client or exception.
     */
    static std::future<IgniteClient> startAsync(IgniteClientConfiguration configuration);

private:
    /** Implementation. */
    std::shared_ptr<impl::IgniteClientImpl> m_impl;
};

} // namespace ignite