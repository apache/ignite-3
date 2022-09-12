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

#include "ignite/ignite_client_configuration.h"

#include "cluster_connection.h"

namespace ignite::impl
{

/**
 * Ignite client implementation.
 */
class IgniteClientImpl
{
public:
    // Deleted
    IgniteClientImpl() = delete;
    IgniteClientImpl(IgniteClientImpl&&) = delete;
    IgniteClientImpl(const IgniteClientImpl&) = delete;
    IgniteClientImpl& operator=(IgniteClientImpl&&) = delete;
    IgniteClientImpl& operator=(const IgniteClientImpl&) = delete;

    // Default
    ~IgniteClientImpl() = default;

    /**
     * Constructor.
     *
     * @param configuration Configuration.
     */
    explicit IgniteClientImpl(IgniteClientConfiguration configuration) :
        m_configuration(std::move(configuration)),
        m_connection(ClusterConnection::create(m_configuration)) { }

    /**
     * Start client.
     *
     * @return Execution future,
     */
    std::future<void> start();

    /**
     * Stop client.
     */
    void stop();

private:
    /** Configuration. */
    const IgniteClientConfiguration m_configuration;

    /** Cluster connection. */
    std::shared_ptr<ClusterConnection> m_connection;
};

} // namespace ignite::impl