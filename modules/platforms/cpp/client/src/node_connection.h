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

#include "ignite/network/async_client_pool.h"

#include "ignite/ignite_client_configuration.h"

namespace ignite::impl
{

class ClusterConnection;

/**
 * Represents connection to the cluster.
 *
 * Considered established while there is connection to at least one server.
 */
class NodeConnection
{
    friend class ClusterConnection;
public:
    // Deleted
    NodeConnection() = delete;
    NodeConnection(const NodeConnection&) = delete;
    NodeConnection& operator=(NodeConnection&&) = delete;
    NodeConnection& operator=(const NodeConnection&) = delete;

    // Default
    ~NodeConnection() = default;
    NodeConnection(NodeConnection&&) = default;

    /**
     * Constructor.
     *
     * @param id Connection ID.
     * @param pool Connection pool.
     */
    NodeConnection(uint64_t id, std::shared_ptr<network::AsyncClientPool> pool);

private:
    /**
     * Callback that called when new message is received.
     *
     * @param msg Received message.
     */
    void processMessage(const network::DataBuffer& msg);

    /** Connection ID. */
    uint64_t m_id;

    /** Handshake result. */
    std::promise<void> handshakeRes;

    /** Connection pool. */
    std::shared_ptr<network::AsyncClientPool> m_pool;
};

} // namespace ignite::impl