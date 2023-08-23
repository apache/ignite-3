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

#include "ignite/protocol/protocol_version.h"

#include "ignite/common/uuid.h"

namespace ignite::protocol {

/**
 * Represents connection to the cluster.
 *
 * Considered established while there is connection to at least one server.
 */
class protocol_context {
public:
    /**
     * Get protocol version.
     *
     * @return protocol version.
     */
    [[nodiscard]] protocol_version get_version() const { return m_version; }

    /**
     * Set version.
     *
     * @param ver Version to set.
     */
    void set_version(protocol_version ver) { m_version = ver; }

    /**
     * Get cluster ID.
     *
     * @return Cluster ID.
     */
    [[nodiscard]] uuid get_cluster_id() const { return m_cluster_id; }

    /**
     * Set Cluster ID.
     *
     * @param id Cluster ID to set.
     */
    void set_cluster_id(uuid id) { m_cluster_id = id; }

private:
    /** Protocol version. */
    protocol_version m_version{protocol_version::get_current()};

    /** Cluster ID. */
    uuid m_cluster_id;
};

} // namespace ignite::protocol
