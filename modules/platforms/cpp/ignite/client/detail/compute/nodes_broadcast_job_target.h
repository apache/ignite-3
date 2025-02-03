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

#include "ignite/client/compute/broadcast_job_target.h"

namespace ignite::detail {

/**
 * Job target represented by a set of nodes.
 */
class nodes_broadcast_job_target : public broadcast_job_target {
public:
    // Default
    nodes_broadcast_job_target() = default;

    /**
     * Constructor.
     *
     * @param nodes Nodes.
     */
    explicit nodes_broadcast_job_target(std::set<cluster_node> &&nodes)
        : m_nodes(std::move(nodes)) {}

    /**
     * Get nodes.
     *
     * @return Nodes.
     */
    [[nodiscard]] const std::set<cluster_node> &get_nodes() const { return m_nodes; }

private:
    /** Nodes. */
    std::set<cluster_node> m_nodes;
};

} // namespace ignite::detail
