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

#include "ignite/client/compute/job_target.h"
#include "ignite/client/detail/compute/job_target_type.h"

namespace ignite::detail {

/**
 * Job target represented by a set of nodes.
 */
class any_node_job_target : public job_target {
public:
    // Default
    any_node_job_target() = default;

    /**
     * Constructor.
     *
     * @param nodes Nodes.
     */
    explicit any_node_job_target(std::set<cluster_node> &&nodes)
        : m_nodes(std::move(nodes)) {}

    /**
     * Get nodes.
     *
     * @return Nodes.
     */
    [[nodiscard]] const std::set<cluster_node> &get_nodes() const { return m_nodes; }

    [[nodiscard]] job_target_type get_type() const override { return job_target_type::ANY_NODE; }

private:
    /** Nodes. */
    std::set<cluster_node> m_nodes;
};

} // namespace ignite::detail
