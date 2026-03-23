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

#include "ignite/common/detail/config.h"
#include "ignite/client/network/cluster_node.h"

#include <set>
#include <vector>
#include <memory>

namespace ignite {

/**
 * @brief Compute task target.
 *
 * Encapsulates which nodes will be used as targets for compute task.
 */
class broadcast_job_target {
public:
    // Default
    virtual ~broadcast_job_target() = default;

    /**
     * Create a single node job target.
     *
     * @param val Node.
     */
    [[nodiscard]] IGNITE_API static std::shared_ptr<broadcast_job_target> node(cluster_node val);

    /**
     * Create a multiple node job target.
     *
     * @param vals Nodes.
     */
    [[nodiscard]] IGNITE_API static std::shared_ptr<broadcast_job_target> nodes(std::set<cluster_node> vals);

    /**
     * Create a multiple node job target.
     *
     * @param vals Nodes.
     */
    [[nodiscard]] IGNITE_API static std::shared_ptr<broadcast_job_target> nodes(const std::vector<cluster_node> &vals);

protected:
    // Default
    broadcast_job_target() = default;
};


} // namespace ignite
