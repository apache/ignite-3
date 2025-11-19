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
#include "ignite/client/table/qualified_name.h"
#include "ignite/client/detail/compute/job_target_type.h"

#include <set>
#include <vector>
#include <memory>

namespace ignite {
class ignite_tuple;
class compute;
class job_target;

/**
 * @brief Information about what nodes may be used for job execution.
 */
class job_target {
    friend class compute;
public:
    // Default
    virtual ~job_target() = default;

    /**
     * Create a single node job target.
     *
     * @param node Node.
     */
    [[nodiscard]] IGNITE_API static std::shared_ptr<job_target> node(cluster_node node);

    /**
     * Create a multiple node job target.
     *
     * @param nodes Nodes.
     */
    [[nodiscard]] IGNITE_API static std::shared_ptr<job_target> any_node(std::set<cluster_node> nodes);

    /**
     * Create a multiple node job target.
     *
     * @param nodes Nodes.
     */
    [[nodiscard]] IGNITE_API static std::shared_ptr<job_target> any_node(const std::vector<cluster_node> &nodes);

    /**
     * Creates a colocated job target for a specific table and key.
     *
     * @param table_name Table name.
     * @param key Key.
     */
    [[nodiscard]] IGNITE_API static std::shared_ptr<job_target> colocated(std::string_view table_name, const ignite_tuple &key);

    /**
     * Creates a colocated job target for a specific table and key.
     *
     * @param table_name Table name.
     * @param key Key.
     */
    [[nodiscard]] IGNITE_API static std::shared_ptr<job_target> colocated(qualified_name table_name, const ignite_tuple &key);

protected:
    // Default
    job_target() = default;

    /**
     * Get the job type.
     *
     * @return Job type.
     */
    [[nodiscard]] virtual detail::job_target_type get_type() const = 0;
};


} // namespace ignite
