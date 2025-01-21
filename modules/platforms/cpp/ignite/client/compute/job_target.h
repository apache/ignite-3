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

#include "ignite/client/network/cluster_node.h"

#include <set>

namespace ignite {
class ignite_tuple;

/**
 * Broadcast execution control object, provides information about the broadcast execution process and result.
 */
class job_target {
public:
    // Default
    job_target() = default;
    ~job_target() = default;

    /**
     * Create a single node job target.
     *
     * @param node Node.
     */
    [[nodiscard]] static std::shared_ptr<job_target> node(cluster_node node);

    /**
     * Create a multiple node job target.
     *
     * @param nodes Nodes.
     */
    [[nodiscard]] static std::shared_ptr<job_target> any_node(std::set<cluster_node> nodes);

    /**
     * Creates a colocated job target for a specific table and key.
     *
     * @param table_name Table name.
     * @param key Key.
     */
    [[nodiscard]] static std::shared_ptr<job_target> colocated(std::string_view table_name, const ignite_tuple &key);
};

} // namespace ignite
