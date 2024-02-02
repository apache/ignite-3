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

#include "ignite/client/compute/deployment_unit.h"
#include "ignite/client/detail/cluster_connection.h"
#include "ignite/client/detail/table/tables_impl.h"
#include "ignite/client/network/cluster_node.h"
#include "ignite/client/table/ignite_tuple.h"
#include "ignite/common/ignite_result.h"
#include "ignite/common/primitive.h"

#include <memory>
#include <utility>

namespace ignite::detail {

/**
 * Ignite Compute implementation.
 */
class compute_impl {
    friend class ignite_client;

public:
    /**
     * Constructor.
     *
     * @param connection Connection.
     */
    explicit compute_impl(std::shared_ptr<cluster_connection> connection, std::shared_ptr<tables_impl> tables)
        : m_connection(std::move(connection))
        , m_tables(std::move(tables)) {}

    /**
     * Executes a compute job represented by the given class on one of the specified node asynchronously. If the node leaves the cluster,
     * it will be restarted on one of the candidate nodes.
     *
     * @param nodes Candidate node to use for the job execution.
     * @param units Deployment units. Can be empty.
     * @param job_class_name Java class name of the job to execute.
     * @param args Job arguments.
     * @param callback A callback called on operation completion with job execution result.
     */
    void execute_on_nodes(const std::vector<cluster_node> &nodes, const std::vector<deployment_unit> &units,
        std::string_view job_class_name, const std::vector<primitive> &args,
        ignite_callback<std::optional<primitive>> callback);

    /**
     * Asynchronously executes a job represented by the given class on one node where the given key is located.
     *
     * @param table_name Name of the table to be used with @c key to determine target node.
     * @param key Table key to be used to determine the target node for job execution.
     * @param units Deployment units. Can be empty.
     * @param job_class_name Java class name of the job to execute.
     * @param args Job arguments.
     * @param callback A callback called on operation completion with job execution result.
     */
    void execute_colocated_async(const std::string &table_name, const ignite_tuple &key,
        const std::vector<deployment_unit> &units, const std::string &job_class_name,
        const std::vector<primitive> &args, ignite_callback<std::optional<primitive>> callback);

private:
    /** Cluster connection. */
    std::shared_ptr<cluster_connection> m_connection;

    /** Tables. */
    std::shared_ptr<tables_impl> m_tables;
};

} // namespace ignite::detail
