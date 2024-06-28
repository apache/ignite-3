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
#include "ignite/client/compute/job_descriptor.h"
#include "ignite/client/compute/job_execution.h"
#include "ignite/client/compute/job_execution_options.h"
#include "ignite/client/detail/cluster_connection.h"
#include "ignite/client/detail/table/tables_impl.h"
#include "ignite/client/network/cluster_node.h"
#include "ignite/client/table/ignite_tuple.h"
#include "ignite/common/binary_object.h"
#include "ignite/common/ignite_result.h"
#include "ignite/common/primitive.h"

#include <memory>
#include <utility>

namespace ignite::detail {

/**
 * Ignite Compute implementation.
 */
class compute_impl : public std::enable_shared_from_this<compute_impl> {
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
     * Submits a compute job represented by the given class for an execution on one of the specified nodes
     * asynchronously. If the node leaves the cluster, it will be restarted on one of the candidate nodes.
     *
     * @param nodes Candidate node to use for the job execution.
     * @param descriptor Descriptor.
     * @param arg Job argument.
     * @param callback A callback called on operation completion with job execution result.
     */
    void submit_to_nodes(const std::vector<cluster_node> &nodes, std::shared_ptr<job_descriptor> descriptor,
        const binary_object &arg, ignite_callback<job_execution> callback);

    /**
     * Submits a compute job represented by the given class for an execution on one of the nodes where the given key is
     * located.
     *
     * @param table_name Name of the table to be used with @c key to determine target node.
     * @param key Table key to be used to determine the target node for job execution.
     * @param descriptor Descriptor.
     * @param arg Job argument.
     * @param callback A callback called on operation completion with job execution result.
     */
    void submit_colocated_async(const std::string &table_name, const ignite_tuple &key,
        std::shared_ptr<job_descriptor> descriptor, const binary_object &arg,
        ignite_callback<job_execution> callback);

    /**
     * Gets the job execution state. Can be @c nullopt if the job state no longer exists due to exceeding the
     * retention time limit.
     *
     * @param id Job ID.
     * @param callback Callback to be called when the operation is complete. Contains the job state. Can be @c nullopt
     *  if the job state no longer exists due to exceeding the retention time limit.
     */
    void get_state_async(uuid id, ignite_callback<std::optional<job_state>> callback);

    /**
     * Cancels the job execution.
     *
     * @param id Job ID.
     * @param callback Callback to be called when the operation is complete. Contains cancel result.
     */
    void cancel_async(uuid id, ignite_callback<job_execution::operation_result> callback);

    /**
     * Changes the job priority. After priority change the job will be the last in the queue of jobs with the same
     * priority.
     *
     * @param id Job ID.
     * @param priority New priority.
     * @param callback Callback to be called when the operation is complete. Contains operation result.
     */
    void change_priority_async(
        uuid id, std::int32_t priority, ignite_callback<job_execution::operation_result> callback);

private:
    /** Cluster connection. */
    std::shared_ptr<cluster_connection> m_connection;

    /** Tables. */
    std::shared_ptr<tables_impl> m_tables;
};

} // namespace ignite::detail
