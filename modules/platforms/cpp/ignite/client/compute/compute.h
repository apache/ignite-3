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
#include "ignite/client/compute/job_execution.h"
#include "ignite/client/compute/job_execution_options.h"
#include "ignite/client/network/cluster_node.h"
#include "ignite/client/table/ignite_tuple.h"
#include "ignite/client/transaction/transaction.h"
#include "ignite/common/config.h"
#include "ignite/common/ignite_result.h"
#include "ignite/common/primitive.h"

#include <map>
#include <memory>
#include <set>
#include <utility>
#include <vector>

namespace ignite {

namespace detail {
class compute_impl;
}

/**
 * Ignite Compute facade.
 */
class compute {
    friend class ignite_client;

public:
    // Delete
    compute() = delete;

    /**
     * Submits for execution a compute job represented by the given class for an execution on one of the specified
     * nodes.
     *
     * @param nodes Nodes to use for the job execution.
     * @param units Deployment units. Can be empty.
     * @param job_class_name Java class name of the job to submit.
     * @param args Job arguments.
     * @param options Job execution options.
     * @param callback A callback called on operation completion with job execution result.
     */
    IGNITE_API void submit_async(const std::vector<cluster_node> &nodes, const std::vector<deployment_unit> &units,
        std::string_view job_class_name, const std::vector<primitive> &args, const job_execution_options &options,
        ignite_callback<job_execution> callback);

    /**
     * Submits for execution a compute job represented by the given class on one of the specified nodes.
     *
     * @param nodes Nodes to use for the job execution.
     * @param units Deployment units. Can be empty.
     * @param job_class_name Java class name of the job to submit.
     * @param args Job arguments.
     * @param options Job execution options.
     * @return Job execution result.
     */
    IGNITE_API job_execution submit(const std::vector<cluster_node> &nodes, const std::vector<deployment_unit> &units,
        std::string_view job_class_name, const std::vector<primitive> &args, const job_execution_options &options) {
        return sync<job_execution>([&](auto callback) mutable {
            submit_async(nodes, units, job_class_name, args, options, std::move(callback));
        });
    }

    /**
     * Broadcast a compute job represented by the given class for an execution on all of the specified nodes.
     *
     * @param nodes Nodes to use for the job execution.
     * @param units Deployment units. Can be empty.
     * @param job_class_name Java class name of the job to submit.
     * @param args Job arguments.
     * @param options Job execution options.
     * @param callback A callback called on operation completion with jobs execution result.
     */
    IGNITE_API void submit_broadcast_async(const std::set<cluster_node> &nodes,
        const std::vector<deployment_unit> &units, std::string_view job_class_name, const std::vector<primitive> &args,
        const job_execution_options &options,
        ignite_callback<std::map<cluster_node, ignite_result<job_execution>>> callback);

    /**
     * Broadcast a compute job represented by the given class on all of the specified nodes.
     *
     * @param nodes Nodes to use for the job execution.
     * @param units Deployment units. Can be empty.
     * @param job_class_name Java class name of the job to submit.
     * @param args Job arguments.
     * @param options Job execution options.
     * @return Job execution result.
     */
    IGNITE_API std::map<cluster_node, ignite_result<job_execution>> submit_broadcast(
        const std::set<cluster_node> &nodes, const std::vector<deployment_unit> &units, std::string_view job_class_name,
        const std::vector<primitive> &args, const job_execution_options &options) {
        return sync<std::map<cluster_node, ignite_result<job_execution>>>([&](auto callback) mutable {
            submit_broadcast_async(nodes, units, job_class_name, args, options, std::move(callback));
        });
    }

    /**
     * Asynchronously executes a job represented by the given class on one node where the given key is located.
     *
     * @param table_name Name of the table to be used with @c key to determine target node.
     * @param key Table key to be used to determine the target node for job execution.
     * @param units Deployment units. Can be empty.
     * @param job_class_name Java class name of the job to submit.
     * @param args Job arguments.
     * @param options Job execution options.
     * @param callback A callback called on operation completion with job execution result.
     */
    IGNITE_API void submit_colocated_async(std::string_view table_name, const ignite_tuple &key,
        const std::vector<deployment_unit> &units, std::string_view job_class_name, const std::vector<primitive> &args,
        const job_execution_options &options, ignite_callback<job_execution> callback);

    /**
     * Synchronously executes a job represented by the given class on one node where the given key is located.
     *
     * @param table_name Name of the table to be used with @c key to determine target node.
     * @param key Table key to be used to determine the target node for job execution.
     * @param units Deployment units. Can be empty.
     * @param job_class_name Java class name of the job to submit.
     * @param args Job arguments.
     * @param options Job execution options.
     * @return Job execution result.
     */
    IGNITE_API job_execution submit_colocated(std::string_view table_name, const ignite_tuple &key,
        const std::vector<deployment_unit> &units, std::string_view job_class_name, const std::vector<primitive> &args,
        const job_execution_options &options) {
        return sync<job_execution>([&](auto callback) mutable {
            submit_colocated_async(table_name, key, units, job_class_name, args, options, std::move(callback));
        });
    }

private:
    /**
     * Constructor
     *
     * @param impl Implementation
     */
    explicit compute(std::shared_ptr<detail::compute_impl> impl)
        : m_impl(std::move(impl)) {}

    /** Implementation. */
    std::shared_ptr<detail::compute_impl> m_impl;
};

} // namespace ignite
