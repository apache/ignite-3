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

#include "ignite/client/compute/broadcast_execution.h"
#include "ignite/client/compute/broadcast_job_target.h"
#include "ignite/client/compute/job_descriptor.h"
#include "ignite/client/compute/job_execution.h"
#include "ignite/client/compute/job_target.h"
#include "ignite/client/network/cluster_node.h"
#include "ignite/common/binary_object.h"
#include "ignite/common/detail/config.h"
#include "ignite/common/ignite_result.h"

#include <memory>
#include <utility>


namespace ignite {

namespace detail {
class compute_impl;
}

/**
 * @brief Ignite Compute API facade.
 */
class compute {
    friend class ignite_client;

public:
    // Delete
    compute() = delete;

    /**
     * Submits for execution a compute job represented by the given class for an execution on the specified target.
     *
     * @param target Job target.
     * @param descriptor Descriptor.
     * @param arg Job argument.
     * @param callback A callback called on operation completion with job execution result.
     */
    IGNITE_API void submit_async(std::shared_ptr<job_target> target, std::shared_ptr<job_descriptor> descriptor,
        const binary_object &arg, ignite_callback<job_execution> callback);

    /**
     * Submits for execution a compute job represented by the given class on the specified target.
     *
     * @param target Job target.
     * @param descriptor Descriptor.
     * @param arg Job argument.
     * @return Job execution result.
     */
    IGNITE_API job_execution submit(std::shared_ptr<job_target> target, std::shared_ptr<job_descriptor> descriptor,
        const binary_object &arg) {
        return sync<job_execution>([&](auto callback) mutable {
            submit_async(std::move(target), std::move(descriptor), arg, std::move(callback));
        });
    }

    /**
     * Broadcast a compute job represented by the given class for an execution on all the specified nodes.
     *
     * @param target Job target.
     * @param descriptor Descriptor.
     * @param arg Job argument.
     * @param callback A callback called on operation completion with jobs execution result.
     */
    IGNITE_API void submit_broadcast_async(std::shared_ptr<broadcast_job_target> target,
        std::shared_ptr<job_descriptor> descriptor, const binary_object &arg,
        ignite_callback<broadcast_execution> callback);

    /**
     * Broadcast a compute job represented by the given class on all the specified nodes.
     *
     * @param target Job target.
     * @param descriptor Descriptor.
     * @param arg Job argument.
     * @return Job execution result.
     */
    IGNITE_API broadcast_execution submit_broadcast(
        std::shared_ptr<broadcast_job_target> target, std::shared_ptr<job_descriptor> descriptor,
        const binary_object &arg) {
        return sync<broadcast_execution>([&](auto callback) mutable {
            submit_broadcast_async(std::move(target), std::move(descriptor), arg, std::move(callback));
        });
    }

private:
    /**
     * Constructor.
     *
     * @param impl Implementation.
     */
    explicit compute(std::shared_ptr<detail::compute_impl> impl)
        : m_impl(std::move(impl)) {}

    /** Implementation. */
    std::shared_ptr<detail::compute_impl> m_impl;
};

} // namespace ignite
