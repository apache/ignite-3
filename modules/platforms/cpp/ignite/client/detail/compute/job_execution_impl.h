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

#include "ignite/client/compute/job_execution.h"
#include "ignite/client/compute/job_state.h"
#include "ignite/client/detail/cluster_connection.h"
#include "ignite/common/detail/config.h"
#include "ignite/common/ignite_result.h"
#include "ignite/common/primitive.h"
#include "ignite/common/uuid.h"

namespace ignite::detail {

class compute_impl;

/**
 * Job control object, provides information about the job execution process and result, allows cancelling the job.
 */
class job_execution_impl {
public:
    // Default
    job_execution_impl() = default;

    /**
     * Constructor
     *
     * @param id Job ID.
     * @param compute Compute.
     */
    explicit job_execution_impl(uuid id, std::shared_ptr<compute_impl> &&compute)
        : m_id(id)
        , m_compute(compute) {}

    /**
     * Gets the job ID.
     *
     * @return Job ID.
     */
    [[nodiscard]] uuid get_id() const { return m_id; }

    /**
     * Gets the job execution result asynchronously.
     *
     * Only one callback can be submitted for this operation at a time, which means you can not call this method in
     * parallel.
     * @param callback Callback to be called when the operation is complete. Called with the job execution result.
     */
    void get_result_async(ignite_callback<std::optional<primitive>> callback);

    /**
     * Set result.
     *
     * @param result Result.
     */
    void set_result(std::optional<primitive> result);

    /**
     * Gets the job execution state. Can be @c nullopt if the job state no longer exists due to exceeding the
     * retention time limit.
     *
     * @param callback Callback to be called when the operation is complete. Contains the job state. Can be @c nullopt
     *  if the job state no longer exists due to exceeding the retention time limit.
     */
    void get_state_async(ignite_callback<std::optional<job_state>> callback);

    /**
     * Set final state.
     *
     * @param state Execution state.
     */
    void set_final_state(const job_state &state);

    /**
     * Set error.
     *
     * @param error Error.
     */
    void set_error(ignite_error error);

    /**
     * Cancels the job execution.
     *
     * @param callback Callback to be called when the operation is complete. Contains cancel result.
     */
    void cancel_async(ignite_callback<job_execution::operation_result> callback);

    /**
     * Changes the job priority. After priority change the job will be the last in the queue of jobs with the same
     * priority.
     *
     * @param priority New priority.
     * @param callback Callback to be called when the operation is complete. Contains operation result.
     */
    void change_priority_async(std::int32_t priority, ignite_callback<job_execution::operation_result> callback);

private:
    /** Job ID. */
    const uuid m_id;

    /** Compute. */
    std::shared_ptr<compute_impl> m_compute;

    /** Mutex. Should be held to change any data. */
    std::mutex m_mutex;

    /** Final state. */
    std::optional<job_state> m_final_state;

    /** Execution result. First optional to understand if the result is available. */
    std::optional<std::optional<primitive>> m_result;

    /** Error. */
    std::optional<ignite_error> m_error;

    /** Result callback. */
    std::shared_ptr<ignite_callback<std::optional<primitive>>> m_result_callback;
};

} // namespace ignite::detail
