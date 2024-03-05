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
#include "ignite/client/compute/job_status.h"
#include "ignite/common/config.h"
#include "ignite/common/ignite_result.h"
#include "ignite/common/primitive.h"
#include "ignite/common/uuid.h"

namespace ignite::detail {

/**
 * Job control object, provides information about the job execution process and result, allows cancelling the job.
 */
class job_execution_impl {
public:
    // Delete
    job_execution_impl() = delete;

    /**
     * Gets the job ID.
     *
     * @return Job ID.
     */
    [[nodiscard]] uuid get_id() const { return m_id; }

    /**
     * Gets the job execution status. Can be @c nullopt if the job status no longer exists due to exceeding the
     * retention time limit.
     *
     * @param callback Callback to be called when the operation is complete. Contains the job status. Can be @c nullopt
     *  if the job status no longer exists due to exceeding the retention time limit.
     */
    void get_status_async(ignite_callback<std::optional<job_status>> callback);

    /**
     * Gets the job execution result asynchronously.
     *
     * @param callback Callback to be called when the operation is complete. Contains the job execution result.
     */
    void get_result_async(ignite_callback<std::optional<primitive>> callback);

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
    uuid m_id;
};

} // namespace ignite::detail
