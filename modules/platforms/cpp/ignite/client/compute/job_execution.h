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

#include "ignite/client/compute/job_state.h"
#include "ignite/common/detail/config.h"
#include "ignite/common/ignite_result.h"
#include "ignite/common/binary_object.h"
#include "ignite/common/uuid.h"

#include <memory>

namespace ignite {

namespace detail {
class job_execution_impl;
}

/**
 * Job control object, provides information about the job execution process and result, allows cancelling the job.
 */
class job_execution {
public:
    /**
     * Job operation result.
     */
    enum class operation_result {
        /// The job operation was successfully complete.
        SUCCESS,

        /// The job has already finished.
        INVALID_STATE,

        /// The job was not found (no longer exists due to exceeding the retention time limit).
        NOT_FOUND
    };

    // Default
    job_execution() = default;

    /**
     * Constructor.
     *
     * @param impl Implementation.
     */
    explicit job_execution(std::shared_ptr<detail::job_execution_impl> impl)
        : m_impl(std::move(impl)) {}

    /**
     * Gets the job ID.
     *
     * @return Job ID.
     */
    [[nodiscard]] uuid get_id() const;

    /**
     * Gets the job execution state asynchronously. Can be @c nullopt if the job state no longer exists due to
     * exceeding the retention time limit.
     *
     * @param callback Callback to be called when the operation is complete. Called with the job state.
     *  Can be @c nullopt if the job state no longer exists due to exceeding the retention time limit.
     */
    IGNITE_API void get_state_async(ignite_callback<std::optional<job_state>> callback);

    /**
     * Gets the job execution state. Can be @c nullopt if the job state no longer exists due to exceeding the
     * retention time limit.
     *
     * @return The job state. Can be @c nullopt if the job state no longer exists due to exceeding the retention
     *  time limit.
     */
    IGNITE_API std::optional<job_state> get_state() {
        return sync<std::optional<job_state>>(
            [this](auto callback) mutable { get_state_async(std::move(callback)); });
    }

    /**
     * Gets the job execution result asynchronously.
     *
     * Only one callback can be submitted for this operation at a time, which means you can not call this method in
     * parallel.
     * @param callback Callback to be called when the operation is complete. Called with the job execution result.
     */
    IGNITE_API void get_result_async(ignite_callback<std::optional<binary_object>> callback);

    /**
     * Gets the job execution result.
     *
     * Only one thread can wait for result at a time, which means you can not call this method in parallel from
     * multiple threads.
     * @return The job execution result.
     */
    IGNITE_API std::optional<binary_object> get_result() {
        return sync<std::optional<binary_object>>(
            [this](auto callback) mutable { get_result_async(std::move(callback)); });
    }

    /**
     * Cancels the job execution asynchronously.
     *
     * @param callback Callback to be called when the operation is complete. Called with the cancel result.
     */
    IGNITE_API void cancel_async(ignite_callback<operation_result> callback);

    /**
     * Cancels the job execution.
     *
     * @param return Result of the cancel operation.
     */
    IGNITE_API operation_result cancel() {
        return sync<operation_result>([this](auto callback) mutable { cancel_async(std::move(callback)); });
    }

    /**
     * Changes the job priority asynchronously. After priority change the job will be the last in the queue of jobs
     * with the same priority.
     *
     * @param priority New priority.
     * @param callback Callback to be called when the operation is complete. Called with the operation result.
     */
    IGNITE_API void change_priority_async(std::int32_t priority, ignite_callback<operation_result> callback);

    /**
     * Changes the job priority. After priority change the job will be the last in the queue of jobs with the same
     * priority.
     *
     * @param priority New priority.
     * @param return Result of the operation.
     */
    IGNITE_API operation_result change_priority(std::int32_t priority) {
        return sync<operation_result>(
            [this, priority](auto callback) mutable { change_priority_async(priority, std::move(callback)); });
    }

private:
    /** Implementation. */
    std::shared_ptr<detail::job_execution_impl> m_impl{};
};

} // namespace ignite
