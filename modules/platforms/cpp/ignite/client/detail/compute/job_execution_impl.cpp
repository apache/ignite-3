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

#include "ignite/client/detail/compute/job_execution_impl.h"
#include "ignite/client/detail/compute/compute_impl.h"

namespace ignite::detail {

void job_execution_impl::get_result_async(ignite_callback<std::optional<primitive>> callback) {
    std::unique_lock<std::mutex> guard(m_mutex);

    if (m_result) {
        auto copy{*m_result};
        guard.unlock();

        callback({std::move(copy)});
    } else if (m_error) {
        auto copy{*m_error};
        guard.unlock();

        callback({std::move(copy)});
    } else {
        if (m_result_callback)
            throw ignite_error("A callback for this result was already submitted");

        m_result_callback = std::make_shared<ignite_callback<std::optional<primitive>>>(std::move(callback));
    }
}

void job_execution_impl::set_result(std::optional<primitive> result) {
    std::unique_lock<std::mutex> guard(m_mutex);

    m_result = result;
    auto callback = std::move(m_result_callback);
    m_result_callback.reset();

    guard.unlock();

    if (callback) {
        (*callback)({std::move(result)});
    }
}

void job_execution_impl::get_status_async(ignite_callback<std::optional<job_status>> callback) {
    std::unique_lock<std::mutex> guard(m_mutex);

    if (m_final_status) {
        auto copy{m_final_status};
        guard.unlock();

        callback({std::move(copy)});
    } else {
        m_compute->get_status_async(m_id, std::move(callback));
    }
}

void job_execution_impl::set_final_status(const job_status &status) {
    std::lock_guard<std::mutex> guard(m_mutex);

    m_final_status = status;
}

void job_execution_impl::set_error(ignite_error error) {
    std::unique_lock<std::mutex> guard(m_mutex);

    m_error = error;
    auto callback = std::move(m_result_callback);
    m_result_callback.reset();

    guard.unlock();

    if (callback) {
        (*callback)({std::move(error)});
    }
}

void job_execution_impl::cancel_async(ignite_callback<job_execution::operation_result> callback) {
    bool status_set;
    {
        std::lock_guard<std::mutex> guard(m_mutex);
        status_set = m_final_status.has_value();
    }

    if (status_set) {
        callback(job_execution::operation_result::INVALID_STATE);

        return;
    }

    m_compute->cancel_async(m_id, std::move(callback));
}

void job_execution_impl::change_priority_async(
    std::int32_t priority, ignite_callback<job_execution::operation_result> callback) {
    bool status_set;
    {
        std::lock_guard<std::mutex> guard(m_mutex);
        status_set = m_final_status.has_value();
    }

    if (status_set) {
        callback(job_execution::operation_result::INVALID_STATE);

        return;
    }

    m_compute->change_priority_async(m_id, priority, std::move(callback));
}

} // namespace ignite::detail
