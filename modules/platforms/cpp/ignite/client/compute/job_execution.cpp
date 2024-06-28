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

#include "ignite/client/compute/job_execution.h"
#include "ignite/client/detail/compute/job_execution_impl.h"

namespace ignite {

uuid job_execution::get_id() const {
    return m_impl->get_id();
}

void job_execution::get_state_async(ignite_callback<std::optional<job_state>> callback) {
    m_impl->get_state_async(std::move(callback));
}

void job_execution::get_result_async(ignite_callback<std::optional<primitive>> callback) {
    m_impl->get_result_async(std::move(callback));
}

void job_execution::cancel_async(ignite_callback<job_execution::operation_result> callback) {
    m_impl->cancel_async(std::move(callback));
}

void job_execution::change_priority_async(
    std::int32_t priority, ignite_callback<job_execution::operation_result> callback) {
    m_impl->change_priority_async(priority, std::move(callback));
}

} // namespace ignite
