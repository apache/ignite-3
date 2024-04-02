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

#include <cstdint>

namespace ignite {

/**
 * Job execution options.
 */
class job_execution_options {
public:
    /**
     * Default constructor.
     *
     * Default options:
     * priority = 0;
     * max_retries = 0;
     */
    job_execution_options() = default;

    /**
     * Constructor.
     *
     * @param priority Job execution priority.
     * @param max_retries Max number of times to retry job execution in case of failure, 0 to not retry.
     */
    explicit job_execution_options(std::int32_t priority, std::int32_t max_retries)
        : m_priority(priority)
        , m_max_retries(max_retries) {}

    /**
     * Gets the job execution priority.
     *
     * @return Job execution priority.
     */
    [[nodiscard]] std::int32_t get_priority() const { return m_priority; }

    /**
     * Gets the max number of times to retry job execution in case of failure, 0 to not retry.
     *
     * @return Max number of times to retry job execution in case of failure, 0 to not retry.
     */
    [[nodiscard]] std::int32_t get_max_retries() const { return m_max_retries; }

private:
    /** Job execution priority. */
    const std::int32_t m_priority{0};

    /** Max re-tries. */
    const std::int32_t m_max_retries{0};
};

} // namespace ignite
