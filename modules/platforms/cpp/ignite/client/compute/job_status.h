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

namespace ignite {

/**
 * Compute job state.
 */
enum class job_status {
    /// The job is submitted and waiting for an execution start.
    QUEUED,

    /// The job is being executed.
    EXECUTING,

    /// The job was unexpectedly terminated during execution.
    FAILED,

    /// The job was executed successfully and the execution result was returned.
    COMPLETED,

    /// The job has received the cancel command, but is still running.
    CANCELING,

    /// The job was successfully cancelled.
    CANCELED
};

} // namespace ignite
