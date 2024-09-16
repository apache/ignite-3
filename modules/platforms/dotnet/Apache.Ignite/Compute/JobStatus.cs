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

namespace Apache.Ignite.Compute;

/// <summary>
/// Compute job status.
/// </summary>
public enum JobStatus
{
    /// <summary>
    /// The job is submitted and waiting for an execution start.
    /// </summary>
    Queued,

    /// <summary>
    /// The job is being executed.
    /// </summary>
    Executing,

    /// <summary>
    /// The job was unexpectedly terminated during execution.
    /// </summary>
    Failed,

    /// <summary>
    /// The job was executed successfully and the execution result was returned.
    /// </summary>
    Completed,

    /// <summary>
    /// The job has received the cancel command, but is still running.
    /// </summary>
    Canceling,

    /// <summary>
    /// The job was successfully cancelled.
    /// </summary>
    Canceled
}
