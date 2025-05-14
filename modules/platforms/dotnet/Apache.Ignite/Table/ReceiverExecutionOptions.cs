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

namespace Apache.Ignite.Table;

using Compute;

/// <summary>
/// Data streamer receiver execution options.
/// </summary>
/// <param name="Priority">Execution priority.</param>
/// <param name="MaxRetries">Number of times to retry receiver execution in case of failure, 0 to not retry.</param>
/// <param name="ExecutorType">>Executor type.</param>
public sealed record ReceiverExecutionOptions(
    int Priority = 0,
    int MaxRetries = 0,
    JobExecutorType ExecutorType = JobExecutorType.JavaEmbedded)
{
    /// <summary>
    /// Default job execution options.
    /// </summary>
    public static readonly ReceiverExecutionOptions Default = new();
}
