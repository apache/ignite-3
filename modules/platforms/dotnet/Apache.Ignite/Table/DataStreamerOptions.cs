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

using System;

/// <summary>
/// Data streamer options.
/// </summary>
/// <param name="BatchSize">Batch size - the number of entries that will be sent to the cluster in one network call.</param>
/// <param name="RetryLimit">Retry limit for a batch. If a batch fails to be sent to the cluster,
/// the streamer will retry it a number of times.</param>
/// <param name="PerNodeParallelOperations">Number of parallel operations per node
/// (how many in-flight requests can be active for a given node).</param>
/// <param name="AutoFlushFrequency">Auto flush frequency - the period of time after which the streamer
/// will flush the per-node buffer even if it is not full.</param>
public sealed record DataStreamerOptions(int BatchSize, int RetryLimit, int PerNodeParallelOperations, TimeSpan AutoFlushFrequency)
{
    /// <summary>
    /// Default streamer options.
    /// </summary>
    public static readonly DataStreamerOptions Default = new(1000, 16, 4, TimeSpan.FromSeconds(5));
}
