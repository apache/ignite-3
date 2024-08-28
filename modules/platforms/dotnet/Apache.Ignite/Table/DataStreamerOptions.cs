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
/// <param name="PageSize">The number of entries that will be sent to the cluster in one network call.</param>
/// <param name="RetryLimit">Retry limit for a batch. If a batch fails to be sent to the cluster,
/// the streamer will retry it a number of times.</param>
/// <param name="AutoFlushInterval">Auto flush interval - the period of time after which the streamer
/// will flush the per-node buffer even if it is not full.</param>
public sealed record DataStreamerOptions(int PageSize, int RetryLimit, TimeSpan AutoFlushInterval)
{
    /// <summary>
    /// Default streamer options.
    /// </summary>
    public static readonly DataStreamerOptions Default = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="DataStreamerOptions"/> class.
    /// </summary>
    public DataStreamerOptions()
        : this(
            PageSize: 1000,
            RetryLimit: 16,
            AutoFlushInterval: TimeSpan.FromSeconds(5))
    {
        // No-op.
    }
}
