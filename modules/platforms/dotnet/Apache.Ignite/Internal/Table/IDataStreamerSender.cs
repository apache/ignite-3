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

namespace Apache.Ignite.Internal.Table;

using System.Threading.Tasks;
using Buffers;
using Proto;

/// <summary>
/// Data streamer sender.
/// </summary>
internal interface IDataStreamerSender
{
    /// <summary>
    /// Sends a batch to the cluster.
    /// </summary>
    /// <param name="buf">Buffer.</param>
    /// <param name="count">Buffer element count.</param>
    /// <param name="preferredNode">Preferred node.</param>
    /// <param name="retryPolicy">Retry policy.</param>
    /// <returns>Task.</returns>
    Task SendBatchAsync(PooledArrayBuffer buf, int count, PreferredNode preferredNode, IRetryPolicy retryPolicy);

    ValueTask<string?[]> GetPartitionAssignmentAsync();

    Task<Schema> GetSchemaAsync(int? schemaId);
}
