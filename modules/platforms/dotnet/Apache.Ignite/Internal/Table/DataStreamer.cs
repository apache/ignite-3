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

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Common;
using Ignite.Table;

/// <summary>
/// Data streamer.
/// </summary>
internal static class DataStreamer
{
    /// <summary>
    /// Streams the data.
    /// </summary>
    /// <param name="data">Data.</param>
    /// <param name="sender">Batch sender.</param>
    /// <param name="partitioner">Partitioner.</param>
    /// <param name="options">Options.</param>
    /// <typeparam name="T">Element type.</typeparam>
    /// <typeparam name="TPartition">Partition type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    internal static async Task StreamDataAsync<T, TPartition>(
        IAsyncEnumerable<T> data,
        Func<IList<T>, TPartition, Task> sender,
        Func<T, ValueTask<TPartition>> partitioner,
        DataStreamerOptions options)
        where TPartition : notnull
    {
        IgniteArgumentCheck.NotNull(data);

        IgniteArgumentCheck.Ensure(options.BatchSize > 0, $"{nameof(options.BatchSize)} should be positive.");
        IgniteArgumentCheck.Ensure(
            options.PerNodeParallelOperations > 0,
            $"{nameof(options.PerNodeParallelOperations)} should be positive.");

        // TODO: Validate options?
        var batches = new Dictionary<TPartition, Batch<T>>();

        await foreach (var item in data)
        {
            var partition = await partitioner(item).ConfigureAwait(false);

            if (!batches.TryGetValue(partition, out var batch))
            {
                batch = new Batch<T>
                {
                    // TODO: Pooled buffers.
                    Buf = new List<T>(options.BatchSize)
                };

                batches.Add(partition, batch);
            }

            batch.Buf.Add(item);

            if (batch.Buf.Count >= options.BatchSize)
            {
                // TODO: Allow adding items to another buffer while sending previous.
                await sender(batch.Buf, partition).ConfigureAwait(false);

                batch.Buf.Clear();
            }
        }

        foreach (var (partition, batch) in batches)
        {
            if (batch.Buf.Count > 0)
            {
                await sender(batch.Buf, partition).ConfigureAwait(false);
            }
        }
    }

    [SuppressMessage("Design", "CA1051:Do not declare visible instance fields", Justification = "Private class.")]
    [SuppressMessage("Design", "CA1002:Do not expose generic lists", Justification = "Private class.")]
    [SuppressMessage("StyleCop.CSharp.MaintainabilityRules", "SA1401:Fields should be private", Justification = "Private class.")]
    private class Batch<T>
    {
        public List<T> Buf = null!;
    }
}
