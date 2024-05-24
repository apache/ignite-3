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
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Common;
using Ignite.Table;
using Serialization;

/// <summary>
/// Data streamer 2.
/// </summary>
internal static class DataStreamer2
{
    /// <summary>
    /// Streams the data.
    /// </summary>
    /// <param name="data">Data.</param>
    /// <param name="provider">Provider.</param>
    /// <param name="table">Table.</param>
    /// <param name="options">Options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <typeparam name="T">Element type.</typeparam>
    /// <typeparam name="TBatch">Batch type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    internal static async Task StreamDataAsync<T, TBatch>(
        IAsyncEnumerable<T> data,
        IDataStreamerProvider<T, TBatch> provider,
        Table table,
        DataStreamerOptions options,
        CancellationToken cancellationToken)
        where TBatch : IDataStreamerBatch
    {
        IgniteArgumentCheck.NotNull(data);

        IgniteArgumentCheck.Ensure(
            options.PageSize > 0,
            nameof(options.PageSize),
            $"{nameof(options.PageSize)} should be positive.");

        IgniteArgumentCheck.Ensure(
            options.AutoFlushFrequency > TimeSpan.Zero,
            nameof(options.AutoFlushFrequency),
            $"{nameof(options.AutoFlushFrequency)} should be positive.");

        IgniteArgumentCheck.Ensure(
            options.RetryLimit >= 0,
            nameof(options.RetryLimit),
            $"{nameof(options.RetryLimit)} should be non-negative.");

        // ConcurrentDictionary is not necessary because we consume the source sequentially.
        // However, locking for batches is required due to auto-flush background task.
        var batches = new Dictionary<int, TBatch>();
        var retryPolicy = new RetryLimitPolicy { RetryLimit = options.RetryLimit };

        var schema = await table.GetSchemaAsync(null).ConfigureAwait(false);

        var partitionAssignment = await table.GetPartitionAssignmentAsync().ConfigureAwait(false);
        var partitionCount = partitionAssignment.Length; // Can't be changed.
        Debug.Assert(partitionCount > 0, "partitionCount > 0");

        using var flushCts = new CancellationTokenSource();

        try
        {
            _ = AutoFlushAsync(flushCts.Token);

            await foreach (var item in data.WithCancellation(cancellationToken))
            {
                // WithCancellation passes the token to the producer.
                // However, not all producers support cancellation, so we need to check it here as well.
                cancellationToken.ThrowIfCancellationRequested();

                var newAssignment = await table.GetPartitionAssignmentAsync().ConfigureAwait(false);
                if (newAssignment != partitionAssignment)
                {
                    // Drain all batches to preserve order when partition assignment changes.
                    await Drain().ConfigureAwait(false);
                    partitionAssignment = newAssignment;
                }

                var batch = await AddWithRetryUnmapped(item).ConfigureAwait(false);
                if (batch.Count >= options.PageSize)
                {
                    await SendAsync(batch).ConfigureAwait(false);
                }
            }

            await Drain().ConfigureAwait(false);
        }
        finally
        {
            flushCts.Cancel();
            foreach (var batch in batches.Values)
            {
                batch.Release();

                Metrics.StreamerItemsQueuedDecrement(batch.Count);
                Metrics.StreamerBatchesActiveDecrement();
            }
        }

        return;

        async ValueTask<TBatch> AddWithRetryUnmapped(T item)
        {
            try
            {
                return provider.Add(item, schema, partitionCount);
            }
            catch (Exception e) when (e.CausedByUnmappedColumns())
            {
                schema = await table.GetSchemaAsync(Table.SchemaVersionForceLatest).ConfigureAwait(false);
                return await AddWithRetryUnmapped(item).ConfigureAwait(false);
            }
        }

        async Task SendAsync(TBatch batch)
        {
            await provider.FlushAsync(batch, retryPolicy, schema).ConfigureAwait(false);

            Metrics.StreamerBatchesActiveIncrement();
        }

        async Task AutoFlushAsync(CancellationToken flushCt)
        {
            while (!flushCt.IsCancellationRequested)
            {
                await Task.Delay(options.AutoFlushFrequency, flushCt).ConfigureAwait(false);
                var ts = Stopwatch.GetTimestamp();

                foreach (var batch in batches.Values)
                {
                    if (batch.Count > 0 && ts - batch.LastFlush > options.AutoFlushFrequency.Ticks)
                    {
                        await SendAsync(batch).ConfigureAwait(false);
                    }
                }
            }
        }

        async Task Drain()
        {
            foreach (var batch in batches.Values)
            {
                if (batch.Count > 0)
                {
                    await SendAsync(batch).ConfigureAwait(false);
                }

                await batch.FinishActiveTasks().ConfigureAwait(false);
            }
        }
    }
}
