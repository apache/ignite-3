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
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Buffers;
using Common;
using Ignite.Table;
using Proto;
using Proto.BinaryTuple;
using Proto.MsgPack;
using Serialization;

/// <summary>
/// Data streamer.
/// <para />
/// Implementation notes:
/// * Hashing is combined with serialization (unlike Java client), so we write binary tuples to a per-node buffer right away.
///   - Pros: cheaper happy path;
///   - Cons: will require re-serialization on schema update.
/// * Iteration and serialization are sequential.
/// * Batches are sent asynchronously; we wait for the previous batch only when the next batch for the given node is full.
/// * The more connections to different servers we have - the more parallelism we get (see benchmark).
/// * There is no parallelism for the same node, because we need to guarantee ordering.
/// </summary>
internal static class DataStreamer
{
    private static readonly TimeSpan PartitionAssignmentUpdateFrequency = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Streams the data.
    /// </summary>
    /// <param name="data">Data.</param>
    /// <param name="table">Table.</param>
    /// <param name="writer">Item writer.</param>
    /// <param name="options">Options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <typeparam name="T">Element type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    internal static async Task StreamDataAsync<T>(
        IAsyncEnumerable<T> data,
        Table table,
        RecordSerializer<T> writer,
        DataStreamerOptions options,
        CancellationToken cancellationToken)
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
        var batches = new Dictionary<int, Batch<T>>();
        var retryPolicy = new RetryLimitPolicy { RetryLimit = options.RetryLimit };

        var schema = await table.GetSchemaAsync(null).ConfigureAwait(false);

        var partitionAssignment = await table.GetPartitionAssignmentAsync().ConfigureAwait(false);
        var partitionCount = partitionAssignment.Length; // Can't be changed.
        Debug.Assert(partitionCount > 0, "partitionCount > 0");
        var lastPartitionsAssignmentCheck = Stopwatch.StartNew();

        using var flushCts = new CancellationTokenSource();

        try
        {
            _ = AutoFlushAsync(flushCts.Token);

            await foreach (var item in data.WithCancellation(cancellationToken))
            {
                // WithCancellation passes the token to the producer.
                // However, not all producers support cancellation, so we need to check it here as well.
                cancellationToken.ThrowIfCancellationRequested();

                var (batch, partition) = await AddWithRetryUnmapped(item).ConfigureAwait(false);
                if (batch.Count >= options.PageSize)
                {
                    await SendAsync(batch, partition).ConfigureAwait(false);
                }

                if (lastPartitionsAssignmentCheck.Elapsed > PartitionAssignmentUpdateFrequency)
                {
                    var newAssignment = await table.GetPartitionAssignmentAsync().ConfigureAwait(false);

                    if (newAssignment != partitionAssignment)
                    {
                        // Drain all batches to preserve order when partition assignment changes.
                        await Drain().ConfigureAwait(false);
                        partitionAssignment = newAssignment;
                    }

                    lastPartitionsAssignmentCheck.Restart();
                }
            }

            await Drain().ConfigureAwait(false);
        }
        finally
        {
            flushCts.Cancel();
            foreach (var batch in batches.Values)
            {
                batch.Buffer.Dispose();
                ArrayPool<T>.Shared.Return(batch.Items);

                Metrics.StreamerItemsQueuedDecrement(batch.Count);
                Metrics.StreamerBatchesActiveDecrement();
            }
        }

        return;

        async ValueTask<(Batch<T> Batch, int Partition)> AddWithRetryUnmapped(T item)
        {
            try
            {
                return Add(item);
            }
            catch (Exception e) when (e.CausedByUnmappedColumns())
            {
                schema = await table.GetSchemaAsync(Table.SchemaVersionForceLatest).ConfigureAwait(false);
                return Add(item);
            }
        }

        (Batch<T> Batch, int Partition) Add(T item)
        {
            var schema0 = schema;
            var tupleBuilder = new BinaryTupleBuilder(schema0.Columns.Length, hashedColumnsPredicate: schema0.HashedColumnIndexProvider);

            try
            {
                return Add0(item, ref tupleBuilder, schema0);
            }
            finally
            {
                tupleBuilder.Dispose();
            }
        }

        (Batch<T> Batch, int Partition) Add0(T item, ref BinaryTupleBuilder tupleBuilder, Schema schema0)
        {
            var columnCount = schema0.Columns.Length;

            // Use MemoryMarshal to work around [CS8352]: "Cannot use variable 'noValueSet' in this context
            // because it may expose referenced variables outside of their declaration scope".
            Span<byte> noValueSet = stackalloc byte[columnCount / 8 + 1];
            Span<byte> noValueSetRef = MemoryMarshal.CreateSpan(ref MemoryMarshal.GetReference(noValueSet), columnCount);

            writer.Handler.Write(ref tupleBuilder, item, schema0, keyOnly: false, noValueSetRef);

            var partitionId = Math.Abs(tupleBuilder.GetHash() % partitionCount);
            var batch = GetOrCreateBatch(partitionId);

            lock (batch)
            {
                batch.Items[batch.Count++] = item;

                if (batch.Schema != schema0)
                {
                    batch.SchemaOutdated = true;
                }

                // 1. To compute target partition, we need key hash.
                // 2. To compute key hash, we need to serialize the key.
                // 3. Since we already serialized the key, we can use it for the message body and avoid re-serialization.
                // However, if schema gets updated, we need to re-serialize the whole batch.
                // Schema update is rare, so we optimize for the happy path.
                if (!batch.SchemaOutdated)
                {
                    noValueSet.CopyTo(batch.Buffer.MessageWriter.WriteBitSet(columnCount));
                    batch.Buffer.MessageWriter.Write(tupleBuilder.Build().Span);
                }
            }

            Metrics.StreamerItemsQueuedIncrement();

            return (batch, partitionId);
        }

        Batch<T> GetOrCreateBatch(int partitionId)
        {
            ref var batchRef = ref CollectionsMarshal.GetValueRefOrAddDefault(batches, partitionId, out _);

            if (batchRef == null)
            {
                batchRef = new Batch<T>(options.PageSize, schema);
                InitBuffer(batchRef, partitionId, schema);

                Metrics.StreamerBatchesActiveIncrement();
            }

            return batchRef;
        }

        async Task SendAsync(Batch<T> batch, int partitionId)
        {
            var expectedSize = batch.Count;

            // Wait for the previous task for this batch to finish: preserve order, backpressure control.
            await batch.Task.ConfigureAwait(false);

            lock (batch)
            {
                if (batch.Count != expectedSize || batch.Count == 0)
                {
                    // Concurrent update happened.
                    return;
                }

                var buf = batch.Buffer;

                // See RecordSerializer.WriteMultiple.
                buf.WriteByte(MsgPackCode.Int32, batch.CountPos);
                buf.WriteIntBigEndian(batch.Count, batch.CountPos + 1);

                batch.Task = SendAndDisposeBufAsync(buf, partitionId, batch.Task, batch.Items, batch.Count, batch.SchemaOutdated);

                batch.Items = ArrayPool<T>.Shared.Rent(options.PageSize);
                batch.Count = 0;
                batch.Buffer = ProtoCommon.GetMessageWriter(); // Prev buf will be disposed in SendAndDisposeBufAsync.
                InitBuffer(batch, partitionId, schema);
                batch.LastFlush = Stopwatch.GetTimestamp();
                batch.Schema = schema;
                batch.SchemaOutdated = false;

                Metrics.StreamerBatchesActiveIncrement();
            }
        }

        async Task SendAndDisposeBufAsync(
            PooledArrayBuffer buf,
            int partitionId,
            Task oldTask,
            T[] items,
            int count,
            bool batchSchemaOutdated)
        {
            Debug.Assert(items.Length > 0, "items.Length > 0");

            if (batchSchemaOutdated)
            {
                // Schema update was detected while the batch was being filled.
                // Re-serialize the whole batch.
                ReWriteBatch(buf, partitionId, schema, items.AsSpan(0, count), writer);
            }

            // ReSharper disable once AccessToModifiedClosure
            var preferredNode = PreferredNode.FromName(partitionAssignment[partitionId] ?? string.Empty);

            try
            {
                int? schemaVersion = null;
                while (true)
                {
                    try
                    {
                        if (schemaVersion != null)
                        {
                            // Might be updated by another batch.
                            if (schema.Version != schemaVersion)
                            {
                                schema = await table.GetSchemaAsync(schemaVersion).ConfigureAwait(false);
                            }

                            // Serialize again with the new schema.
                            ReWriteBatch(buf, partitionId, schema, items.AsSpan(0, count), writer);
                        }

                        // Wait for the previous batch for this node to preserve item order.
                        await oldTask.ConfigureAwait(false);
                        await SendBatchAsync(table, buf, count, preferredNode, retryPolicy).ConfigureAwait(false);

                        return;
                    }
                    catch (IgniteException e) when (e.Code == ErrorGroups.Table.SchemaVersionMismatch &&
                                                    schemaVersion != e.GetExpectedSchemaVersion())
                    {
                        // Schema update detected after the batch was serialized.
                        schemaVersion = e.GetExpectedSchemaVersion();
                    }
                    catch (Exception e) when (e.CausedByUnmappedColumns() && schemaVersion == null)
                    {
                        schemaVersion = Table.SchemaVersionForceLatest;
                    }
                }
            }
            finally
            {
                buf.Dispose();
                ArrayPool<T>.Shared.Return(items);

                Metrics.StreamerItemsQueuedDecrement(count);
                Metrics.StreamerBatchesActiveDecrement();
            }
        }

        async Task AutoFlushAsync(CancellationToken flushCt)
        {
            while (!flushCt.IsCancellationRequested)
            {
                await Task.Delay(options.AutoFlushFrequency, flushCt).ConfigureAwait(false);
                var ts = Stopwatch.GetTimestamp();

                foreach (var (partition, batch) in batches)
                {
                    if (batch.Count > 0 && ts - batch.LastFlush > options.AutoFlushFrequency.Ticks)
                    {
                        await SendAsync(batch, partition).ConfigureAwait(false);
                    }
                }
            }
        }

        async Task Drain()
        {
            foreach (var (partition, batch) in batches)
            {
                if (batch.Count > 0)
                {
                    await SendAsync(batch, partition).ConfigureAwait(false);
                }

                await batch.Task.ConfigureAwait(false);
            }
        }
    }

    private static void InitBuffer<T>(Batch<T> batch, int partitionId, Schema schema)
    {
        var buf = batch.Buffer;
        WriteBatchHeader(buf, partitionId, schema);

        batch.CountPos = buf.Position;
        buf.Advance(5); // Reserve count.
    }

    private static void WriteBatchHeader(PooledArrayBuffer buf, int partitionId, Schema schema)
    {
        var w = buf.MessageWriter;
        w.Write(schema.TableId);
        w.Write(partitionId);
        w.WriteNil(); // Deleted rows bit set.
        w.Write(schema.Version);
    }

    private static void ReWriteBatch<T>(
        PooledArrayBuffer buf,
        int partitionId,
        Schema schema,
        ReadOnlySpan<T> items,
        RecordSerializer<T> writer)
    {
        buf.Reset();
        WriteBatchHeader(buf, partitionId, schema);

        var w = buf.MessageWriter;
        w.Write(items.Length);

        foreach (var item in items)
        {
            writer.Handler.Write(ref w, schema, item, keyOnly: false, computeHash: false);
        }
    }

    private static async Task SendBatchAsync(
        Table table,
        PooledArrayBuffer buf,
        int count,
        PreferredNode preferredNode,
        IRetryPolicy retryPolicy)
    {
        var (resBuf, socket) = await table.Socket.DoOutInOpAndGetSocketAsync(
                ClientOp.StreamerBatchSend,
                tx: null,
                buf,
                preferredNode,
                retryPolicy)
            .ConfigureAwait(false);

        resBuf.Dispose();

        Metrics.StreamerBatchesSent.Add(1, socket.MetricsContext.Tags);
        Metrics.StreamerItemsSent.Add(count, socket.MetricsContext.Tags);
    }

    private sealed record Batch<T>
    {
        public Batch(int capacity, Schema schema)
        {
            Items = ArrayPool<T>.Shared.Rent(capacity);
            Schema = schema;
        }

        public PooledArrayBuffer Buffer { get; set; } = ProtoCommon.GetMessageWriter();

        [SuppressMessage("Performance", "CA1819:Properties should not return arrays", Justification = "Private record")]
        public T[] Items { get; set; }

        public Schema Schema { get; set; }

        public bool SchemaOutdated { get; set; }

        public int Count { get; set; }

        public int CountPos { get; set; }

        public Task Task { get; set; } = Task.CompletedTask; // Task for the previous buffer.

        public long LastFlush { get; set; }
    }
}
