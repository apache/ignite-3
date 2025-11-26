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
using System.Collections.Concurrent;
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
    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Cleanup.")]
    [SuppressMessage("Usage", "CA2219:Do not raise exceptions in finally clauses", Justification = "Rethrow.")]
    [SuppressMessage(
        "Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "WaitHandle is not used in SemaphoreSlim, no need to dispose.")]
    internal static async Task StreamDataAsync<T>(
        IAsyncEnumerable<DataStreamerItem<T>> data,
        Table table,
        IRecordSerializerHandler<T> writer,
        DataStreamerOptions options,
        CancellationToken cancellationToken)
    {
        IgniteArgumentCheck.NotNull(data);
        ValidateOptions(options);

        // ConcurrentDictionary is not necessary because we consume the source sequentially.
        // However, locking for batches is required due to auto-flush background task.
        var batches = new Dictionary<int, Batch<T>>();
        var failedItems = new ConcurrentQueue<DataStreamerItem<T>>();
        var retryPolicy = new RetryLimitPolicy { RetryLimit = options.RetryLimit };

        var schema = await table.GetSchemaAsync(null).ConfigureAwait(false);
        var schemaLock = new SemaphoreSlim(1);

        var partitionAssignment = await table.GetPartitionAssignmentAsync().ConfigureAwait(false);
        var partitionCount = partitionAssignment.Length; // Can't be changed.
        Debug.Assert(partitionCount > 0, "partitionCount > 0");

        using var autoFlushCts = new CancellationTokenSource();
        Task? autoFlushTask = null;
        Exception? error = null;

        try
        {
            autoFlushTask = AutoFlushAsync(autoFlushCts.Token);

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

                if (autoFlushTask.IsFaulted)
                {
                    await autoFlushTask.ConfigureAwait(false);
                }
            }

            await Drain().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            error = e;
        }
        finally
        {
            await autoFlushCts.CancelAsync().ConfigureAwait(false);

            if (autoFlushTask is { })
            {
                try
                {
                    await autoFlushTask.ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    if (e is not OperationCanceledException)
                    {
                        error ??= e;
                    }
                }
            }

            foreach (var batch in batches.Values)
            {
                lock (batch)
                {
                    for (var i = 0; i < batch.Count; i++)
                    {
                        failedItems.Enqueue(batch.Items[i]);
                    }

                    batch.Buffer.Dispose();
                    GetPool<T>().Return(batch.Items);

                    Metrics.StreamerItemsQueuedDecrement(batch.Count);
                    Metrics.StreamerBatchesActiveDecrement();
                }
            }

            if (error is { })
            {
                throw DataStreamerException.Create(error, failedItems);
            }

            if (!failedItems.IsEmpty)
            {
                // Should not happen.
                throw DataStreamerException.Create(new InvalidOperationException("Some items were not processed."), failedItems);
            }
        }

        return;

        async ValueTask<Batch<T>> AddWithRetryUnmapped(DataStreamerItem<T> item)
        {
            try
            {
                return Add(item);
            }
            catch (Exception e) when (e.CausedByUnmappedColumns())
            {
                await UpdateSchema().ConfigureAwait(false);
                return Add(item);
            }
        }

        Batch<T> Add(DataStreamerItem<T> item)
        {
            var schema0 = schema;

            var columnCount = item.OperationType == DataStreamerOperationType.Remove
                ? schema0.KeyColumns.Length
                : schema0.Columns.Length;

            var tupleBuilder = new BinaryTupleBuilder(columnCount, hashedColumnsPredicate: schema0.HashedColumnIndexProvider);

            try
            {
                return Add0(item, ref tupleBuilder, schema0);
            }
            finally
            {
                tupleBuilder.Dispose();
            }
        }

        Batch<T> Add0(DataStreamerItem<T> item, ref BinaryTupleBuilder tupleBuilder, Schema schema0)
        {
            var columnCount = schema0.Columns.Length;
            Span<byte> noValueSet = stackalloc byte[columnCount / 8 + 1];

            var keyOnly = item.OperationType == DataStreamerOperationType.Remove;
            writer.Write(ref tupleBuilder, item.Data, schema0, keyOnly: keyOnly, noValueSet);

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

            return batch;
        }

        Batch<T> GetOrCreateBatch(int partitionId)
        {
            ref var batchRef = ref CollectionsMarshal.GetValueRefOrAddDefault(batches, partitionId, out _);

            if (batchRef == null)
            {
                batchRef = new Batch<T>(options.PageSize, schema, partitionId);
                InitBuffer(batchRef, schema);

                Metrics.StreamerBatchesActiveIncrement();
            }

            return batchRef;
        }

        async Task SendAsync(Batch<T> batch)
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

                FinalizeBatchHeader(batch);
                batch.Task = SendAndDisposeBufAsync(
                    batch.Buffer, batch.PartitionId, batch.Task, batch.Items, batch.Count, batch.SchemaOutdated, batch.Schema.Version);

                batch.Items = GetPool<T>().Rent(options.PageSize);
                batch.Count = 0;
                batch.Schema = schema;
                batch.SchemaOutdated = false;
                batch.Buffer = ProtoCommon.GetMessageWriter(); // Prev buf will be disposed in SendAndDisposeBufAsync.
                InitBuffer(batch, batch.Schema);
                batch.LastFlush = Stopwatch.GetTimestamp();

                Metrics.StreamerBatchesActiveIncrement();
            }
        }

        async Task SendAndDisposeBufAsync(
            PooledArrayBuffer buf,
            int partitionId,
            Task oldTask,
            DataStreamerItem<T>[] items,
            int count,
            bool batchSchemaOutdated,
            int batchSchemaVer)
        {
            try
            {
                Debug.Assert(items.Length > 0, "items.Length > 0");
                var schema0 = schema;

                if (batchSchemaOutdated || batchSchemaVer < schema0.Version)
                {
                    // Schema update was detected while the batch was being filled.
                    // Re-serialize the whole batch.
                    ReWriteBatch(buf, partitionId, schema0, items.AsSpan(0, count), writer);
                }

                // ReSharper disable once AccessToModifiedClosure
                var preferredNode = PreferredNode.FromName(partitionAssignment[partitionId] ?? string.Empty);

                int? schemaVersion = null;
                while (true)
                {
                    try
                    {
                        if (schemaVersion is { })
                        {
                            // Serialize again with the new schema.
                            schema0 = await UpdateSchema(schemaVersion.Value).ConfigureAwait(false);
                            ReWriteBatch(buf, partitionId, schema0, items.AsSpan(0, count), writer);
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
                    catch (Exception e) when (e.CausedByUnmappedColumns())
                    {
                        schemaVersion = Table.SchemaVersionForceLatest;
                    }
                }
            }
            catch (Exception)
            {
                for (var i = 0; i < count; i++)
                {
                    failedItems.Enqueue(items[i]);
                }

                throw;
            }
            finally
            {
                buf.Dispose();
                GetPool<T>().Return(items);

                Metrics.StreamerItemsQueuedDecrement(count);
                Metrics.StreamerBatchesActiveDecrement();
            }
        }

        async Task AutoFlushAsync(CancellationToken flushCt)
        {
            while (!flushCt.IsCancellationRequested)
            {
                await Task.Delay(options.AutoFlushInterval, flushCt).ConfigureAwait(false);

                foreach (var batch in batches.Values)
                {
                    if (batch is { Count: > 0, Task.IsCompleted: true } &&
                        Stopwatch.GetElapsedTime(batch.LastFlush) > options.AutoFlushInterval)
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

                await batch.Task.ConfigureAwait(false);
            }
        }

        async ValueTask<Schema> UpdateSchema(int ver = Table.SchemaVersionForceLatest)
        {
            await schemaLock.WaitAsync(cancellationToken).ConfigureAwait(false);

            try
            {
                if (ver != Table.SchemaVersionForceLatest && schema.Version >= ver)
                {
                    return schema;
                }

                schema = await table.GetSchemaAsync(ver).ConfigureAwait(false);
                return schema;
            }
            finally
            {
                schemaLock.Release();
            }
        }
    }

    /// <summary>
    /// Validates the options.
    /// </summary>
    /// <param name="options">Streamer options.</param>
    internal static void ValidateOptions(DataStreamerOptions options)
    {
        IgniteArgumentCheck.Ensure(
            options.PageSize > 0,
            nameof(options.PageSize),
            $"{nameof(options.PageSize)} should be positive.");

        IgniteArgumentCheck.Ensure(
            options.AutoFlushInterval > TimeSpan.Zero,
            nameof(options.AutoFlushInterval),
            $"{nameof(options.AutoFlushInterval)} should be positive.");

        IgniteArgumentCheck.Ensure(
            options.RetryLimit >= 0,
            nameof(options.RetryLimit),
            $"{nameof(options.RetryLimit)} should be non-negative.");
    }

    private static void InitBuffer<T>(Batch<T> batch, Schema schema)
    {
        var buf = batch.Buffer;

        WriteBatchHeader(buf, batch.PartitionId, schema, deletedSetReserveSize: batch.Items.Length);

        batch.CountPos = buf.Position;
        buf.Advance(5); // Reserve count.
    }

    private static void WriteBatchHeader(PooledArrayBuffer buf, int partitionId, Schema schema, int deletedSetReserveSize)
    {
        var w = buf.MessageWriter;

        // Reserve space for deleted set - we don't know if we need it or not, and which size.
        w.WriteBitSet(deletedSetReserveSize);
        buf.Offset = buf.Position;

        // Write header.
        w.Write(schema.TableId);
        w.Write(partitionId);
        w.WriteNil(); // Deleted set. We assume there are no deleted items by default. The header will be rewritten if needed.
        w.Write(schema.Version);
    }

    private static void FinalizeBatchHeader<T>(Batch<T> batch)
    {
        var buf = batch.Buffer;

        if (HasDeletedItems<T>(batch.Items.AsSpan(0, batch.Count)))
        {
            // Re-write the entire header with the deleted set of actual size.
            var reservedBitSetSize = buf.Offset;
            var oldPos = buf.Position;

            buf.Position = 0;
            buf.MessageWriter.WriteBitSet(batch.Count);
            var actualBitSetSize = buf.Position;

            buf.Offset = 1 + reservedBitSetSize - actualBitSetSize; // 1 byte for null bit set used before.
            buf.Position = buf.Offset;

            var w = buf.MessageWriter;
            w.Write(batch.Schema.TableId);
            w.Write(batch.PartitionId);

            var deletedSet = w.WriteBitSet(batch.Count);

            for (var i = 0; i < batch.Count; i++)
            {
                if (batch.Items[i].OperationType == DataStreamerOperationType.Remove)
                {
                    deletedSet.SetBit(i);
                }
            }

            w.Write(batch.Schema.Version);

            // Count position should not change - we only rearrange the header above it.
            Debug.Assert(buf.Position == batch.CountPos, $"buf.Position = {buf.Position}, batch.CountPos = {batch.CountPos}");
            buf.Position = oldPos;
        }

        // Update count.
        buf.WriteByte(MsgPackCode.Int32, batch.CountPos);
        buf.WriteIntBigEndian(batch.Count, batch.CountPos + 1);
    }

    private static void ReWriteBatch<T>(
        PooledArrayBuffer buf,
        int partitionId,
        Schema schema,
        ReadOnlySpan<DataStreamerItem<T>> items,
        IRecordSerializerHandler<T> writer)
    {
        buf.Reset();

        var w = buf.MessageWriter;
        w.Write(schema.TableId);
        w.Write(partitionId);

        if (HasDeletedItems(items))
        {
            var deletedSet = w.WriteBitSet(items.Length);

            for (var i = 0; i < items.Length; i++)
            {
                if (items[i].OperationType == DataStreamerOperationType.Remove)
                {
                    deletedSet.SetBit(i);
                }
            }
        }
        else
        {
            w.WriteNil();
        }

        w.Write(schema.Version);
        w.Write(items.Length);

        foreach (var item in items)
        {
            var remove = item.OperationType == DataStreamerOperationType.Remove;
            writer.Write(ref w, schema, item.Data, keyOnly: remove, computeHash: false);
        }
    }

    private static bool HasDeletedItems<T>(ReadOnlySpan<DataStreamerItem<T>> items)
    {
        foreach (var t in items)
        {
            if (t.OperationType == DataStreamerOperationType.Remove)
            {
                return true;
            }
        }

        return false;
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

    private static ArrayPool<DataStreamerItem<T>> GetPool<T>() => ArrayPool<DataStreamerItem<T>>.Shared;

    private sealed record Batch<T>
    {
        public Batch(int capacity, Schema schema, int partitionId)
        {
            PartitionId = partitionId;
            Items = GetPool<T>().Rent(capacity);
            Schema = schema;
        }

        public int PartitionId { get; }

        public PooledArrayBuffer Buffer { get; set; } = ProtoCommon.GetMessageWriter();

        [SuppressMessage("Performance", "CA1819:Properties should not return arrays", Justification = "Private record")]
        public DataStreamerItem<T>[] Items { get; set; }

        public Schema Schema { get; set; }

        public bool SchemaOutdated { get; set; }

        public int Count { get; set; }

        public int CountPos { get; set; }

        public Task Task { get; set; } = Task.CompletedTask; // Task for the previous buffer.

        public long LastFlush { get; set; }
    }
}
