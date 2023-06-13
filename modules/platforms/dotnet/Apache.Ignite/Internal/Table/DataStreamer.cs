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
    /// <param name="sender">Batch sender.</param>
    /// <param name="writer">Item writer.</param>
    /// <param name="schemaProvider">Schema provider.</param>
    /// <param name="partitionAssignmentProvider">Partitioner.</param>
    /// <param name="options">Options.</param>
    /// <typeparam name="T">Element type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    internal static async Task StreamDataAsync<T>(
        IAsyncEnumerable<T> data,
        Func<PooledArrayBuffer, string, IRetryPolicy, Task> sender,
        IRecordSerializerHandler<T> writer,
        Func<Task<Schema>> schemaProvider,
        Func<ValueTask<string[]?>> partitionAssignmentProvider,
        DataStreamerOptions options)
    {
        IgniteArgumentCheck.NotNull(data);

        IgniteArgumentCheck.Ensure(options.BatchSize > 0, $"{nameof(options.BatchSize)} should be positive.");
        IgniteArgumentCheck.Ensure(options.AutoFlushFrequency > TimeSpan.Zero, $"{nameof(options.AutoFlushFrequency)} should be positive.");
        IgniteArgumentCheck.Ensure(options.RetryLimit >= 0, $"{nameof(options.RetryLimit)} should be non-negative.");

        // ConcurrentDictionary is not necessary because we consume the source sequentially.
        // However, locking for batches is required due to auto-flush background task.
        var batches = new Dictionary<string, Batch>();
        var retryPolicy = new RetryLimitPolicy { RetryLimit = options.RetryLimit };

        // TODO: IGNITE-19710 Data Streamer schema synchronization
        var schema = await schemaProvider().ConfigureAwait(false);
        var partitionAssignment = await partitionAssignmentProvider().ConfigureAwait(false);
        var lastPartitionsAssignmentCheck = Stopwatch.StartNew();
        using var cts = new CancellationTokenSource();

        try
        {
            _ = AutoFlushAsync(cts.Token);

            await foreach (var item in data)
            {
                var (batch, partition) = Add(item);

                if (batch.Count >= options.BatchSize)
                {
                    await SendAsync(batch, partition).ConfigureAwait(false);
                }

                if (lastPartitionsAssignmentCheck.Elapsed > PartitionAssignmentUpdateFrequency)
                {
                    var newAssignment = await partitionAssignmentProvider().ConfigureAwait(false);

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
            cts.Cancel();
            foreach (var batch in batches.Values)
            {
                batch.Buffer.Dispose();
            }
        }

        (Batch Batch, string Partition) Add(T item)
        {
            var tupleBuilder = new BinaryTupleBuilder(schema.Columns.Count, hashedColumnsPredicate: schema);

            try
            {
                return Add0(item, ref tupleBuilder);
            }
            finally
            {
                tupleBuilder.Dispose();
            }
        }

        (Batch Batch, string Partition) Add0(T item, ref BinaryTupleBuilder tupleBuilder)
        {
            var columnCount = schema.Columns.Count;

            // Use MemoryMarshal to work around [CS8352]: "Cannot use variable 'noValueSet' in this context
            // because it may expose referenced variables outside of their declaration scope".
            Span<byte> noValueSet = stackalloc byte[columnCount / 8 + 1];
            Span<byte> noValueSetRef = MemoryMarshal.CreateSpan(ref MemoryMarshal.GetReference(noValueSet), columnCount);

            writer.Write(ref tupleBuilder, item, schema, columnCount, noValueSetRef);

            var partition = partitionAssignment == null
                ? string.Empty // Default connection.
                : partitionAssignment[Math.Abs(tupleBuilder.Hash % partitionAssignment.Length)];

            var batch = GetOrCreateBatch(partition);

            lock (batch)
            {
                batch.Count++;

                noValueSet.CopyTo(batch.Buffer.MessageWriter.WriteBitSet(columnCount));
                batch.Buffer.MessageWriter.Write(tupleBuilder.Build().Span);
            }

            return (batch, partition);
        }

        Batch GetOrCreateBatch(string partition)
        {
            ref var batchRef = ref CollectionsMarshal.GetValueRefOrAddDefault(batches, partition, out _);

            if (batchRef == null)
            {
                batchRef = new Batch();
                InitBuffer(batchRef);
            }

            return batchRef;
        }

        async Task SendAsync(Batch batch, string partition)
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

                batch.Task = SendAndDisposeBufAsync(buf, partition, batch.Task);

                batch.Count = 0;
                batch.Buffer = ProtoCommon.GetMessageWriter(); // Prev buf will be disposed in SendAndDisposeBufAsync.
                InitBuffer(batch);
                batch.LastFlush = Stopwatch.GetTimestamp();
            }
        }

        async Task SendAndDisposeBufAsync(PooledArrayBuffer buf, string partition, Task oldTask)
        {
            try
            {
                // Wait for the previous batch for this node to preserve item order.
                await oldTask.ConfigureAwait(false);
                await sender(buf, partition, retryPolicy).ConfigureAwait(false);
            }
            finally
            {
                buf.Dispose();
            }
        }

        async Task AutoFlushAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(options.AutoFlushFrequency, cancellationToken).ConfigureAwait(false);
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

        void InitBuffer(Batch batch)
        {
            var buf = batch.Buffer;

            var w = buf.MessageWriter;
            w.Write(schema.TableId);
            w.WriteTx(null);
            w.Write(schema.Version);

            batch.CountPos = buf.Position;
            buf.Advance(5); // Reserve count.
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

    private sealed record Batch
    {
        public PooledArrayBuffer Buffer { get; set; } = ProtoCommon.GetMessageWriter();

        public int Count { get; set; }

        public int CountPos { get; set; }

        public Task Task { get; set; } = Task.CompletedTask; // Task for the previous buffer.

        public long LastFlush { get; set; }
    }
}
