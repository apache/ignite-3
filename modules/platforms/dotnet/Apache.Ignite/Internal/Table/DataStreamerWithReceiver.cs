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
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Buffers;
using Common;
using Compute;
using Ignite.Compute;
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
internal static class DataStreamerWithReceiver
{
    /// <summary>
    /// Streams the data.
    /// </summary>
    /// <param name="data">Data.</param>
    /// <param name="table">Table.</param>
    /// <param name="keySelector">Key func.</param>
    /// <param name="payloadSelector">Payload func.</param>
    /// <param name="keyWriter">Key writer.</param>
    /// <param name="options">Options.</param>
    /// <param name="expectResults">Whether to expect results from the receiver.</param>
    /// <param name="units">Deployment units. Can be empty.</param>
    /// <param name="receiverClassName">Java class name of the streamer receiver to execute on the server.</param>
    /// <param name="receiverArgs">Receiver args.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <typeparam name="TSource">Source type.</typeparam>
    /// <typeparam name="TKey">Key type.</typeparam>
    /// <typeparam name="TPayload">Payload type.</typeparam>
    /// <typeparam name="TResult">Result type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    internal static async IAsyncEnumerable<TResult> StreamDataAsync<TSource, TKey, TPayload, TResult>(
        IAsyncEnumerable<TSource> data,
        Table table,
        Func<TSource, TKey> keySelector,
        Func<TSource, TPayload> payloadSelector,
        IRecordSerializerHandler<TKey> keyWriter,
        DataStreamerOptions options,
        bool expectResults,
        IEnumerable<DeploymentUnit> units,
        string receiverClassName,
        ICollection<object>? receiverArgs,
        [EnumeratorCancellation] CancellationToken cancellationToken)
        where TKey : notnull
        where TPayload : notnull
    {
        IgniteArgumentCheck.NotNull(data);
        DataStreamer.ValidateOptions(options);

        Channel<TResult> resultChannel = CreateResultChannel<TResult>(options.PageSize);

        // ConcurrentDictionary is not necessary because we consume the source sequentially.
        // However, locking for batches is required due to auto-flush background task.
        var batches = new Dictionary<int, Batch<TPayload>>();
        var retryPolicy = new RetryLimitPolicy { RetryLimit = options.RetryLimit };
        var units0 = units as ICollection<DeploymentUnit> ?? units.ToList(); // Avoid multiple enumeration.

        var schema = await table.GetSchemaAsync(null).ConfigureAwait(false);

        var partitionAssignment = await table.GetPartitionAssignmentAsync().ConfigureAwait(false);
        var partitionCount = partitionAssignment.Length; // Can't be changed.
        Debug.Assert(partitionCount > 0, "partitionCount > 0");

        Type? payloadType = null;
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

                var batch = Add(item);
                if (batch.Count >= options.PageSize)
                {
                    await SendAsync(batch).ConfigureAwait(false);
                }

                // Yield results.
                // TODO: Results might be blocked by the producer, we should use Task.WhenAny.
                while (expectResults && resultChannel.Reader.TryRead(out var result))
                {
                    yield return result;
                }
            }

            // Drain batches.
            await Drain().ConfigureAwait(false);

            // Drain results.
            while (expectResults && resultChannel.Reader.TryRead(out var result))
            {
                yield return result;
            }
        }
        finally
        {
            flushCts.Cancel();
            resultChannel.Writer.Complete();

            foreach (var batch in batches.Values)
            {
                GetPool<TPayload>().Return(batch.Items);

                Metrics.StreamerItemsQueuedDecrement(batch.Count);
                Metrics.StreamerBatchesActiveDecrement();
            }
        }

        yield break;

        Batch<TPayload> Add(TSource item)
        {
            var tupleBuilder = new BinaryTupleBuilder(schema.KeyColumns.Length, hashedColumnsPredicate: schema.HashedColumnIndexProvider);

            try
            {
                return Add0(item, ref tupleBuilder);
            }
            finally
            {
                tupleBuilder.Dispose();
            }
        }

        Batch<TPayload> Add0(TSource item, ref BinaryTupleBuilder tupleBuilder)
        {
            // Write key to compute hash.
            var key = keySelector(item);
            keyWriter.Write(ref tupleBuilder, key, schema, keyOnly: true, Span<byte>.Empty);

            var partitionId = Math.Abs(tupleBuilder.GetHash() % partitionCount);
            var batch = GetOrCreateBatch(partitionId);

            var payload = payloadSelector(item);
            IgniteArgumentCheck.NotNull(payload);

            if (payloadType == null)
            {
                payloadType = payload.GetType();
            }
            else if (payloadType != payload.GetType())
            {
                throw new InvalidOperationException(
                    $"All streamer items returned by payloadSelector must be of the same type. " +
                    $"Expected: {payloadType}, actual: {payload.GetType()}.");
            }

            lock (batch)
            {
                batch.Items[batch.Count++] = payload;
            }

            Metrics.StreamerItemsQueuedIncrement();

            return batch;
        }

        Batch<TPayload> GetOrCreateBatch(int partitionId)
        {
            ref var batchRef = ref CollectionsMarshal.GetValueRefOrAddDefault(batches, partitionId, out _);

            if (batchRef == null)
            {
                batchRef = new Batch<TPayload>(options.PageSize, partitionId);
                Metrics.StreamerBatchesActiveIncrement();
            }

            return batchRef;
        }

        async Task SendAsync(Batch<TPayload> batch)
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

                batch.Task = SendAndDisposeBufAsync(batch.PartitionId, batch.Task, batch.Items, batch.Count);

                batch.Items = GetPool<TPayload>().Rent(options.PageSize);
                batch.Count = 0;
                batch.LastFlush = Stopwatch.GetTimestamp();

                Metrics.StreamerBatchesActiveIncrement();
            }
        }

        async Task SendAndDisposeBufAsync(
            int partitionId,
            Task oldTask,
            TPayload[] items,
            int count)
        {
            // Release the thread that holds the batch lock.
            await Task.Yield();

            var buf = ProtoCommon.GetMessageWriter();
            TResult[]? results = null;

            try
            {
                SerializeBatch(buf, items.AsSpan(0, count), partitionId);

                // ReSharper disable once AccessToModifiedClosure
                var preferredNode = PreferredNode.FromName(partitionAssignment[partitionId] ?? string.Empty);

                // Wait for the previous batch for this node to preserve item order.
                await oldTask.ConfigureAwait(false);
                (results, int resultsCount) = await SendBatchAsync<TResult>(table, buf, count, preferredNode, retryPolicy).ConfigureAwait(false);

                if (results != null)
                {
                    for (var i = 0; i < resultsCount; i++)
                    {
                        TResult result = results[i];
                        await resultChannel.Writer.WriteAsync(result, cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            finally
            {
                buf.Dispose();
                GetPool<TPayload>().Return(items);

                if (results != null)
                {
                    GetPool<TResult>().Return(results);
                }

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

                await batch.Task.ConfigureAwait(false);
            }
        }

        void SerializeBatch<T>(
            PooledArrayBuffer buf,
            Span<T> items,
            int partitionId)
        {
            // T is one of the supported types (numbers, strings, etc).
            var w = buf.MessageWriter;

            w.Write(table.Id);
            w.Write(partitionId);

            Compute.WriteUnits(units0, buf);

            w.Write(expectResults);
            WriteReceiverPayload(ref w, receiverClassName, receiverArgs ?? Array.Empty<object>(), items);
        }
    }

    private static Channel<TResult> CreateResultChannel<TResult>(int pageSize) =>
        Channel.CreateBounded<TResult>(new BoundedChannelOptions(pageSize)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true, // One reader: resulting IAsyncEnumerable.
            SingleWriter = false // Many writers: batches may complete in parallel.
        });

    private static void WriteReceiverPayload<T>(ref MsgPackWriter w, string className, ICollection<object> args, Span<T> items)
    {
        Debug.Assert(items.Length > 0, "items.Length > 0");
        Debug.Assert(!items.IsEmpty, "!items.IsEmpty");

        // className + args size + args + items size + item type + items.
        int binaryTupleSize = 1 + 1 + args.Count * 3 + 1 + 1 + items.Length;
        using var builder = new BinaryTupleBuilder(binaryTupleSize);

        builder.AppendString(className);
        builder.AppendInt(args.Count);

        foreach (var arg in args)
        {
            builder.AppendObjectWithType(arg);
        }

        builder.AppendObjectCollectionWithType(items);

        w.Write(binaryTupleSize);
        w.Write(builder.Build().Span);
    }

    private static async Task<(T[]? ResultsPooledArray, int ResultsCount)> SendBatchAsync<T>(
        Table table,
        PooledArrayBuffer buf,
        int count,
        PreferredNode preferredNode,
        IRetryPolicy retryPolicy)
    {
        var (resBuf, socket) = await table.Socket.DoOutInOpAndGetSocketAsync(
                ClientOp.StreamerWithReceiverBatchSend,
                tx: null,
                buf,
                preferredNode,
                retryPolicy)
            .ConfigureAwait(false);

        using (resBuf)
        {
            Metrics.StreamerBatchesSent.Add(1, socket.MetricsContext.Tags);
            Metrics.StreamerItemsSent.Add(count, socket.MetricsContext.Tags);

            return Read(resBuf.GetReader());
        }

        static (T[]? ResultsPooledArray, int ResultsCount) Read(MsgPackReader reader)
        {
            if (reader.TryReadNil())
            {
                return (null, 0);
            }

            var numElements = reader.ReadInt32();
            if (numElements == 0)
            {
                return (null, 0);
            }

            var tuple = new BinaryTupleReader(reader.ReadBinary(), numElements);

            return tuple.GetObjectCollectionWithType<T>();
        }
    }

    private static ArrayPool<T> GetPool<T>() => ArrayPool<T>.Shared;

    private sealed record Batch<TPayload>
    {
        public Batch(int capacity, int partitionId)
        {
            PartitionId = partitionId;
            Items = GetPool<TPayload>().Rent(capacity);
        }

        public int PartitionId { get; }

        [SuppressMessage("Performance", "CA1819:Properties should not return arrays", Justification = "Private record")]
        public TPayload[] Items { get; set; }

        public int Count { get; set; }

        public Task Task { get; set; } = Task.CompletedTask; // Task for the previous buffer.

        public long LastFlush { get; set; }
    }
}
