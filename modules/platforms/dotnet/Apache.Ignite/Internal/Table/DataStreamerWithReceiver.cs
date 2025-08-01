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
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Buffers;
using Common;
using Compute;
using Ignite.Compute;
using Ignite.Table;
using Marshalling;
using Proto;
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
    /// <param name="resultChannel">Channel for results from the receiver. Null when results are not expected.</param>
    /// <param name="units">Deployment units. Can be empty.</param>
    /// <param name="receiverClassName">Java class name of the streamer receiver to execute on the server.</param>
    /// <param name="receiverExecutionOptions">Receiver options.</param>
    /// <param name="payloadMarshaller">Payload marshaller.</param>
    /// <param name="argMarshaller">Argument marshaller.</param>
    /// <param name="resultMarshaller">Result marshaller.</param>
    /// <param name="receiverArg">Receiver arg.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <typeparam name="TSource">Source type.</typeparam>
    /// <typeparam name="TKey">Key type.</typeparam>
    /// <typeparam name="TPayload">Payload type.</typeparam>
    /// <typeparam name="TArg">Arg type.</typeparam>
    /// <typeparam name="TResult">Result type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Cleanup.")]
    [SuppressMessage("Usage", "CA2219:Do not raise exceptions in finally clauses", Justification = "Rethrow.")]
    internal static async Task StreamDataAsync<TSource, TKey, TPayload, TArg, TResult>(
        IAsyncEnumerable<TSource> data,
        Table table,
        Func<TSource, TKey> keySelector,
        Func<TSource, TPayload> payloadSelector,
        IRecordSerializerHandler<TKey> keyWriter,
        DataStreamerOptions options,
        Channel<TResult>? resultChannel,
        IEnumerable<DeploymentUnit> units,
        string receiverClassName,
        ReceiverExecutionOptions receiverExecutionOptions,
        IMarshaller<TPayload>? payloadMarshaller,
        IMarshaller<TArg>? argMarshaller,
        IMarshaller<TResult>? resultMarshaller,
        TArg receiverArg,
        CancellationToken cancellationToken)
        where TKey : notnull
        where TPayload : notnull
    {
        IgniteArgumentCheck.NotNull(data);
        DataStreamer.ValidateOptions(options);

        var customReceiverExecutionOptions = receiverExecutionOptions != ReceiverExecutionOptions.Default;

        // ConcurrentDictionary is not necessary because we consume the source sequentially.
        // However, locking for batches is required due to auto-flush background task.
        var batches = new Dictionary<int, Batch<TSource, TPayload>>();
        var failedItems = new ConcurrentQueue<TSource>();
        var retryPolicy = new RetryLimitPolicy { RetryLimit = options.RetryLimit };
        var units0 = units as ICollection<DeploymentUnit> ?? units.ToList(); // Avoid multiple enumeration.

        var schema = await table.GetSchemaAsync(null).ConfigureAwait(false);

        var partitionAssignment = await table.GetPartitionAssignmentAsync().ConfigureAwait(false);
        var partitionCount = partitionAssignment.Length; // Can't be changed.
        Debug.Assert(partitionCount > 0, "partitionCount > 0");

        Type? payloadType = null;
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

                var batch = Add(item);
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
                        failedItems.Enqueue(batch.SourceItems[i]);
                    }

                    GetPool<TPayload>().Return(batch.Items);
                    GetPool<TSource>().Return(batch.SourceItems);

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

        Batch<TSource, TPayload> Add(TSource item)
        {
            var key = keySelector(item);
            var hash = keyWriter.GetKeyColocationHash(schema, key);
            var partitionId = Math.Abs(hash % partitionCount);
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
                batch.Items[batch.Count] = payload;
                batch.SourceItems[batch.Count] = item;

                batch.Count++;
            }

            Metrics.StreamerItemsQueuedIncrement();

            return batch;
        }

        Batch<TSource, TPayload> GetOrCreateBatch(int partitionId)
        {
            ref var batchRef = ref CollectionsMarshal.GetValueRefOrAddDefault(batches, partitionId, out _);

            if (batchRef == null)
            {
                batchRef = new Batch<TSource, TPayload>(options.PageSize, partitionId);
                Metrics.StreamerBatchesActiveIncrement();
            }

            return batchRef;
        }

        async Task SendAsync(Batch<TSource, TPayload> batch)
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

                batch.Task = SendAndDisposeBufAsync(batch.PartitionId, batch.Task, batch.Items, batch.SourceItems, batch.Count);

                batch.Items = GetPool<TPayload>().Rent(options.PageSize);
                batch.SourceItems = GetPool<TSource>().Rent(options.PageSize);
                batch.Count = 0;
                batch.LastFlush = Stopwatch.GetTimestamp();

                Metrics.StreamerBatchesActiveIncrement();
            }
        }

        async Task SendAndDisposeBufAsync(
            int partitionId,
            Task oldTask,
            TPayload[] items,
            TSource[] sourceItems,
            int count)
        {
            // Release the thread that holds the batch lock.
            await Task.Yield();

            var buf = ProtoCommon.GetMessageWriter();
            TResult[]? results = null;

            try
            {
                SerializeBatch(buf, new ArraySegment<TPayload>(items, 0, count), partitionId);

                // ReSharper disable once AccessToModifiedClosure
                var preferredNode = PreferredNode.FromName(partitionAssignment[partitionId] ?? string.Empty);

                // Wait for the previous batch for this node to preserve item order.
                await oldTask.ConfigureAwait(false);
                (results, int resultsCount) = await SendBatchAsync(
                    table,
                    buf,
                    count,
                    preferredNode,
                    retryPolicy,
                    expectResults: resultChannel != null,
                    customReceiverExecutionOptions,
                    resultMarshaller).ConfigureAwait(false);

                if (results != null && resultChannel != null)
                {
                    for (var i = 0; i < resultsCount; i++)
                    {
                        TResult result = results[i];
                        await resultChannel.Writer.WriteAsync(result, cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            catch (ChannelClosedException)
            {
                // Consumer does not want more results, stop returning them, but keep streaming.
                resultChannel = null;
            }
            catch (Exception)
            {
                for (var i = 0; i < count; i++)
                {
                    failedItems.Enqueue(sourceItems[i]);
                }

                throw;
            }
            finally
            {
                buf.Dispose();
                GetPool<TPayload>().Return(items);
                GetPool<TSource>().Return(sourceItems);

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
                await Task.Delay(options.AutoFlushInterval, flushCt).ConfigureAwait(false);
                var ts = Stopwatch.GetTimestamp();

                foreach (var batch in batches.Values)
                {
                    if (batch.Count > 0 && ts - batch.LastFlush > options.AutoFlushInterval.Ticks)
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

        void SerializeBatch(
            PooledArrayBuffer buf,
            ArraySegment<TPayload> items,
            int partitionId)
        {
            // T is one of the supported types (numbers, strings, etc).
            var w = buf.MessageWriter;

            w.Write(table.Id);
            w.Write(partitionId);

            Compute.WriteUnits(units0, buf);

            var expectResults = resultChannel != null;
            w.Write(expectResults);
            StreamerReceiverSerializer.WriteReceiverInfo(ref w, receiverClassName, receiverArg, items, payloadMarshaller, argMarshaller);

            w.Write(receiverExecutionOptions.Priority);
            w.Write(receiverExecutionOptions.MaxRetries);
            w.Write((int)receiverExecutionOptions.ExecutorType);
        }
    }

    private static async Task<(T[]? ResultsPooledArray, int ResultsCount)> SendBatchAsync<T>(
        Table table,
        PooledArrayBuffer buf,
        int count,
        PreferredNode preferredNode,
        IRetryPolicy retryPolicy,
        bool expectResults,
        bool customReceiverExecutionOptions,
        IMarshaller<T>? marshaller)
    {
        var (resBuf, socket) = await table.Socket.DoWithRetryAsync(
                (buf, customReceiverExecutionOptions),
                static (_, _) => ClientOp.StreamerWithReceiverBatchSend,
                async static (socket, arg) =>
                {
                    if (arg.customReceiverExecutionOptions &&
                        !socket.ConnectionContext.ServerHasFeature(ProtocolBitmaskFeature.StreamerReceiverExecutionOptions))
                    {
                        throw new IgniteClientException(
                            ErrorGroups.Client.ProtocolCompatibility,
                            $"{nameof(ReceiverExecutionOptions)} are not supported by the server.");
                    }

                    var res = await socket.DoOutInOpAsync(ClientOp.StreamerWithReceiverBatchSend, arg.buf)
                        .ConfigureAwait(false);

                    return (res, socket);
                },
                preferredNode,
                retryPolicy)
            .ConfigureAwait(false);

        using (resBuf)
        {
            Metrics.StreamerBatchesSent.Add(1, socket.MetricsContext.Tags);
            Metrics.StreamerItemsSent.Add(count, socket.MetricsContext.Tags);

            return expectResults
                ? StreamerReceiverSerializer.ReadReceiverResults(resBuf.GetReader(), marshaller)
                : (null, 0);
        }
    }

    private static ArrayPool<T> GetPool<T>() => ArrayPool<T>.Shared;

    private sealed record Batch<TSource, TPayload>
    {
        public Batch(int capacity, int partitionId)
        {
            PartitionId = partitionId;
            Items = GetPool<TPayload>().Rent(capacity);
            SourceItems = GetPool<TSource>().Rent(capacity);
        }

        public int PartitionId { get; }

        [SuppressMessage("Performance", "CA1819:Properties should not return arrays", Justification = "Private record")]
        public TSource[] SourceItems { get; set; }

        [SuppressMessage("Performance", "CA1819:Properties should not return arrays", Justification = "Private record")]
        public TPayload[] Items { get; set; }

        public int Count { get; set; }

        public Task Task { get; set; } = Task.CompletedTask; // Task for the previous buffer.

        public long LastFlush { get; set; }
    }
}
