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
using System.Threading.Tasks;
using Buffers;
using Ignite.Table;
using Proto;
using Proto.BinaryTuple;
using Serialization;

/// <summary>
/// Data streamer provider.
/// </summary>
/// <typeparam name="T">Item type.</typeparam>
internal sealed class DataStreamerProvider<T>
    : IDataStreamerProvider<DataStreamerItem<T>, DataStreamerItemBatch<T>>
{
    private readonly IRecordSerializerHandler<T> _handler;

    private readonly Dictionary<int, DataStreamerItemBatch<T>> _batches = new();

    private readonly int _pageSize;

    /// <inheritdoc/>
    public DataStreamerItemBatch<T> Add(DataStreamerItem<T> item, Schema schema, int partitionCount)
    {
        var schema0 = schema;

        var columnCount = item.OperationType == DataStreamerOperationType.Remove
            ? schema0.KeyColumns.Length
            : schema0.Columns.Length;

        var tupleBuilder = new BinaryTupleBuilder(columnCount, hashedColumnsPredicate: schema0.HashedColumnIndexProvider);

        try
        {
            return Add0(item, ref tupleBuilder, schema0, partitionCount);
        }
        finally
        {
            tupleBuilder.Dispose();
        }
    }

    /// <inheritdoc/>
    public async Task FlushAsync(DataStreamerItemBatch<T> batch, IRetryPolicy retryPolicy, Schema schema)
    {
        batch.LastFlush = Stopwatch.GetTimestamp();
        await Task.Delay(1).ConfigureAwait(false);
    }

    private DataStreamerItemBatch<T> Add0(
        DataStreamerItem<T> item, ref BinaryTupleBuilder tupleBuilder, Schema schema, int partitionCount)
    {
        var columnCount = schema.Columns.Length;

        // Use MemoryMarshal to work around [CS8352]: "Cannot use variable 'noValueSet' in this context
        // because it may expose referenced variables outside of their declaration scope".
        Span<byte> noValueSet = stackalloc byte[columnCount / 8 + 1];
        Span<byte> noValueSetRef = MemoryMarshal.CreateSpan(ref MemoryMarshal.GetReference(noValueSet), columnCount);

        var keyOnly = item.OperationType == DataStreamerOperationType.Remove;
        _handler.Write(ref tupleBuilder, item.Data, schema, keyOnly: keyOnly, noValueSetRef);

        var partitionId = Math.Abs(tupleBuilder.GetHash() % partitionCount);
        var batch = GetOrCreateBatch(partitionId);

        lock (batch)
        {
            batch.Items[batch.Count++] = item;

            if (batch.Schema != schema)
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

        DataStreamerItemBatch<T> GetOrCreateBatch(int partitionId)
        {
            ref var batchRef = ref CollectionsMarshal.GetValueRefOrAddDefault(_batches, partitionId, out _);

            if (batchRef == null)
            {
                batchRef = new DataStreamerItemBatch<T>(_pageSize, schema, partitionId);
                batchRef.InitBuffer(schema);

                Metrics.StreamerBatchesActiveIncrement();
            }

            return batchRef;
        }
    }
}

/// <summary>
/// Batch.
/// </summary>
/// <typeparam name="T">Item type.</typeparam>
internal sealed record DataStreamerItemBatch<T> : IDataStreamerBatch
{
    /// <summary>
    /// Initializes a new instance of the <see cref="DataStreamerItemBatch{T}"/> class.
    /// </summary>
    /// <param name="capacity">Capacity.</param>
    /// <param name="schema">Schema.</param>
    /// <param name="partitionId">Partition id.</param>
    public DataStreamerItemBatch(int capacity, Schema schema, int partitionId)
    {
        PartitionId = partitionId;
        Items = GetPool().Rent(capacity);
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

    public Task FinishActiveTasks() => Task;

    public void InitBuffer(Schema schema)
    {
        var buf = Buffer;

        WriteBatchHeader(buf, PartitionId, schema, deletedSetReserveSize: Count);

        CountPos = buf.Position;
        buf.Advance(5); // Reserve count.
    }

    public void Release()
    {
        Buffer.Dispose();
    }

    private static ArrayPool<DataStreamerItem<T>> GetPool() => ArrayPool<DataStreamerItem<T>>.Shared;

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
}

