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
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Buffers;
using Common;
using Ignite.Table;
using Proto;
using Proto.BinaryTuple;
using Serialization;

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
    /// <param name="writer">Item writer.</param>
    /// <param name="schemaProvider">Schema provider.</param>
    /// <param name="partitionAssignmentProvider">Partitioner.</param>
    /// <param name="options">Options.</param>
    /// <typeparam name="T">Element type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    internal static async Task StreamDataAsync<T>(
        IAsyncEnumerable<T> data,
        Func<IList<T>, string, Task> sender,
        IRecordSerializerHandler<T> writer,
        Func<Task<Schema>> schemaProvider,
        Func<ValueTask<string[]?>> partitionAssignmentProvider,
        DataStreamerOptions options)
    {
        IgniteArgumentCheck.NotNull(data);

        IgniteArgumentCheck.Ensure(options.BatchSize > 0, $"{nameof(options.BatchSize)} should be positive.");
        IgniteArgumentCheck.Ensure(
            options.PerNodeParallelOperations > 0,
            $"{nameof(options.PerNodeParallelOperations)} should be positive.");

        var batches = new Dictionary<string, Batch<T>>();

        try
        {
            await foreach (var item in data)
            {
                var schema = await schemaProvider().ConfigureAwait(false);
                var partitionAssignment = await partitionAssignmentProvider().ConfigureAwait(false);

                var (batch, partition) = Add(item, schema, partitionAssignment);

                batch.Items.Add(item);

                if (batch.Items.Count >= options.BatchSize)
                {
                    // TODO: Allow adding items to another buffer while sending previous.
                    await sender(batch.Items, partition).ConfigureAwait(false);

                    batch.Items.Clear();
                    batch.Buffer.Clear();
                }
            }

            foreach (var (partition, batch) in batches)
            {
                if (batch.Items.Count > 0)
                {
                    await sender(batch.Items, partition).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            foreach (var batch in batches.Values)
            {
                batch.Buffer.Dispose();
            }
        }

        (Batch<T> Batch, string Partition) Add(T item, Schema schema, string[]? partitionAssignment)
        {
            var tupleBuilder = new BinaryTupleBuilder(schema.Columns.Count, hashedColumnsPredicate: schema);

            try
            {
                return Add0(item, schema, partitionAssignment, ref tupleBuilder);
            }
            finally
            {
                tupleBuilder.Dispose();
            }
        }

        (Batch<T> Batch, string Partition) Add0(
            T item,
            Schema schema,
            string[]? partitionAssignment,
            ref BinaryTupleBuilder tupleBuilder)
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

            var batch = GetOrCreateBatch(partition, schema);
            batch.Items.Add(item);

            noValueSet.CopyTo(batch.Buffer.MessageWriter.WriteBitSet(columnCount));
            batch.Buffer.MessageWriter.Write(tupleBuilder.Build().Span);

            if (batch.Schema != schema)
            {
                // TODO: Support schema change during streaming? Separate ticket?
                // We should re-serialize the buffer in this case.
                throw new NotSupportedException("Schema change is not supported.");
            }

            return (batch, partition);
        }

        Batch<T> GetOrCreateBatch(string partition, Schema schema)
        {
            if (batches.TryGetValue(partition, out var batch))
            {
                return batch;
            }

            var buf = ProtoCommon.GetMessageWriter();

            var w = buf.MessageWriter;
            w.Write(schema.TableId);
            w.WriteTx(null);
            w.Write(schema.Version);

            var countPos = buf.Position;
            buf.Advance(5); // Reserve count.

            // TODO: Pooled buffers for Items?
            batch = new Batch<T>(new List<T>(options.BatchSize), buf, schema, countPos);
            batches.Add(partition, batch);

            return batch;
        }
    }

    private sealed record Batch<T>(IList<T> Items, PooledArrayBuffer Buffer, Schema Schema, int CountPos);
}
