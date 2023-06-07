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
        Func<ValueTask<string[]>> partitionAssignmentProvider,
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
                // TODO: Don't call schemaProvider/partitionAssignmentProvider for each item.
                var schema = await schemaProvider().ConfigureAwait(false);
                var partitionAssignment = await partitionAssignmentProvider().ConfigureAwait(false);

                var (batch, partition) = AddItem(item, schema, partitionAssignment);

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

        (Batch<T> Batch, string Partition) AddItem(T item, Schema schema, string[] partitionAssignment)
        {
            // TODO: Dispose.
            var tupleBuilder = new BinaryTupleBuilder(schema.KeyColumnCount);

            // TODO: Entire batch must use the same schema.
            // Should we fix the schema for the entire streamer? Should be possible.
            // writer(item, schema, ref tupleBuilder);
            var columnCount = schema.Columns.Count;
            Span<byte> noValueSet = stackalloc byte[columnCount];
            writer.Write(ref tupleBuilder, item, schema, columnCount, noValueSet);

            var hash = tupleBuilder.Hash;
            var partition = partitionAssignment[Math.Abs(hash % partitionAssignment.Length)];

            if (!batches.TryGetValue(partition, out var batch))
            {
                batch = new Batch<T>
                {
                    Items = new List<T>(options.BatchSize), // TODO: Pooled buffers.
                    Buffer = ProtoCommon.GetMessageWriter(),
                    Schema = schema
                };

                // TODO: Write buffer header (schema, count placeholder).
                batches.Add(partition, batch);
            }

            // TODO: Write to buffer from tupleBuilder.
            batch.Items.Add(item);

            return (batch, partition);
        }
    }

    [SuppressMessage("Design", "CA1051:Do not declare visible instance fields", Justification = "Private class.")]
    [SuppressMessage("Design", "CA1002:Do not expose generic lists", Justification = "Private class.")]
    [SuppressMessage("StyleCop.CSharp.MaintainabilityRules", "SA1401:Fields should be private", Justification = "Private class.")]
    private sealed class Batch<T>
    {
        public List<T> Items { get; init; } = null!;

        public PooledArrayBuffer Buffer { get; init; } = null!;

        public Schema Schema { get; init; } = null!;
    }
}
