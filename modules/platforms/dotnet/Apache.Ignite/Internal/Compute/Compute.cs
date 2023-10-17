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

namespace Apache.Ignite.Internal.Compute
{
    using System;
    using System.Buffers.Binary;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Threading.Tasks;
    using Buffers;
    using Common;
    using Ignite.Compute;
    using Ignite.Network;
    using Ignite.Table;
    using Proto;
    using Proto.MsgPack;
    using Table;
    using Table.Serialization;

    /// <summary>
    /// Compute API.
    /// </summary>
    internal sealed class Compute : ICompute
    {
        /** Socket. */
        private readonly ClientFailoverSocket _socket;

        /** Tables. */
        private readonly Tables _tables;

        /** Cached tables. */
        private readonly ConcurrentDictionary<string, Table> _tableCache = new();

        /// <summary>
        /// Initializes a new instance of the <see cref="Compute"/> class.
        /// </summary>
        /// <param name="socket">Socket.</param>
        /// <param name="tables">Tables.</param>
        public Compute(ClientFailoverSocket socket, Tables tables)
        {
            _socket = socket;
            _tables = tables;
        }

        /// <inheritdoc/>
        public async Task<T> ExecuteAsync<T>(
            IEnumerable<IClusterNode> nodes,
            IEnumerable<DeploymentUnit> units,
            string jobClassName,
            params object?[]? args)
        {
            IgniteArgumentCheck.NotNull(nodes);
            IgniteArgumentCheck.NotNull(jobClassName);

            return await ExecuteOnOneNode<T>(GetRandomNode(nodes), units, jobClassName, args).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task<T> ExecuteColocatedAsync<T>(
            string tableName,
            IIgniteTuple key,
            IEnumerable<DeploymentUnit> units,
            string jobClassName,
            params object?[]? args) =>
            await ExecuteColocatedAsync<T, IIgniteTuple>(
                    tableName,
                    key,
                    serializerHandlerFunc: static _ => TupleSerializerHandler.Instance,
                    units,
                    jobClassName,
                    args)
                .ConfigureAwait(false);

        /// <inheritdoc/>
        public async Task<T> ExecuteColocatedAsync<T, TKey>(
            string tableName,
            TKey key,
            IEnumerable<DeploymentUnit> units,
            string jobClassName,
            params object?[]? args)
            where TKey : notnull =>
            await ExecuteColocatedAsync<T, TKey>(
                    tableName,
                    key,
                    serializerHandlerFunc: table => table.GetRecordViewInternal<TKey>().RecordSerializer.Handler,
                    units,
                    jobClassName,
                    args)
                .ConfigureAwait(false);

        /// <inheritdoc/>
        public IDictionary<IClusterNode, Task<T>> BroadcastAsync<T>(
            IEnumerable<IClusterNode> nodes,
            IEnumerable<DeploymentUnit> units,
            string jobClassName,
            params object?[]? args)
        {
            IgniteArgumentCheck.NotNull(nodes);
            IgniteArgumentCheck.NotNull(jobClassName);
            IgniteArgumentCheck.NotNull(units);

            var res = new Dictionary<IClusterNode, Task<T>>();
            var units0 = units as ICollection<DeploymentUnit> ?? units.ToList(); // Avoid multiple enumeration.

            foreach (var node in nodes)
            {
                var task = ExecuteOnOneNode<T>(node, units0, jobClassName, args);

                res[node] = task;
            }

            return res;
        }

        /// <inheritdoc/>
        public override string ToString() => IgniteToStringBuilder.Build(GetType());

        [SuppressMessage("Security", "CA5394:Do not use insecure randomness", Justification = "Secure random is not required here.")]
        private static IClusterNode GetRandomNode(IEnumerable<IClusterNode> nodes)
        {
            var nodesCol = GetNodesCollection(nodes);

            IgniteArgumentCheck.Ensure(nodesCol.Count > 0, nameof(nodes), "Nodes can't be empty.");

            var idx = Random.Shared.Next(0, nodesCol.Count);

            return nodesCol.ElementAt(idx);
        }

        private static ICollection<IClusterNode> GetNodesCollection(IEnumerable<IClusterNode> nodes) =>
            nodes as ICollection<IClusterNode> ?? nodes.ToList();

        private static void WriteUnits(IEnumerable<DeploymentUnit> units, PooledArrayBuffer buf)
        {
            var w = buf.MessageWriter;

            if (units.TryGetNonEnumeratedCount(out var count))
            {
                w.Write(count);
                foreach (var unit in units)
                {
                    IgniteArgumentCheck.NotNullOrEmpty(unit.Name);
                    IgniteArgumentCheck.NotNullOrEmpty(unit.Version);

                    w.Write(unit.Name);
                    w.Write(unit.Version);
                }

                return;
            }

            // Enumerable without known count - enumerate first, write count later.
            count = 0;
            var countSpan = buf.GetSpan(5);
            buf.Advance(5);

            foreach (var unit in units)
            {
                count++;
                w.Write(unit.Name);
                w.Write(unit.Version);
            }

            countSpan[0] = MsgPackCode.Array32;
            BinaryPrimitives.WriteInt32BigEndian(countSpan[1..], count);
        }

        private async Task<T> ExecuteOnOneNode<T>(
            IClusterNode node,
            IEnumerable<DeploymentUnit> units,
            string jobClassName,
            object?[]? args)
        {
            IgniteArgumentCheck.NotNull(node);

            using var writer = ProtoCommon.GetMessageWriter();
            Write();

            using var res = await _socket.DoOutInOpAsync(ClientOp.ComputeExecute, writer, PreferredNode.FromName(node.Name))
                .ConfigureAwait(false);

            return Read(res);

            void Write()
            {
                var w = writer.MessageWriter;

                w.Write(node.Name);
                WriteUnits(units, writer);
                w.Write(jobClassName);
                w.WriteObjectCollectionAsBinaryTuple(args);
            }

            static T Read(in PooledBuffer buf)
            {
                var reader = buf.GetReader();

                return (T)reader.ReadObjectFromBinaryTuple()!;
            }
        }

        private async Task<Table> GetTableAsync(string tableName)
        {
            if (_tableCache.TryGetValue(tableName, out var cachedTable))
            {
                return cachedTable;
            }

            var table = await _tables.GetTableInternalAsync(tableName).ConfigureAwait(false);

            if (table != null)
            {
                _tableCache[tableName] = table;
                return table;
            }

            _tableCache.TryRemove(tableName, out _);

            throw new IgniteClientException(ErrorGroups.Client.TableIdNotFound, $"Table '{tableName}' does not exist.");
        }

        [SuppressMessage("Maintainability", "CA1508:Avoid dead conditional code", Justification = "False positive")]
        private async Task<T> ExecuteColocatedAsync<T, TKey>(
            string tableName,
            TKey key,
            Func<Table, IRecordSerializerHandler<TKey>> serializerHandlerFunc,
            IEnumerable<DeploymentUnit> units,
            string jobClassName,
            params object?[]? args)
            where TKey : notnull
        {
            IgniteArgumentCheck.NotNull(tableName);
            IgniteArgumentCheck.NotNull(key);
            IgniteArgumentCheck.NotNull(jobClassName);

            var units0 = units as ICollection<DeploymentUnit> ?? units.ToList(); // Avoid multiple enumeration.
            int? schemaVersion = null;

            while (true)
            {
                var table = await GetTableAsync(tableName).ConfigureAwait(false);
                var schema = await table.GetSchemaAsync(schemaVersion).ConfigureAwait(false);

                try
                {
                    using var bufferWriter = ProtoCommon.GetMessageWriter();
                    var colocationHash = Write(bufferWriter, table, schema);
                    var preferredNode = await table.GetPreferredNode(colocationHash, null).ConfigureAwait(false);

                    using var res = await _socket.DoOutInOpAsync(ClientOp.ComputeExecuteColocated, bufferWriter, preferredNode)
                        .ConfigureAwait(false);

                    return Read(res);
                }
                catch (IgniteException e) when (e.Code == ErrorGroups.Client.TableIdNotFound)
                {
                    // Table was dropped - remove from cache.
                    // Try again in case a new table with the same name exists.
                    _tableCache.TryRemove(tableName, out _);
                    schemaVersion = null;
                }
                catch (IgniteException e) when (e.Code == ErrorGroups.Table.SchemaVersionMismatch &&
                                                schemaVersion != e.GetExpectedSchemaVersion())
                {
                    schemaVersion = e.GetExpectedSchemaVersion();
                }
                catch (Exception e) when (e.CausedByUnmappedColumns() &&
                                          schemaVersion == null)
                {
                    schemaVersion = Table.SchemaVersionForceLatest;
                }
            }

            int Write(PooledArrayBuffer bufferWriter, Table table, Schema schema)
            {
                var w = bufferWriter.MessageWriter;

                w.Write(table.Id);
                w.Write(schema.Version);

                var serializerHandler = serializerHandlerFunc(table);
                var colocationHash = serializerHandler.Write(ref w, schema, key, keyOnly: true, computeHash: true);

                WriteUnits(units0, bufferWriter);
                w.Write(jobClassName);
                w.WriteObjectCollectionAsBinaryTuple(args);

                return colocationHash;
            }

            static T Read(in PooledBuffer buf)
            {
                var reader = buf.GetReader();

                _ = reader.ReadInt32();

                return (T)reader.ReadObjectFromBinaryTuple()!;
            }
        }
    }
}
