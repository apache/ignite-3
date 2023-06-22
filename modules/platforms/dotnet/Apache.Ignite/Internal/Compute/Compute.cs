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
            IgniteArgumentCheck.NotNull(nodes, nameof(nodes));
            IgniteArgumentCheck.NotNull(jobClassName, nameof(jobClassName));

            return await ExecuteOnOneNode<T>(GetRandomNode(nodes), jobClassName, args).ConfigureAwait(false);
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
                    serializerHandlerFunc: _ => TupleSerializerHandler.Instance,
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
            IgniteArgumentCheck.NotNull(nodes, nameof(nodes));
            IgniteArgumentCheck.NotNull(jobClassName, nameof(jobClassName));

            var res = new Dictionary<IClusterNode, Task<T>>();

            foreach (var node in nodes)
            {
                var task = ExecuteOnOneNode<T>(node, jobClassName, args);

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

        private async Task<T> ExecuteOnOneNode<T>(IClusterNode node, string jobClassName, object?[]? args)
        {
            IgniteArgumentCheck.NotNull(node, nameof(node));

            using var writer = ProtoCommon.GetMessageWriter();
            Write();

            using var res = await _socket.DoOutInOpAsync(ClientOp.ComputeExecute, writer, PreferredNode.FromName(node.Name))
                .ConfigureAwait(false);

            return Read(res);

            void Write()
            {
                var w = writer.MessageWriter;

                w.Write(node.Name);
                w.WriteNil(); // DeploymentUnits
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

        private async Task<T> ExecuteColocatedAsync<T, TKey>(
            string tableName,
            TKey key,
            Func<Table, IRecordSerializerHandler<TKey>> serializerHandlerFunc,
            string jobClassName,
            params object?[]? args)
            where TKey : notnull
        {
            IgniteArgumentCheck.NotNull(tableName, nameof(tableName));
            IgniteArgumentCheck.NotNull(key, nameof(key));
            IgniteArgumentCheck.NotNull(jobClassName, nameof(jobClassName));

            while (true)
            {
                var table = await GetTableAsync(tableName).ConfigureAwait(false);
                var schema = await table.GetLatestSchemaAsync().ConfigureAwait(false);

                using var bufferWriter = ProtoCommon.GetMessageWriter();
                var colocationHash = Write(bufferWriter, table, schema);
                var preferredNode = await table.GetPreferredNode(colocationHash, null).ConfigureAwait(false);

                try
                {
                    using var res = await _socket.DoOutInOpAsync(ClientOp.ComputeExecuteColocated, bufferWriter, preferredNode)
                        .ConfigureAwait(false);

                    return Read(res);
                }
                catch (IgniteException e) when (e.Code == ErrorGroups.Client.TableIdNotFound)
                {
                    // Table was dropped - remove from cache.
                    // Try again in case a new table with the same name exists.
                    _tableCache.TryRemove(tableName, out _);
                }
            }

            int Write(PooledArrayBuffer bufferWriter, Table table, Schema schema)
            {
                var w = bufferWriter.MessageWriter;

                w.Write(table.Id);
                w.Write(schema.Version);

                var serializerHandler = serializerHandlerFunc(table);
                var colocationHash = serializerHandler.Write(ref w, schema, key, keyOnly: true, computeHash: true);

                w.WriteNil(); // DeploymentUnits
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
