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
using System.Collections.ObjectModel;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Common;
using Ignite.Network;
using Ignite.Table;
using Network;
using Proto;
using Proto.MsgPack;
using Serialization;

/// <summary>
/// Table partition manager.
/// </summary>
internal sealed class PartitionManager : IPartitionManager
{
    private static readonly object PartitionsLock = new();

    // Cached partition objects (HashPartition is just a wrapper around a number).
    // Those wrappers implement IPartition interface and can't be structs.
    private static volatile HashPartition[]? _partitions;

    private readonly Table _table;

    private readonly object _primaryReplicasLock = new();

    // Cached primary replicas.
    private volatile PrimaryReplicas? _primaryReplicas;

    /// <summary>
    /// Initializes a new instance of the <see cref="PartitionManager"/> class.
    /// </summary>
    /// <param name="table">Table.</param>
    internal PartitionManager(Table table)
    {
        _table = table;
    }

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyDictionary<IPartition, IClusterNode>> GetPrimaryReplicasAsync()
    {
        var replicas = await GetPrimaryReplicasInternalAsync().ConfigureAwait(false);

        return replicas.GetDictionary();
    }

    /// <inheritdoc/>
    public async ValueTask<IClusterNode> GetPrimaryReplicaAsync(IPartition partition)
    {
        IgniteArgumentCheck.NotNull(partition);

        if (partition is not HashPartition hashPartition)
        {
            throw new ArgumentException("Unsupported partition type: " + partition.GetType());
        }

        if (hashPartition.PartitionId < 0)
        {
            throw new ArgumentException("Partition id can't be negative: " + partition);
        }

        var replicas = await GetPrimaryReplicasInternalAsync().ConfigureAwait(false);
        var nodes = replicas.Nodes;

        if (hashPartition.PartitionId >= nodes.Length)
        {
            throw new ArgumentException($"Partition id can't be greater than {nodes.Length - 1}: {partition}");
        }

        return nodes[hashPartition.PartitionId];
    }

    /// <inheritdoc/>
    public ValueTask<IPartition> GetPartitionAsync(IIgniteTuple tuple) =>
        GetPartitionInternalAsync(tuple, TupleSerializerHandler.Instance);

    /// <inheritdoc/>
    public ValueTask<IPartition> GetPartitionAsync<TK>(TK key)
        where TK : notnull =>
        GetPartitionInternalAsync(key, _table.GetRecordViewInternal<TK>().RecordSerializer.Handler);

    /// <inheritdoc/>
    public override string ToString() =>
        new IgniteToStringBuilder(GetType())
            .Append(_table, "Table")
            .Build();

    private static HashPartition[] GetCachedPartitionArray(int count)
    {
        var parts = _partitions;
        if (parts != null && parts.Length >= count)
        {
            return parts;
        }

        lock (PartitionsLock)
        {
            parts = _partitions;
            if (parts != null && parts.Length >= count)
            {
                return parts;
            }

            parts = new HashPartition[count];
            for (var i = 0; i < count; i++)
            {
                parts[i] = new HashPartition(i);
            }

            _partitions = parts;
            return parts;
        }
    }

    private async ValueTask<PrimaryReplicas> GetPrimaryReplicasInternalAsync()
    {
        // Socket.PartitionAssignmentTimestamp is updated on every response, including heartbeats,
        // so the cache can't be stale for very long.
        var timestamp = _table.Socket.PartitionAssignmentTimestamp;
        var cached = _primaryReplicas;

        if (cached != null && cached.Timestamp >= timestamp)
        {
            return cached;
        }

        using var bufferWriter = ProtoCommon.GetMessageWriter();
        bufferWriter.MessageWriter.Write(_table.Id);

        using var resBuf = await _table.Socket.DoOutInOpAsync(ClientOp.PrimaryReplicasGet, bufferWriter).ConfigureAwait(false);
        return Read(resBuf.GetReader());

        PrimaryReplicas Read(MsgPackReader r)
        {
            var count = r.ReadInt32();
            var primaryReplicas = new ClusterNode[count];

            for (var i = 0; i < count; i++)
            {
                var id = r.ReadInt32();
                var node = ClusterNode.Read(ref r);

                primaryReplicas[id] = node;
            }

            PrimaryReplicas? replicas;

            lock (_primaryReplicasLock)
            {
                replicas = _primaryReplicas;

                if (replicas == null || replicas.Timestamp < timestamp)
                {
                    // Got newer data - update cached replicas.
                    replicas = new PrimaryReplicas(primaryReplicas, timestamp);
                    _primaryReplicas = replicas;
                }
            }

            return replicas;
        }
    }

    private async ValueTask<IPartition> GetPartitionInternalAsync<TK>(TK key, IRecordSerializerHandler<TK> serializerHandler)
    {
        var schema = await _table.GetSchemaAsync(null).ConfigureAwait(false);
        var colocationHash = serializerHandler.GetKeyColocationHash(schema, key);

        var partitions = await GetPrimaryReplicasInternalAsync().ConfigureAwait(false);
        var partitionsCount = partitions.Nodes.Length;

        var partitionId = Math.Abs(colocationHash % partitionsCount);
        return GetCachedPartitionArray(partitionsCount)[partitionId];
    }

    [SuppressMessage("Performance", "CA1819:Properties should not return arrays", Justification = "Private record.")]
    private record PrimaryReplicas(ClusterNode[] Nodes, long Timestamp)
    {
        private volatile ReadOnlyDictionary<IPartition, IClusterNode>? _dict;

        public ReadOnlyDictionary<IPartition, IClusterNode> GetDictionary()
        {
            var dict = _dict;

            // Race condition here is ok, one of the threads will just overwrite the other's result with the same data.
            if (dict == null)
            {
                var res = new Dictionary<IPartition, IClusterNode>();
                var parts = GetCachedPartitionArray(Nodes.Length);

                for (var i = 0; i < Nodes.Length; i++)
                {
                    res.Add(parts[i], Nodes[i]);
                }

                dict = new ReadOnlyDictionary<IPartition, IClusterNode>(res);
                _dict = dict;
            }

            return dict;
        }
    }
}
