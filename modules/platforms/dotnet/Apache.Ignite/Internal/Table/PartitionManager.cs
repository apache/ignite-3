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
    private readonly Table _table;

    private readonly object _partitionsLock = new();

    private readonly object _primaryReplicasLock = new();

    private volatile HashPartition[]? _partitions; // Cached partition objects.

    private volatile ClusterNode[]? _primaryReplicas; // Cached primary replicas.

    /// <summary>
    /// Initializes a new instance of the <see cref="PartitionManager"/> class.
    /// </summary>
    /// <param name="table">Table.</param>
    internal PartitionManager(Table table)
    {
        _table = table;
    }

    /// <inheritdoc/>
    public async ValueTask<IDictionary<IPartition, IClusterNode>> GetPrimaryReplicasAsync()
    {
        // TODO: Check cached assignment.
        using var bufferWriter = ProtoCommon.GetMessageWriter();
        bufferWriter.MessageWriter.Write(_table.Id);

        using var resBuf = await _table.Socket.DoOutInOpAsync(ClientOp.PrimaryReplicasGet, bufferWriter).ConfigureAwait(false);
        return Read(resBuf.GetReader());

        IDictionary<IPartition, IClusterNode> Read(MsgPackReader r)
        {
            var count = r.ReadInt32();
            var parts = GetPartitionArray(count);
            var res = new Dictionary<IPartition, IClusterNode>(count);

            lock (_primaryReplicasLock)
            {
                var primaryReplicas = _primaryReplicas != null && _primaryReplicas.Length == count
                    ? _primaryReplicas
                    : new ClusterNode[count];

                for (var i = 0; i < count; i++)
                {
                    var id = r.ReadInt32();
                    var node = ClusterNode.Read(r);

                    res.Add(parts[id], node);
                    primaryReplicas[id] = node;
                }

                _primaryReplicas = primaryReplicas;
            }

            return res;
        }
    }

    /// <inheritdoc/>
    public async ValueTask<IClusterNode> GetPrimaryReplicaAsync(IPartition partition)
    {
        IgniteArgumentCheck.NotNull(partition);

        if (partition is not HashPartition hashPartition)
        {
            throw new ArgumentException("Unsupported partition type: " + partition.GetType());
        }

        // TODO: Use cached array.
        var replicas = await GetPrimaryReplicasAsync().ConfigureAwait(false);
        if (!replicas.TryGetValue(hashPartition, out var node))
        {
            throw new ArgumentException("Primary replica not found for partition: " + partition);
        }

        return node;
    }

    /// <inheritdoc/>
    public ValueTask<IPartition> GetPartitionAsync(IIgniteTuple tuple) =>
        GetPartitionInternal(tuple, TupleSerializerHandler.Instance);

    /// <inheritdoc/>
    public ValueTask<IPartition> GetPartitionAsync<TK>(TK key)
        where TK : notnull =>
        GetPartitionInternal(key, _table.GetRecordViewInternal<TK>().RecordSerializer.Handler);

    private async ValueTask<IPartition> GetPartitionInternal<TK>(TK key, IRecordSerializerHandler<TK> serializerHandler)
    {
        var schema = await _table.GetSchemaAsync(null).ConfigureAwait(false);
        var colocationHash = serializerHandler.GetKeyColocationHash(schema, key);

        // TODO: Use cached.
        var partitions = await GetPrimaryReplicasAsync().ConfigureAwait(false);

        var partitionId = Math.Abs(colocationHash % partitions.Count);
        return GetPartitionArray(partitions.Count)[partitionId];
    }

    private HashPartition[] GetPartitionArray(int count)
    {
        var parts = _partitions;
        if (parts != null && parts.Length >= count)
        {
            return parts;
        }

        lock (_partitionsLock)
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
}
