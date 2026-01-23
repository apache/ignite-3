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

namespace Apache.Ignite.Table;

using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Internal.Table.Serialization;
using Mapper;
using Network;

/// <summary>
/// Partition distribution provides table partition information.
/// This interface can be used to get all partitions of a table, the location of the primary replica of a partition,
/// the partition for a specific table key.
/// </summary>
public interface IPartitionDistribution
{
    /// <summary>
    /// Gets a list with all partitions.
    /// </summary>
    /// <returns>A task representing the asynchronous operation with a list of all partitions.</returns>
    ValueTask<IReadOnlyList<IPartition>> GetPartitionsAsync();

    /// <summary>
    /// Gets map with all partitions and their locations as of the time of the call.
    /// <para />
    /// NOTE: Prefer <see cref="GetPrimaryReplicaAsync"/> for performance-critical code.
    /// <para />
    /// NOTE: This assignment may become outdated if a re-assignment happens on the cluster.
    /// </summary>
    /// <returns>Map of partition to primary replica node.</returns>
    ValueTask<IReadOnlyDictionary<IPartition, IClusterNode>> GetPrimaryReplicasAsync();

    /// <summary>
    /// Gets all partitions hosted by the specified node as a primary replica as of the time of the call.
    /// <para />
    /// NOTE: This assignment may become outdated if a re-assignment happens on the cluster.
    /// </summary>
    /// <param name="node">Cluster node.</param>
    /// <returns>A task representing the asynchronous operation with a list of all partitions hosted by the specified node
    /// as a primary replica.</returns>
    ValueTask<IReadOnlyList<IPartition>> GetPrimaryReplicasAsync(IClusterNode node);

    /// <summary>
    /// Gets the primary replica for the specified partition.
    /// <para />
    /// NOTE: Prefer this method over <see cref="GetPrimaryReplicasAsync()"/> for performance-critical code.
    /// <para />
    /// NOTE: This assignment may become outdated if a re-assignment happens on the cluster.
    /// </summary>
    /// <param name="partition">Partition.</param>
    /// <returns>Primary replica.</returns>
    ValueTask<IClusterNode> GetPrimaryReplicaAsync(IPartition partition);

    /// <summary>
    /// Gets the partition for the specified table key.
    /// </summary>
    /// <param name="tuple">Table key tuple.</param>
    /// <returns>Partition that contains the specified key.</returns>
    ValueTask<IPartition> GetPartitionAsync(IIgniteTuple tuple);

    /// <summary>
    /// Gets the partition for the specified table key.
    /// </summary>
    /// <param name="key">Table key.</param>
    /// <returns>Partition that contains the specified key.</returns>
    /// <typeparam name="TK">Key type.</typeparam>
    [RequiresUnreferencedCode(ReflectionUtils.TrimWarning)]
    ValueTask<IPartition> GetPartitionAsync<TK>(TK key)
        where TK : notnull;

    /// <summary>
    /// Gets the partition for the specified table key.
    /// </summary>
    /// <param name="key">Table key.</param>
    /// <param name="mapper">Mapper for the key.</param>
    /// <returns>Partition that contains the specified key.</returns>
    /// <typeparam name="TK">Key type.</typeparam>
    ValueTask<IPartition> GetPartitionAsync<TK>(TK key, IMapper<TK> mapper)
        where TK : notnull;
}
