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

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Internal.Table.Serialization;
using Mapper;
using Network;

/// <summary>
/// Partition manager provides table partition information.
/// </summary>
[Obsolete("Use IPartitionDistribution instead.")]
public interface IPartitionManager
{
    /// <summary>
    /// Gets the primary replicas for all partitions.
    /// <para />
    /// NOTE: Prefer <see cref="GetPrimaryReplicaAsync"/> for performance-critical code.
    /// </summary>
    /// <returns>Map of partition to the primary replica node.</returns>
    ValueTask<IReadOnlyDictionary<IPartition, IClusterNode>> GetPrimaryReplicasAsync();

    /// <summary>
    /// Gets the primary replica for the specified partition.
    /// <para />
    /// NOTE: Prefer this method over <see cref="GetPrimaryReplicasAsync()"/> for performance-critical code.
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
