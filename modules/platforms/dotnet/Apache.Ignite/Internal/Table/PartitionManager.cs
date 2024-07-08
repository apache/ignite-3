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

using System.Collections.Generic;
using System.Threading.Tasks;
using Ignite.Network;
using Ignite.Table;

/// <summary>
/// Table partition manager.
/// </summary>
internal sealed class PartitionManager : IPartitionManager
{
    private readonly Table _table;

    /// <summary>
    /// Initializes a new instance of the <see cref="PartitionManager"/> class.
    /// </summary>
    /// <param name="table">Table.</param>
    internal PartitionManager(Table table)
    {
        _table = table;
    }

    /// <inheritdoc/>
    public Task<IDictionary<IPartition, IClusterNode>> GetPrimaryReplicasAsync()
    {
        throw new System.NotImplementedException();
    }

    /// <inheritdoc/>
    public Task<IClusterNode> GetPrimaryReplicaAsync(IPartition partition)
    {
        throw new System.NotImplementedException();
    }

    /// <inheritdoc/>
    public Task<IPartition> GetPartitionAsync(IIgniteTuple tuple)
    {
        throw new System.NotImplementedException();
    }

    /// <inheritdoc/>
    public Task<IPartition> GetPartitionAsync<TK>(TK key)
        where TK : notnull
    {
        throw new System.NotImplementedException();
    }
}
