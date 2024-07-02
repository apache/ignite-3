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

namespace Apache.Ignite.Compute;

using System.Collections.Generic;
using Internal.Common;
using Network;

/// <summary>
/// Compute job target.
/// </summary>
public static class JobTarget
{
    /// <summary>
    /// Creates a job target for a specific node.
    /// </summary>
    /// <param name="node">Node.</param>
    /// <returns>Single node job target.</returns>
    public static IJobTarget<IClusterNode> Node(IClusterNode node)
    {
        IgniteArgumentCheck.NotNull(node);

        return new SingleNodeTarget(node);
    }

    /// <summary>
    /// Creates a job target for any node from the provided collection.
    /// </summary>
    /// <param name="nodes">Nodes.</param>
    /// <returns>Any node job target.</returns>
    public static IJobTarget<IEnumerable<IClusterNode>> AnyNode(IEnumerable<IClusterNode> nodes)
    {
        IgniteArgumentCheck.NotNull(nodes);

        return new AnyNodeTarget(nodes);
    }

    /// <summary>
    /// Creates a job target for any node from the provided collection.
    /// </summary>
    /// <param name="nodes">Nodes.</param>
    /// <returns>Any node job target.</returns>
    public static IJobTarget<IEnumerable<IClusterNode>> AnyNode(params IClusterNode[] nodes)
    {
        IgniteArgumentCheck.NotNull(nodes);

        return new AnyNodeTarget(nodes);
    }

    /// <summary>
    /// Creates a colocated job target for a specific table and key.
    /// </summary>
    /// <param name="tableName">Table name.</param>
    /// <param name="key">Key.</param>
    /// <typeparam name="TKey">Key type.</typeparam>
    /// <returns>Colocated job target.</returns>
    public static IJobTarget<TKey> Colocated<TKey>(string tableName, TKey key)
        where TKey : notnull
    {
        IgniteArgumentCheck.NotNull(tableName);
        IgniteArgumentCheck.NotNull(key);

        return new ColocatedTarget<TKey>(tableName, key);
    }

    internal sealed record SingleNodeTarget(IClusterNode Data) : IJobTarget<IClusterNode>;

    internal sealed record AnyNodeTarget(IEnumerable<IClusterNode> Data) : IJobTarget<IEnumerable<IClusterNode>>;

    internal sealed record ColocatedTarget<TKey>(string TableName, TKey Data) : IJobTarget<TKey>
        where TKey : notnull;
}
