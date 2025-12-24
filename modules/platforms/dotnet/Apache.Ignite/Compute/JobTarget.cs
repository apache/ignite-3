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

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Internal.Common;
using Internal.Table;
using Internal.Table.Serialization;
using Network;
using Table;
using Table.Mapper;

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
    [RequiresUnreferencedCode(ReflectionUtils.TrimWarning)]
    public static IJobTarget<TKey> Colocated<TKey>(QualifiedName tableName, TKey key)
        where TKey : notnull
    {
        IgniteArgumentCheck.NotNull(key);

        return new ColocatedTarget<TKey>(tableName, key, null);
    }

    /// <summary>
    /// Creates a colocated job target for a specific table and key.
    /// </summary>
    /// <param name="tableName">Table name.</param>
    /// <param name="key">Key.</param>
    /// <param name="mapper">Mapper for the key.</param>
    /// <typeparam name="TKey">Key type.</typeparam>
    /// <returns>Colocated job target.</returns>
    public static IJobTarget<TKey> Colocated<TKey>(QualifiedName tableName, TKey key, IMapper<TKey> mapper)
        where TKey : notnull
    {
        IgniteArgumentCheck.NotNull(key);

        return new ColocatedTarget<TKey>(tableName, key, mapper);
    }

    /// <summary>
    /// Creates a colocated job target for a specific table and key.
    /// </summary>
    /// <param name="tableName">Table name.</param>
    /// <param name="key">Key.</param>
    /// <typeparam name="TKey">Key type.</typeparam>
    /// <returns>Colocated job target.</returns>
    [RequiresUnreferencedCode(ReflectionUtils.TrimWarning)]
    public static IJobTarget<TKey> Colocated<TKey>(string tableName, TKey key)
        where TKey : notnull =>
        Colocated(QualifiedName.Parse(tableName), key);

    /// <summary>
    /// Creates a colocated job target for a specific table and key.
    /// </summary>
    /// <param name="tableName">Table name.</param>
    /// <param name="key">Key.</param>
    /// <param name="mapper">Mapper for the key.</param>
    /// <typeparam name="TKey">Key type.</typeparam>
    /// <returns>Colocated job target.</returns>
    public static IJobTarget<TKey> Colocated<TKey>(string tableName, TKey key, IMapper<TKey> mapper)
        where TKey : notnull =>
        Colocated(QualifiedName.Parse(tableName), key, mapper);

    /// <summary>
    /// Single node job target.
    /// </summary>
    /// <param name="Data">Cluster node.</param>
    internal sealed record SingleNodeTarget(IClusterNode Data) : IJobTarget<IClusterNode>;

    /// <summary>
    /// Any node job target.
    /// </summary>
    /// <param name="Data">Nodes.</param>
    internal sealed record AnyNodeTarget(IEnumerable<IClusterNode> Data) : IJobTarget<IEnumerable<IClusterNode>>;

    /// <summary>
    /// Colocated job target.
    /// </summary>
    /// <param name="TableName">Table name.</param>
    /// <param name="Data">Key.</param>
    /// <param name="Mapper">Optional mapper for the key.</param>
    /// <typeparam name="TKey">Key type.</typeparam>
    internal sealed record ColocatedTarget<TKey>(QualifiedName TableName, TKey Data, IMapper<TKey>? Mapper) : IJobTarget<TKey>
        where TKey : notnull
    {
        /// <summary>
        /// Gets the cached serializer handler function.
        /// </summary>
        internal Func<Table, IRecordSerializerHandler<TKey>>? SerializerHandlerFunc { get; } = GetSerializerHandlerFunc(Mapper);

        private static Func<Table, IRecordSerializerHandler<TKey>>? GetSerializerHandlerFunc(IMapper<TKey>? mapper)
        {
            if (mapper == null)
            {
                return null;
            }

            var handler = new MapperSerializerHandler<TKey>(mapper);

            return _ => handler;
        }
    }
}
