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

namespace Apache.Ignite.Internal.Linq;

using System;
using System.Collections.Generic;
using Ignite.Sql;

/// <summary>
/// Key for <see cref="ResultSelector"/> cached delegates. Equality logic is based on column types.
/// </summary>
/// <typeparam name="T">Target type.</typeparam>
internal readonly struct ResultSelectorCacheKey<T> : IEquatable<ResultSelectorCacheKey<T>>
    where T : notnull
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ResultSelectorCacheKey{T}"/> struct.
    /// </summary>
    /// <param name="target">Target object (can be type, constructor, etc).</param>
    /// <param name="columns">Columns.</param>
    /// <param name="options">Options.</param>
    public ResultSelectorCacheKey(T target, IReadOnlyList<IColumnMetadata> columns, ResultSelectorOptions options = default)
    {
        Target = target;
        Columns = columns;
        Options = options;
    }

    /// <summary>
    /// Gets columns.
    /// </summary>
    public IReadOnlyList<IColumnMetadata> Columns { get; }

    /// <summary>
    /// Gets options.
    /// </summary>
    public ResultSelectorOptions Options { get; }

    /// <summary>
    /// Gets target object (can be type, constructor, etc).
    /// </summary>
    public T Target { get; }

    public static bool operator ==(ResultSelectorCacheKey<T> left, ResultSelectorCacheKey<T> right)
    {
        return left.Equals(right);
    }

    public static bool operator !=(ResultSelectorCacheKey<T> left, ResultSelectorCacheKey<T> right)
    {
        return !(left == right);
    }

    /// <inheritdoc/>
    public bool Equals(ResultSelectorCacheKey<T> other)
    {
        if (!Target.Equals(other.Target))
        {
            return false;
        }

        if (Options != other.Options)
        {
            return false;
        }

        if (Columns.Count != other.Columns.Count)
        {
            return false;
        }

        for (var i = 0; i < Columns.Count; i++)
        {
            if (Columns[i].Type != other.Columns[i].Type)
            {
                return false;
            }

            if (Columns[i].Scale != other.Columns[i].Scale)
            {
                return false;
            }

            if (Columns[i].Nullable != other.Columns[i].Nullable)
            {
                return false;
            }
        }

        return true;
    }

    /// <inheritdoc/>
    public override bool Equals(object? obj) =>
        obj is ResultSelectorCacheKey<T> other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        HashCode hash = default;

        hash.Add(Target);
        hash.Add(Options);

        foreach (var column in Columns)
        {
            hash.Add(column.Type);
            hash.Add(column.Scale);
            hash.Add(column.Nullable);
        }

        return hash.ToHashCode();
    }
}
