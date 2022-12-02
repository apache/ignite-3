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
internal readonly struct ResultSelectorCacheKey : IEquatable<ResultSelectorCacheKey>
{
    private readonly IReadOnlyList<IColumnMetadata> _columns;

    private readonly object _target;

    /// <summary>
    /// Initializes a new instance of the <see cref="ResultSelectorCacheKey"/> struct.
    /// </summary>
    /// <param name="columns">Columns.</param>
    /// <param name="target">Target object (can be type, constructor, etc).</param>
    public ResultSelectorCacheKey(IReadOnlyList<IColumnMetadata> columns, object target)
    {
        _columns = columns;
        _target = target;
    }

    public static bool operator ==(ResultSelectorCacheKey left, ResultSelectorCacheKey right)
    {
        return left.Equals(right);
    }

    public static bool operator !=(ResultSelectorCacheKey left, ResultSelectorCacheKey right)
    {
        return !(left == right);
    }

    /// <inheritdoc/>
    public bool Equals(ResultSelectorCacheKey other)
    {
        if (_target != other._target)
        {
            return false;
        }

        if (_columns.Count != other._columns.Count)
        {
            return false;
        }

        for (var i = 0; i < _columns.Count; i++)
        {
            if (_columns[i].Type != other._columns[i].Type)
            {
                return false;
            }
        }

        return true;
    }

    /// <inheritdoc/>
    public override bool Equals(object? obj) =>
        obj is ResultSelectorCacheKey other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        HashCode hash = default;

        hash.Add(_target);

        foreach (var column in _columns)
        {
            hash.Add(column.Type);
        }

        return hash.ToHashCode();
    }
}
