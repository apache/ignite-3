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

namespace Apache.Ignite.Sql;

using System;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Internal.Common;
using Table;

/// <summary>
/// LINQ extensions for Apache Ignite SQL engine.
/// <para />
/// Use <see cref="IRecordView{T}.AsQueryable"/> and <see cref="IKeyValueView{TK,TV}.AsQueryable"/> to query Ignite with LINQ, then
/// materialize the query results by calling <see cref="ToResultSetAsync{T}"/>.
/// </summary>
public static partial class IgniteQueryableExtensions
{
    /// <summary>
    /// Removes all rows that are matched by the specified query.
    /// <para />
    /// This method results in "DELETE FROM" distributed SQL query, performing bulk delete
    /// (as opposed to fetching all rows locally before deleting them).
    /// </summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="query">Query.</param>
    /// <returns>Affected row count.</returns>
    public static async Task<long> RemoveAllAsync<T>(this IQueryable<T> query)
    {
        IgniteArgumentCheck.NotNull(query, nameof(query));

        // TODO
        await Task.Delay(1).ConfigureAwait(false);

        return 0;
    }

    /// <summary>
    /// Removes all rows that are matched by the specified query.
    /// <para />
    /// This method results in "DELETE FROM" distributed SQL query, performing bulk delete
    /// (as opposed to fetching all rows locally before deleting them).
    /// </summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="query">Query.</param>
    /// <param name="predicate">Predicate.</param>
    /// <returns>Affected row count.</returns>
    public static async Task<long> RemoveAllAsync<T>(this IQueryable<T> query, Expression<Func<T, bool>> predicate)
    {
        IgniteArgumentCheck.NotNull(query, nameof(query));

        // TODO
        await Task.Delay(1).ConfigureAwait(false);

        return 0;
    }

    /// <summary>
    /// Updates all rows that are matched by the specified query.
    /// <para />
    /// This method results in "UPDATE" distributed SQL query, performing bulk update
    /// (as opposed to fetching all rows locally).
    /// </summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="query">Query.</param>
    /// <param name="updateDescriptor">Update descriptor.</param>
    /// <returns>Affected row count.</returns>
    public static async Task<long> UpdateAllAsync<T>(
        this IQueryable<T> query,
        Expression<Func<IUpdateDescriptor<T>, IUpdateDescriptor<T>>> updateDescriptor)
    {
        IgniteArgumentCheck.NotNull(query, nameof(query));
        IgniteArgumentCheck.NotNull(updateDescriptor, nameof(updateDescriptor));

        // TODO
        await Task.Delay(1).ConfigureAwait(false);

        return 0;
    }
}
