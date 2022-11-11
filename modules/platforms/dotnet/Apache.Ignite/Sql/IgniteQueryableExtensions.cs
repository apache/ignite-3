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
using System.Threading.Tasks;
using Internal.Linq;
using Table;

/// <summary>
/// LINQ extensions for Apache Ignite SQL engine.
/// <para />
/// Use <see cref="IRecordView{T}.AsQueryable"/> and <see cref="IKeyValueView{TK,TV}.AsQueryable"/> to query Ignite with LINQ, then
/// materialize the query results by calling <see cref="ToResultSetAsync{T}"/>.
/// </summary>
public static class IgniteQueryableExtensions
{
    /// <summary>
    /// Executes the Ignite query represented by the provided <paramref name="queryable"/> and returns the resulting
    /// <see cref="IResultSet{T}"/>.
    /// </summary>
    /// <param name="queryable">Queryable.</param>
    /// <typeparam name="T">Result type.</typeparam>
    /// <returns>Result set.</returns>
    public static async Task<IResultSet<T>> ToResultSetAsync<T>(this IQueryable<T> queryable)
    {
        if (queryable is not IIgniteQueryableInternal queryableInternal)
        {
            throw new InvalidOperationException("Provided query does not originate from Ignite table: " + queryable);
        }

        var model = queryableInternal.GetQueryModel();

        return await queryableInternal.Provider.Executor.ExecuteResultSetInternalAsync<T>(model).ConfigureAwait(false);
    }
}
