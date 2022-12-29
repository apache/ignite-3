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
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Internal.Common;
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
        IgniteArgumentCheck.NotNull(queryable, nameof(queryable));

        var queryableInternal = queryable.ToQueryableInternal();
        var model = queryableInternal.GetQueryModel();

        return await queryableInternal.Provider.Executor.ExecuteResultSetInternalAsync<T>(model).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the Ignite query represented by the provided <paramref name="queryable"/> and returns the resulting
    /// <see cref="IResultSet{T}"/> as <see cref="IAsyncEnumerable{T}"/>.
    /// </summary>
    /// <param name="queryable">Queryable.</param>
    /// <typeparam name="T">Result type.</typeparam>
    /// <returns>Result set.</returns>
    [SuppressMessage("Reliability", "CA2007:Consider calling ConfigureAwait on the awaited task", Justification = "False positive.")]
    public static async IAsyncEnumerable<T> AsAsyncEnumerable<T>(this IQueryable<T> queryable)
    {
        IgniteArgumentCheck.NotNull(queryable, nameof(queryable));

        await using var resultSet = await queryable.ToResultSetAsync().ConfigureAwait(false);

        await foreach (var row in resultSet)
        {
            yield return row;
        }
    }

    /// <summary>
    /// Determines whether a sequence contains any elements.
    /// </summary>
    /// <param name="queryable">Query.</param>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <returns>
    /// A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains <see langword="true" /> if the source sequence contains any elements; otherwise, <see langword="false" />.
    /// </returns>
    public static async Task<bool> AnyAsync<TSource>(this IQueryable<TSource> queryable)
    {
        IgniteArgumentCheck.NotNull(queryable, nameof(queryable));

        // TODO: Better way to do this? Cache like in CachedReflectionInfo?
        var method = new Func<IQueryable<object>, bool>(Queryable.Any)
            .GetMethodInfo()
            .GetGenericMethodDefinition()
            .MakeGenericMethod(typeof(TSource));

        var provider = queryable.ToQueryableInternal().Provider;

        return await provider
            .ExecuteSingleAsync<bool>(Expression.Call(null, method, queryable.Expression), returnDefaultWhenEmpty: false)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Determines whether all elements of a sequence satisfy a condition.
    /// </summary>
    /// <param name="queryable">Query.</param>
    /// <param name="predicate">Predicate.</param>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <returns>
    /// A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains <see langword="true" /> if the source sequence contains any elements; otherwise, <see langword="false" />.
    /// </returns>
    public static Task<bool> AllAsync<TSource>(this IQueryable<TSource> queryable, Expression<Func<TSource, bool>> predicate)
    {
        IgniteArgumentCheck.NotNull(queryable, nameof(queryable));

        throw new NotImplementedException();
    }

    /// <summary>
    /// Returns the number of elements in a sequence.
    /// </summary>
    /// <param name="queryable">Query.</param>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains the number of elements in the input sequence.</returns>
    public static Task<int> CountAsync<TSource>(this IQueryable<TSource> queryable)
    {
        IgniteArgumentCheck.NotNull(queryable, nameof(queryable));

        throw new NotImplementedException();
    }

    /// <summary>
    /// Returns a <see cref="long" /> that represents the total number of elements in a sequence.
    /// </summary>
    /// <param name="queryable">Query.</param>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains the number of elements in the input sequence.</returns>
    public static Task<long> LongCountAsync<TSource>(this IQueryable<TSource> queryable)
    {
        IgniteArgumentCheck.NotNull(queryable, nameof(queryable));

        throw new NotImplementedException();
    }

    /// <summary>
    /// Returns the first element of a sequence.
    /// </summary>
    /// <param name="queryable">Query.</param>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains the first element in the input sequence.</returns>
    public static Task<TSource> FirstAsync<TSource>(this IQueryable<TSource> queryable)
    {
        IgniteArgumentCheck.NotNull(queryable, nameof(queryable));

        throw new NotImplementedException();
    }

    /// <summary>
    /// Returns the first element of a sequence, or a default value if the sequence contains no elements.
    /// </summary>
    /// <param name="queryable">Query.</param>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains <see langword="default" /> ( <typeparamref name="TSource" /> ) if
    /// <paramref name="queryable" /> is empty; otherwise, the first element in <paramref name="queryable" />.
    /// </returns>
    public static Task<TSource?> FirstOrDefaultAsync<TSource>(this IQueryable<TSource> queryable)
    {
        IgniteArgumentCheck.NotNull(queryable, nameof(queryable));

        throw new NotImplementedException();
    }

    /// <summary>
    /// Returns the last element of a sequence.
    /// </summary>
    /// <param name="queryable">Query.</param>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains the last element in the input sequence.</returns>
    public static Task<TSource> LastAsync<TSource>(this IQueryable<TSource> queryable)
    {
        IgniteArgumentCheck.NotNull(queryable, nameof(queryable));

        throw new NotImplementedException();
    }

    /// <summary>
    /// Returns the last element of a sequence, or a default value if the sequence contains no elements.
    /// </summary>
    /// <param name="queryable">Query.</param>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains <see langword="default" /> ( <typeparamref name="TSource" /> ) if
    /// <paramref name="queryable" /> is empty; otherwise, the last element in <paramref name="queryable" />.
    /// </returns>
    public static Task<TSource?> LastOrDefaultAsync<TSource>(this IQueryable<TSource> queryable)
    {
        IgniteArgumentCheck.NotNull(queryable, nameof(queryable));

        throw new NotImplementedException();
    }

    /* TODO: Single/SingleOrDefault, Min, Max, Sum, Average, Contains?, ToList, ToArray, ToDictionary, AsAsyncEnumerable */

    /// <summary>
    /// Generates SQL representation of the specified query.
    /// </summary>
    /// <param name="queryable">Query.</param>
    /// <returns>SQL string.</returns>
    public static string ToQueryString(this IQueryable queryable) => queryable.ToQueryableInternal().GetQueryData().QueryText;

    private static IIgniteQueryableInternal ToQueryableInternal(this IQueryable queryable) =>
        queryable as IIgniteQueryableInternal ?? throw GetInvalidQueryableException(queryable);

    private static InvalidOperationException GetInvalidQueryableException(IQueryable queryable) =>
        new($"Provided query does not originate from Ignite table: '{queryable}'. " +
            "Use 'IRecordView<T>.AsQueryable()' and 'IKeyValueView<TK, TV>.AsQueryable()' to run LINQ queries in Ignite.");
}
