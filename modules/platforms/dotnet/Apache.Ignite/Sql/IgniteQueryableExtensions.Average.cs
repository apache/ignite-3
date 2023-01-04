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
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
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
    /// Returns the average of a sequence of values.
    /// </summary>
    /// <param name="queryable">Query.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains the average of a sequence of values.
    /// </returns>
    [DynamicDependency("Average`1", typeof(Queryable))]
    public static async Task<double> AverageAsync(this IQueryable<int> queryable)
    {
        // TODO: With double, decimal (int, long, double -> double; float -> float; decimal -> decimal)
        // TODO: With nullables
        IgniteArgumentCheck.NotNull(queryable, nameof(queryable));

        var method = new Func<IQueryable<int>, double>(Queryable.Average).GetMethodInfo();
        var expression = Expression.Call(null, method, queryable.Expression);

        var provider = queryable.ToQueryableInternal().Provider;
        return await provider.ExecuteSingleAsync<double>(expression).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the average of a sequence of values.
    /// </summary>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <param name="queryable">Query.</param>
    /// <param name="selector">A projection function to apply to each element.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains the maximum value in the sequence.
    /// </returns>
    [DynamicDependency("Average`1", typeof(Queryable))]
    public static async Task<double> AverageAsync<TSource>(
        this IQueryable<TSource> queryable,
        Expression<Func<TSource, int>> selector)
    {
        IgniteArgumentCheck.NotNull(queryable, nameof(queryable));

        var method = new Func<IQueryable<TSource>, Expression<Func<TSource, int>>, double>(Queryable.Average).GetMethodInfo();
        var expression = Expression.Call(null, method, queryable.Expression, Expression.Quote(selector));

        var provider = queryable.ToQueryableInternal().Provider;
        return await provider.ExecuteSingleAsync<double>(expression).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the average of a sequence of values.
    /// </summary>
    /// <param name="queryable">Query.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains the average of a sequence of values.
    /// </returns>
    [DynamicDependency("Average`1", typeof(Queryable))]
    public static async Task<double?> AverageAsync(this IQueryable<int?> queryable)
    {
        IgniteArgumentCheck.NotNull(queryable, nameof(queryable));

        var method = new Func<IQueryable<int?>, double?>(Queryable.Average).GetMethodInfo();
        var expression = Expression.Call(null, method, queryable.Expression);

        var provider = queryable.ToQueryableInternal().Provider;
        return await provider.ExecuteSingleAsync<double?>(expression).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the average of a sequence of values.
    /// </summary>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <param name="queryable">Query.</param>
    /// <param name="selector">A projection function to apply to each element.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains the maximum value in the sequence.
    /// </returns>
    [DynamicDependency("Average`1", typeof(Queryable))]
    public static async Task<double?> AverageAsync<TSource>(
        this IQueryable<TSource> queryable,
        Expression<Func<TSource, int?>> selector)
    {
        IgniteArgumentCheck.NotNull(queryable, nameof(queryable));

        var method = new Func<IQueryable<TSource>, Expression<Func<TSource, int?>>, double?>(Queryable.Average).GetMethodInfo();
        var expression = Expression.Call(null, method, queryable.Expression, Expression.Quote(selector));

        var provider = queryable.ToQueryableInternal().Provider;
        return await provider.ExecuteSingleAsync<double?>(expression).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the average of a sequence of values.
    /// </summary>
    /// <param name="queryable">Query.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains the average of a sequence of values.
    /// </returns>
    [DynamicDependency("Average`1", typeof(Queryable))]
    public static async Task<double> AverageAsync(this IQueryable<long> queryable)
    {
        IgniteArgumentCheck.NotNull(queryable, nameof(queryable));

        var method = new Func<IQueryable<long>, double>(Queryable.Average).GetMethodInfo();
        var expression = Expression.Call(null, method, queryable.Expression);

        var provider = queryable.ToQueryableInternal().Provider;
        return await provider.ExecuteSingleAsync<double>(expression).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the average of a sequence of values.
    /// </summary>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <param name="queryable">Query.</param>
    /// <param name="selector">A projection function to apply to each element.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains the maximum value in the sequence.
    /// </returns>
    [DynamicDependency("Average`1", typeof(Queryable))]
    public static async Task<double> AverageAsync<TSource>(
        this IQueryable<TSource> queryable,
        Expression<Func<TSource, long>> selector)
    {
        IgniteArgumentCheck.NotNull(queryable, nameof(queryable));

        var method = new Func<IQueryable<TSource>, Expression<Func<TSource, long>>, double>(Queryable.Average).GetMethodInfo();
        var expression = Expression.Call(null, method, queryable.Expression, Expression.Quote(selector));

        var provider = queryable.ToQueryableInternal().Provider;
        return await provider.ExecuteSingleAsync<double>(expression).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the average of a sequence of values.
    /// </summary>
    /// <param name="queryable">Query.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains the average of a sequence of values.
    /// </returns>
    [DynamicDependency("Average`1", typeof(Queryable))]
    public static async Task<double?> AverageAsync(this IQueryable<long?> queryable)
    {
        IgniteArgumentCheck.NotNull(queryable, nameof(queryable));

        var method = new Func<IQueryable<long?>, double?>(Queryable.Average).GetMethodInfo();
        var expression = Expression.Call(null, method, queryable.Expression);

        var provider = queryable.ToQueryableInternal().Provider;
        return await provider.ExecuteSingleAsync<double?>(expression).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the average of a sequence of values.
    /// </summary>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <param name="queryable">Query.</param>
    /// <param name="selector">A projection function to apply to each element.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains the maximum value in the sequence.
    /// </returns>
    [DynamicDependency("Average`1", typeof(Queryable))]
    public static async Task<double?> AverageAsync<TSource>(
        this IQueryable<TSource> queryable,
        Expression<Func<TSource, long?>> selector)
    {
        IgniteArgumentCheck.NotNull(queryable, nameof(queryable));

        var method = new Func<IQueryable<TSource>, Expression<Func<TSource, long?>>, double?>(Queryable.Average).GetMethodInfo();
        var expression = Expression.Call(null, method, queryable.Expression, Expression.Quote(selector));

        var provider = queryable.ToQueryableInternal().Provider;
        return await provider.ExecuteSingleAsync<double?>(expression).ConfigureAwait(false);
    }
}
