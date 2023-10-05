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
public static partial class IgniteQueryableExtensions
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
        IgniteArgumentCheck.NotNull(queryable);

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
        IgniteArgumentCheck.NotNull(queryable);

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
    [DynamicDependency("Any`1", typeof(Queryable))]
    public static async Task<bool> AnyAsync<TSource>(this IQueryable<TSource> queryable)
    {
        IgniteArgumentCheck.NotNull(queryable);

        var method = new Func<IQueryable<TSource>, bool>(Queryable.Any).GetMethodInfo();
        var expression = Expression.Call(null, method, queryable.Expression);

        var provider = queryable.ToQueryableInternal().Provider;
        return await provider.ExecuteSingleAsync<bool>(expression).ConfigureAwait(false);
    }

    /// <summary>
    /// Determines whether any element of a sequence satisfies a condition.
    /// </summary>
    /// <param name="queryable">Query.</param>
    /// <param name="predicate">Predicate.</param>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <returns>
    /// A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains <see langword="true" /> if the source sequence contains any elements matching the specified predicate;
    /// otherwise, <see langword="false" />.
    /// </returns>
    [DynamicDependency("Any`1", typeof(Queryable))]
    public static async Task<bool> AnyAsync<TSource>(this IQueryable<TSource> queryable, Expression<Func<TSource, bool>> predicate)
    {
        IgniteArgumentCheck.NotNull(queryable);

        var method = new Func<IQueryable<TSource>, Expression<Func<TSource, bool>>, bool>(Queryable.Any).GetMethodInfo();
        var expression = Expression.Call(null, method, queryable.Expression, Expression.Quote(predicate));

        var provider = queryable.ToQueryableInternal().Provider;
        return await provider.ExecuteSingleAsync<bool>(expression).ConfigureAwait(false);
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
    [DynamicDependency("All`1", typeof(Queryable))]
    public static async Task<bool> AllAsync<TSource>(this IQueryable<TSource> queryable, Expression<Func<TSource, bool>> predicate)
    {
        IgniteArgumentCheck.NotNull(queryable);

        var method = new Func<IQueryable<TSource>, Expression<Func<TSource, bool>>, bool>(Queryable.All).GetMethodInfo();
        var expression = Expression.Call(null, method, queryable.Expression, Expression.Quote(predicate));

        var provider = queryable.ToQueryableInternal().Provider;
        return await provider.ExecuteSingleAsync<bool>(expression).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the number of elements in a sequence.
    /// </summary>
    /// <param name="queryable">Query.</param>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains the number of elements in the input sequence.</returns>
    [DynamicDependency("Count`1", typeof(Queryable))]
    public static async Task<int> CountAsync<TSource>(this IQueryable<TSource> queryable)
    {
        IgniteArgumentCheck.NotNull(queryable);

        var method = new Func<IQueryable<TSource>, int>(Queryable.Count).GetMethodInfo();
        var expression = Expression.Call(null, method, queryable.Expression);

        var provider = queryable.ToQueryableInternal().Provider;
        return await provider.ExecuteSingleAsync<int>(expression).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the number of elements in a sequence.
    /// </summary>
    /// <param name="queryable">Query.</param>
    /// <param name="predicate">Predicate.</param>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains the number of elements in the input sequence.</returns>
    [DynamicDependency("Count`1", typeof(Queryable))]
    public static async Task<int> CountAsync<TSource>(this IQueryable<TSource> queryable, Expression<Func<TSource, bool>> predicate)
    {
        IgniteArgumentCheck.NotNull(queryable);

        var method = new Func<IQueryable<TSource>, Expression<Func<TSource, bool>>, int>(Queryable.Count).GetMethodInfo();
        var expression = Expression.Call(null, method, queryable.Expression, Expression.Quote(predicate));

        var provider = queryable.ToQueryableInternal().Provider;
        return await provider.ExecuteSingleAsync<int>(expression).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns a <see cref="long" /> that represents the total number of elements in a sequence.
    /// </summary>
    /// <param name="queryable">Query.</param>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains the number of elements in the input sequence.</returns>
    [DynamicDependency("LongCount`1", typeof(Queryable))]
    public static async Task<long> LongCountAsync<TSource>(this IQueryable<TSource> queryable)
    {
        IgniteArgumentCheck.NotNull(queryable);

        var method = new Func<IQueryable<TSource>, long>(Queryable.LongCount).GetMethodInfo();
        var expression = Expression.Call(null, method, queryable.Expression);

        var provider = queryable.ToQueryableInternal().Provider;
        return await provider.ExecuteSingleAsync<long>(expression).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns a <see cref="long" /> that represents the total number of elements in a sequence.
    /// </summary>
    /// <param name="queryable">Query.</param>
    /// <param name="predicate">Predicate.</param>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains the number of elements in the input sequence.</returns>
    [DynamicDependency("LongCount`1", typeof(Queryable))]
    public static async Task<long> LongCountAsync<TSource>(this IQueryable<TSource> queryable, Expression<Func<TSource, bool>> predicate)
    {
        IgniteArgumentCheck.NotNull(queryable);

        var method = new Func<IQueryable<TSource>, Expression<Func<TSource, bool>>, long>(Queryable.LongCount).GetMethodInfo();
        var expression = Expression.Call(null, method, queryable.Expression, Expression.Quote(predicate));

        var provider = queryable.ToQueryableInternal().Provider;
        return await provider.ExecuteSingleAsync<long>(expression).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the first element of a sequence.
    /// </summary>
    /// <param name="queryable">Query.</param>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains the first element in the input sequence.</returns>
    [DynamicDependency("First`1", typeof(Queryable))]
    public static async Task<TSource> FirstAsync<TSource>(this IQueryable<TSource> queryable)
    {
        IgniteArgumentCheck.NotNull(queryable);

        var method = new Func<IQueryable<TSource>, TSource>(Queryable.First).GetMethodInfo();
        var expression = Expression.Call(null, method, queryable.Expression);

        var provider = queryable.ToQueryableInternal().Provider;
        return await provider.ExecuteSingleAsync<TSource>(expression).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the first element of a sequence.
    /// </summary>
    /// <param name="queryable">Query.</param>
    /// <param name="predicate">Predicate.</param>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains the first element in the input sequence.</returns>
    [DynamicDependency("First`1", typeof(Queryable))]
    public static async Task<TSource> FirstAsync<TSource>(this IQueryable<TSource> queryable, Expression<Func<TSource, bool>> predicate)
    {
        IgniteArgumentCheck.NotNull(queryable);

        var method = new Func<IQueryable<TSource>, Expression<Func<TSource, bool>>, TSource>(Queryable.First).GetMethodInfo();
        var expression = Expression.Call(null, method, queryable.Expression, Expression.Quote(predicate));

        var provider = queryable.ToQueryableInternal().Provider;
        return await provider.ExecuteSingleAsync<TSource>(expression).ConfigureAwait(false);
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
    [DynamicDependency("FirstOrDefault`1", typeof(Queryable))]
    public static async Task<TSource?> FirstOrDefaultAsync<TSource>(this IQueryable<TSource> queryable)
    {
        IgniteArgumentCheck.NotNull(queryable);

        var method = new Func<IQueryable<TSource>, TSource?>(Queryable.FirstOrDefault).GetMethodInfo();
        var expression = Expression.Call(null, method, queryable.Expression);

        var provider = queryable.ToQueryableInternal().Provider;
        return await provider.ExecuteSingleOrDefaultAsync<TSource>(expression).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the first element of a sequence, or a default value if the sequence contains no elements.
    /// </summary>
    /// <param name="queryable">Query.</param>
    /// <param name="predicate">Predicate.</param>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains <see langword="default" /> ( <typeparamref name="TSource" /> ) if
    /// <paramref name="queryable" /> is empty; otherwise, the first element in <paramref name="queryable" />.
    /// </returns>
    [DynamicDependency("FirstOrDefault`1", typeof(Queryable))]
    public static async Task<TSource?> FirstOrDefaultAsync<TSource>(
        this IQueryable<TSource> queryable,
        Expression<Func<TSource, bool>> predicate)
    {
        IgniteArgumentCheck.NotNull(queryable);

        var method = new Func<IQueryable<TSource>, Expression<Func<TSource, bool>>, TSource?>(Queryable.FirstOrDefault).GetMethodInfo();
        var expression = Expression.Call(null, method, queryable.Expression, Expression.Quote(predicate));

        var provider = queryable.ToQueryableInternal().Provider;
        return await provider.ExecuteSingleOrDefaultAsync<TSource>(expression).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the only element of a sequence, and throws an exception if there is not exactly one element in the sequence.
    /// </summary>
    /// <param name="queryable">Query.</param>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains the single element of the input sequence.
    /// </returns>
    [DynamicDependency("Single`1", typeof(Queryable))]
    public static async Task<TSource> SingleAsync<TSource>(this IQueryable<TSource> queryable)
    {
        IgniteArgumentCheck.NotNull(queryable);

        var method = new Func<IQueryable<TSource>, TSource>(Queryable.Single).GetMethodInfo();
        var expression = Expression.Call(null, method, queryable.Expression);

        var provider = queryable.ToQueryableInternal().Provider;
        return await provider.ExecuteSingleAsync<TSource>(expression).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the only element of a sequence, and throws an exception if there is not exactly one element in the sequence.
    /// </summary>
    /// <param name="queryable">Query.</param>
    /// <param name="predicate">Predicate.</param>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains the single element of the input sequence.
    /// </returns>
    [DynamicDependency("Single`1", typeof(Queryable))]
    public static async Task<TSource> SingleAsync<TSource>(this IQueryable<TSource> queryable, Expression<Func<TSource, bool>> predicate)
    {
        IgniteArgumentCheck.NotNull(queryable);

        var method = new Func<IQueryable<TSource>, Expression<Func<TSource, bool>>, TSource>(Queryable.Single).GetMethodInfo();
        var expression = Expression.Call(null, method, queryable.Expression, Expression.Quote(predicate));

        var provider = queryable.ToQueryableInternal().Provider;
        return await provider.ExecuteSingleAsync<TSource>(expression).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the only element of a sequence, or a default value if the sequence is empty;
    /// throws an exception if there is more than one element in the sequence.
    /// </summary>
    /// <param name="queryable">Query.</param>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains the single element of the input sequence, or <see langword="default" /> (<typeparamref name="TSource" />)
    /// if the sequence contains no elements.
    /// </returns>
    [DynamicDependency("SingleOrDefault`1", typeof(Queryable))]
    public static async Task<TSource?> SingleOrDefaultAsync<TSource>(this IQueryable<TSource> queryable)
    {
        IgniteArgumentCheck.NotNull(queryable);

        var method = new Func<IQueryable<TSource>, TSource?>(Queryable.SingleOrDefault).GetMethodInfo();
        var expression = Expression.Call(null, method, queryable.Expression);

        var provider = queryable.ToQueryableInternal().Provider;
        return await provider.ExecuteSingleOrDefaultAsync<TSource>(expression).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the only element of a sequence, or a default value if the sequence is empty;
    /// throws an exception if there is more than one element in the sequence.
    /// </summary>
    /// <param name="queryable">Query.</param>
    /// <param name="predicate">Predicate.</param>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains the single element of the input sequence, or <see langword="default" /> (<typeparamref name="TSource" />)
    /// if the sequence contains no elements.
    /// </returns>
    [DynamicDependency("SingleOrDefault`1", typeof(Queryable))]
    public static async Task<TSource?> SingleOrDefaultAsync<TSource>(
        this IQueryable<TSource> queryable,
        Expression<Func<TSource, bool>> predicate)
    {
        IgniteArgumentCheck.NotNull(queryable);

        var method = new Func<IQueryable<TSource>, Expression<Func<TSource, bool>>, TSource?>(Queryable.SingleOrDefault).GetMethodInfo();
        var expression = Expression.Call(null, method, queryable.Expression, Expression.Quote(predicate));

        var provider = queryable.ToQueryableInternal().Provider;
        return await provider.ExecuteSingleOrDefaultAsync<TSource>(expression).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the minimum value of a sequence.
    /// </summary>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <param name="queryable">Query.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains the minimum value in the sequence.
    /// </returns>
    [DynamicDependency("Min`1", typeof(Queryable))]
    public static async Task<TSource> MinAsync<TSource>(this IQueryable<TSource> queryable)
    {
        IgniteArgumentCheck.NotNull(queryable);

        var method = new Func<IQueryable<TSource>, TSource?>(Queryable.Min).GetMethodInfo();
        var expression = Expression.Call(null, method, queryable.Expression);

        var provider = queryable.ToQueryableInternal().Provider;
        var res = await provider.ExecuteSingleAsync<TSource?>(expression).ConfigureAwait(false);

        return res!;
    }

    /// <summary>
    /// Returns the minimum value of a sequence.
    /// </summary>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <typeparam name="TResult">The type of the value returned by the function represented by <paramref name="selector" />.</typeparam>
    /// <param name="queryable">Query.</param>
    /// <param name="selector">A projection function to apply to each element.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains the minimum value in the sequence.
    /// </returns>
    [DynamicDependency("Min`2", typeof(Queryable))]
    public static async Task<TResult> MinAsync<TSource, TResult>(
        this IQueryable<TSource> queryable,
        Expression<Func<TSource, TResult>> selector)
    {
        IgniteArgumentCheck.NotNull(queryable);

        var method = new Func<IQueryable<TSource>, Expression<Func<TSource, TResult>>, TResult?>(Queryable.Min).GetMethodInfo();
        var expression = Expression.Call(null, method, queryable.Expression, Expression.Quote(selector));

        var provider = queryable.ToQueryableInternal().Provider;
        return await provider.ExecuteSingleAsync<TResult>(expression).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the maximum value of a sequence.
    /// </summary>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <param name="queryable">Query.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains the maximum value in the sequence.
    /// </returns>
    [DynamicDependency("Max`1", typeof(Queryable))]
    public static async Task<TSource> MaxAsync<TSource>(this IQueryable<TSource> queryable)
    {
        IgniteArgumentCheck.NotNull(queryable);

        var method = new Func<IQueryable<TSource>, TSource?>(Queryable.Max).GetMethodInfo();
        var expression = Expression.Call(null, method, queryable.Expression);

        var provider = queryable.ToQueryableInternal().Provider;
        return await provider.ExecuteSingleAsync<TSource>(expression).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the maximum value of a sequence.
    /// </summary>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <typeparam name="TResult">The type of the value returned by the function represented by <paramref name="selector" />.</typeparam>
    /// <param name="queryable">Query.</param>
    /// <param name="selector">A projection function to apply to each element.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains the maximum value in the sequence.
    /// </returns>
    [DynamicDependency("Max`2", typeof(Queryable))]
    public static async Task<TResult> MaxAsync<TSource, TResult>(
        this IQueryable<TSource> queryable,
        Expression<Func<TSource, TResult>> selector)
    {
        IgniteArgumentCheck.NotNull(queryable);

        var method = new Func<IQueryable<TSource>, Expression<Func<TSource, TResult>>, TResult?>(Queryable.Max).GetMethodInfo();
        var expression = Expression.Call(null, method, queryable.Expression, Expression.Quote(selector));

        var provider = queryable.ToQueryableInternal().Provider;
        return await provider.ExecuteSingleAsync<TResult>(expression).ConfigureAwait(false);
    }

    /// <summary>
    /// Creates a <see cref="List{T}" /> from an <see cref="IQueryable{T}" /> by enumerating it asynchronously.
    /// </summary>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <param name="queryable">Query.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains a <see cref="List{T}" /> that contains elements from the input sequence.
    /// </returns>
    [SuppressMessage("Reliability", "CA2007:Consider calling ConfigureAwait on the awaited task", Justification = "False positive.")]
    public static async Task<List<TSource>> ToListAsync<TSource>(this IQueryable<TSource> queryable)
    {
        // NOTE: ToArrayAsync counterpart is not implemented here, because it is just ToList().ToArray(), which is less efficient.
        IgniteArgumentCheck.NotNull(queryable);

        await using var resultSet = await queryable.ToResultSetAsync().ConfigureAwait(false);

        return await resultSet.ToListAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Creates a <see cref="Dictionary{TK, TV}" /> from an <see cref="IQueryable{T}" /> by enumerating it asynchronously.
    /// </summary>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <typeparam name="TK">Dictionary key type.</typeparam>
    /// <typeparam name="TV">Dictionary value type.</typeparam>
    /// <param name="queryable">Query.</param>
    /// <param name="keySelector">Key selector.</param>
    /// <param name="valSelector">Value selector.</param>
    /// <param name="comparer">Optional comparer.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.
    /// The task result contains a <see cref="Dictionary{TK, TV}" /> that contains elements from the input sequence.
    /// </returns>
    [SuppressMessage("Reliability", "CA2007:Consider calling ConfigureAwait on the awaited task", Justification = "False positive.")]
    public static async Task<Dictionary<TK, TV>> ToDictionaryAsync<TSource, TK, TV>(
        this IQueryable<TSource> queryable,
        Func<TSource, TK> keySelector,
        Func<TSource, TV> valSelector,
        IEqualityComparer<TK>? comparer = null)
        where TK : notnull
    {
        IgniteArgumentCheck.NotNull(queryable);

        await using var resultSet = await queryable.ToResultSetAsync().ConfigureAwait(false);

        return await resultSet.ToDictionaryAsync(keySelector, valSelector, comparer).ConfigureAwait(false);
    }

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
