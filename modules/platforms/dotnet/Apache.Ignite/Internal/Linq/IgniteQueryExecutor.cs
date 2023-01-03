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
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Ignite.Sql;
using Ignite.Transactions;
using Remotion.Linq;
using Sql;

/// <summary>
/// Fields query executor.
/// </summary>
internal sealed class IgniteQueryExecutor : IQueryExecutor
{
    private readonly Sql _sql;
    private readonly ITransaction? _transaction;
    private readonly QueryableOptions? _options;

    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteQueryExecutor" /> class.
    /// </summary>
    /// <param name="sql">SQL.</param>
    /// <param name="transaction">Transaction.</param>
    /// <param name="options">Options.</param>
    public IgniteQueryExecutor(Sql sql, ITransaction? transaction, QueryableOptions? options)
    {
        _sql = sql;
        _transaction = transaction;
        _options = options;
    }

    /// <summary>
    /// Generates <see cref="QueryData"/> from specified <see cref="QueryModel"/>.
    /// </summary>
    /// <param name="queryModel">Query model.</param>
    /// <returns>Query data.</returns>
    public static QueryData GetQueryData(QueryModel queryModel) =>
        new IgniteQueryModelVisitor().GenerateQuery(queryModel);

    /** <inheritdoc /> */
    public T? ExecuteScalar<T>(QueryModel queryModel) =>
        ExecuteSingle<T>(queryModel, false);

    /** <inheritdoc /> */
    public T? ExecuteSingle<T>(QueryModel queryModel, bool returnDefaultWhenEmpty) =>
        ExecuteSingleInternalAsync<T>(
            queryModel,
            returnDefaultWhenEmpty ? ExecutionOptions.ReturnDefaultWhenEmpty : ExecutionOptions.None)
            .GetAwaiter().GetResult();

    /** <inheritdoc /> */
    public IEnumerable<T> ExecuteCollection<T>(QueryModel queryModel)
    {
        // Sync over async lazy enumeration.
        // Users should prefer async APIs - AsAsyncEnumerable, ToListAsync, etc.
        using IResultSet<T> resultSet = ExecuteResultSetInternal<T>(queryModel);
        var enumerator = resultSet.GetAsyncEnumerator();

        try
        {
            while (true)
            {
                ValueTask<bool> moveNextTask = enumerator.MoveNextAsync();
                var moveNextSuccess = moveNextTask.IsCompleted
                    ? moveNextTask.Result
                    : moveNextTask.AsTask().GetAwaiter().GetResult();

                if (!moveNextSuccess)
                {
                    break;
                }

                yield return enumerator.Current;
            }
        }
        finally
        {
            enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }

    /// <summary>
    /// Executes query and returns <see cref="IResultSet{T}"/>.
    /// </summary>
    /// <param name="queryModel">Query model.</param>
    /// <param name="throwNoElementsOnNull">
    /// Whether to throw "Sequence contains no elements" exception when the result is a single null element.</param>
    /// <typeparam name="T">Result type.</typeparam>
    /// <returns>Result set.</returns>
    internal async Task<IResultSet<T>> ExecuteResultSetInternalAsync<T>(QueryModel queryModel, bool throwNoElementsOnNull = false)
    {
        var qryData = GetQueryData(queryModel);

        var statement = new SqlStatement(qryData.QueryText)
        {
            Timeout = _options?.Timeout ?? SqlStatement.DefaultTimeout,
            PageSize = _options?.PageSize ?? SqlStatement.DefaultPageSize
        };

        var selectorOptions = qryData.HasOuterJoins ? ResultSelectorOptions.DefaultIfNull : ResultSelectorOptions.None;

        if (throwNoElementsOnNull)
        {
            selectorOptions |= ResultSelectorOptions.ThrowNoElementsOnNull;
        }

        IResultSet<T> resultSet = await _sql.ExecuteAsyncInternal(
            _transaction,
            statement,
            cols => ResultSelector.Get<T>(cols, queryModel.SelectClause.Selector, selectorOptions),
            qryData.Parameters)
            .ConfigureAwait(false);

        return resultSet;
    }

    /// <summary>
    /// Executes query and returns single result.
    /// </summary>
    /// <param name="queryModel">Query model.</param>
    /// <param name="options">Execution options.</param>
    /// <typeparam name="T">Result type.</typeparam>
    /// <returns>Single result from result set.</returns>
    [SuppressMessage("Reliability", "CA2007:Consider calling ConfigureAwait on the awaited task", Justification = "False positive.")]
    internal async Task<T?> ExecuteSingleInternalAsync<T>(QueryModel queryModel, ExecutionOptions options = default)
    {
        await using IResultSet<T> resultSet = await ExecuteResultSetInternalAsync<T>(
                queryModel,
                throwNoElementsOnNull: options.HasFlag(ExecutionOptions.ThrowNoElementsOnNull))
            .ConfigureAwait(false);

        var res = default(T);
        var count = 0;

        await foreach (var row in resultSet)
        {
            if (count > 0)
            {
                throw new InvalidOperationException(
                    "ResultSet is expected to have one row, but has more: " + GetQueryData(queryModel).QueryText);
            }

            res = row;
            count++;
        }

        if (count == 0 && !options.HasFlag(ExecutionOptions.ReturnDefaultWhenEmpty))
        {
            throw new InvalidOperationException("ResultSet is empty: " + GetQueryData(queryModel).QueryText);
        }

        return res;
    }

    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Disposable is returned.")]
    private IResultSet<T> ExecuteResultSetInternal<T>(QueryModel queryModel) =>
        ExecuteResultSetInternalAsync<T>(queryModel).GetAwaiter().GetResult();
}
