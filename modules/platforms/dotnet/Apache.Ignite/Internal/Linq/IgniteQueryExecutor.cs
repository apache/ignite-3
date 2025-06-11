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
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Common;
using Ignite.Sql;
using Ignite.Transactions;
using Microsoft.Extensions.Logging;
using Remotion.Linq;
using Remotion.Linq.Clauses.ResultOperators;
using Sql;

/// <summary>
/// Fields query executor.
/// </summary>
internal sealed class IgniteQueryExecutor : IQueryExecutor
{
    private readonly Sql _sql;
    private readonly ITransaction? _transaction;
    private readonly QueryableOptions? _options;
    private readonly ILogger _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteQueryExecutor" /> class.
    /// </summary>
    /// <param name="sql">SQL.</param>
    /// <param name="transaction">Transaction.</param>
    /// <param name="options">Options.</param>
    /// <param name="configuration">Configuration.</param>
    public IgniteQueryExecutor(Sql sql, ITransaction? transaction, QueryableOptions? options, IgniteClientConfiguration configuration)
    {
        _sql = sql;
        _transaction = transaction;
        _options = options;
        _logger = configuration.LoggerFactory.CreateLogger<IgniteQueryExecutor>();
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
    /// <typeparam name="T">Result type.</typeparam>
    /// <returns>Result set.</returns>
    internal async Task<IResultSet<T>> ExecuteResultSetInternalAsync<T>(QueryModel queryModel)
    {
        var queryData = GetQueryData(queryModel);

        var statement = new SqlStatement(queryData.QueryText)
        {
            Timeout = _options?.Timeout ?? SqlStatement.DefaultTimeout,
            PageSize = _options?.PageSize ?? SqlStatement.DefaultPageSize
        };

        var selectorOptions = GetResultSelectorOptions(queryModel, queryData.HasOuterJoins);

        // Explicit enabled check because of StringJoin.
        if (_logger.IsEnabled(LogLevel.Debug))
        {
            _logger.LogExecutingSqlStatementDebug(statement, queryData.Parameters.StringJoin());
        }

        IResultSet<T> resultSet = await _sql.ExecuteAsyncInternal(
            _transaction,
            statement,
            cols => ResultSelector.Get<T>(cols, queryModel.SelectClause.Selector, selectorOptions),
            queryData.Parameters,
            CancellationToken.None)
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
        await using IResultSet<T> resultSet = await ExecuteResultSetInternalAsync<T>(queryModel).ConfigureAwait(false);

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

        if (count == 0 && options != ExecutionOptions.ReturnDefaultWhenEmpty)
        {
            throw new InvalidOperationException("ResultSet is empty: " + GetQueryData(queryModel).QueryText);
        }

        return res;
    }

    /// <summary>
    /// Executes a non-query statement (DML).
    /// </summary>
    /// <param name="queryModel">Query model.</param>
    /// <returns>The number of rows affected.</returns>
    [SuppressMessage("Reliability", "CA2007:Consider calling ConfigureAwait on the awaited task", Justification = "False positive.")]
    internal async Task<long> ExecuteNonQueryAsync(QueryModel queryModel)
    {
        await using IResultSet<object> resultSet = await ExecuteResultSetInternalAsync<object>(queryModel).ConfigureAwait(false);

        Debug.Assert(!resultSet.HasRowSet, "!resultSet.HasRowSet");
        Debug.Assert(resultSet.AffectedRows >= 0, "resultSet.AffectedRows >= 0");

        return resultSet.AffectedRows;
    }

    private static ResultSelectorOptions GetResultSelectorOptions(QueryModel model, bool hasOuterJoins)
    {
        foreach (var op in model.ResultOperators)
        {
            if (op is MinResultOperator or MaxResultOperator or AverageResultOperator)
            {
                // SQL MIN/MAX/AVG return null when there are no rows in the table,
                // but LINQ Min/Max/Average throw <see cref="InvalidOperationException"/> in this case.
                return ResultSelectorOptions.ThrowNoElementsIfNull;
            }

            if (op is SumResultOperator)
            {
                // SQL SUM returns null when there are no rows in the table,
                // but LINQ Sum returns 0.
                return ResultSelectorOptions.ReturnZeroIfNull;
            }
        }

        return hasOuterJoins ? ResultSelectorOptions.ReturnDefaultIfNull : ResultSelectorOptions.None;
    }

    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Disposable is returned.")]
    private IResultSet<T> ExecuteResultSetInternal<T>(QueryModel queryModel) =>
        ExecuteResultSetInternalAsync<T>(queryModel).GetAwaiter().GetResult();
}
