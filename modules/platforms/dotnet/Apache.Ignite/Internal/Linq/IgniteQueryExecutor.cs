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
using System.Linq.Expressions;
using System.Threading.Tasks;
using Ignite.Sql;
using Ignite.Transactions;
using Proto.BinaryTuple;
using Remotion.Linq;
using Sql;

/// <summary>
/// Fields query executor.
/// </summary>
internal sealed class IgniteQueryExecutor : IQueryExecutor
{
    private readonly Sql _sql;

    private readonly ITransaction? _transaction;

    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteQueryExecutor" /> class.
    /// </summary>
    /// <param name="sql">SQL.</param>
    /// <param name="transaction">Transaction.</param>
    public IgniteQueryExecutor(Sql sql, ITransaction? transaction)
    {
        _sql = sql;
        _transaction = transaction;
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
        ExecuteSingleInternalAsync<T>(queryModel, returnDefaultWhenEmpty).GetAwaiter().GetResult();

    /** <inheritdoc /> */
    public IEnumerable<T> ExecuteCollection<T>(QueryModel queryModel)
    {
        // TODO: Log a warning about sync-over-async with recommendation to use async variant?
        using IResultSet<T> resultSet = ExecuteResultSetInternal<T>(queryModel);

        return resultSet.ToListAsync().AsTask().GetAwaiter().GetResult();
    }

    /// <summary>
    /// Executes query and returns <see cref="IResultSet{T}"/>.
    /// </summary>
    /// <param name="queryModel">Query model.</param>
    /// <typeparam name="T">Result type.</typeparam>
    /// <returns>Result set.</returns>
    internal async Task<IResultSet<T>> ExecuteResultSetInternalAsync<T>(QueryModel queryModel)
    {
        var qryData = GetQueryData(queryModel);
        var statement = new SqlStatement(qryData.QueryText);

        IResultSet<T> resultSet = await _sql.ExecuteAsyncInternal(
            _transaction,
            statement,
            cols => GetResultSelector<T>(cols, queryModel.SelectClause.Selector),
            qryData.Parameters)
            .ConfigureAwait(false);

        return resultSet;
    }

    /// <summary>
    /// Gets the result selector.
    /// </summary>
    private static RowReader<T> GetResultSelector<T>(IReadOnlyList<IColumnMetadata> columns, Expression selectorExpression)
    {
        // var newExpr = selectorExpression as NewExpression;
        //
        // if (newExpr != null)
        //     return GetCompiledCtor<T>(newExpr.Constructor);
        //
        // var entryCtor = GetCacheEntryCtorInfo(typeof(T));
        //
        // if (entryCtor != null)
        //     return GetCompiledCtor<T>(entryCtor);
        return (IReadOnlyList<IColumnMetadata> cols, ref BinaryTupleReader reader) => (T)Sql.ReadColumnValue(ref reader, cols[0], 0)!;
    }

    [SuppressMessage("Reliability", "CA2007:Consider calling ConfigureAwait on the awaited task", Justification = "False positive.")]
    private async Task<T?> ExecuteSingleInternalAsync<T>(QueryModel queryModel, bool returnDefaultWhenEmpty)
    {
        await using IResultSet<T> resultSet = await ExecuteResultSetInternalAsync<T>(queryModel).ConfigureAwait(false);

        var res = default(T);
        var count = 0;

        await foreach (var row in resultSet)
        {
            if (count > 0)
            {
                throw new InvalidOperationException("ResultSet has more than one element: " + queryModel);
            }

            res = row;
            count++;
        }

        if (count == 0 && !returnDefaultWhenEmpty)
        {
            throw new InvalidOperationException("ResultSet is empty: " + queryModel);
        }

        return res;
    }

    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Disposable is returned.")]
    private IResultSet<T> ExecuteResultSetInternal<T>(QueryModel queryModel) =>
        ExecuteResultSetInternalAsync<T>(queryModel).GetAwaiter().GetResult();
}
