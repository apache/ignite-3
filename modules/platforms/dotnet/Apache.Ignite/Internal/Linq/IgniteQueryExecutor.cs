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
using System.Globalization;
using System.Linq.Expressions;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Ignite.Sql;
using Ignite.Transactions;
using Proto.BinaryTuple;
using Remotion.Linq;
using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.Expressions;
using Sql;
using Table.Serialization;

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
        ExecuteSingleInternalAsync<T>(queryModel, returnDefaultWhenEmpty).GetAwaiter().GetResult();

    /** <inheritdoc /> */
    public IEnumerable<T> ExecuteCollection<T>(QueryModel queryModel)
    {
        // Sync over async lazy enumeration.
        // Users should prefer async APIs - AsAsyncEnumerable, ToListAsync, etc (TODO IGNITE-18084).
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
        var qryData = GetQueryData(queryModel);

        var statement = new SqlStatement(qryData.QueryText)
        {
            Timeout = _options?.Timeout ?? SqlStatement.DefaultTimeout,
            PageSize = _options?.PageSize ?? SqlStatement.DefaultPageSize
        };

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
        // TODO: IGNITE-18136 Replace reflection with emitted delegates.
        if (selectorExpression is NewExpression newExpr)
        {
            // TODO: Cache compiled delegate based on constructor? Do we care about column types here as well?
            return (IReadOnlyList<IColumnMetadata> cols, ref BinaryTupleReader reader) =>
            {
                var args = new object?[cols.Count];

                for (int i = 0; i < cols.Count; i++)
                {
                    var val = Sql.ReadColumnValue(ref reader, cols[i], i);

                    if (val != null)
                    {
                        val = Convert.ChangeType(val, newExpr.Arguments[i].Type, CultureInfo.InvariantCulture);
                    }

                    args[i] = val;
                }

                return (T)newExpr.Constructor!.Invoke(args);
            };
        }

        if (columns.Count == 1 && typeof(T).ToSqlColumnType() is not null)
        {
            return (IReadOnlyList<IColumnMetadata> cols, ref BinaryTupleReader reader) =>
                (T)Convert.ChangeType(Sql.ReadColumnValue(ref reader, cols[0], 0)!, typeof(T), CultureInfo.InvariantCulture);
        }

        if (typeof(T).GetKeyValuePairTypes() is var (keyType, valType))
        {
            return (IReadOnlyList<IColumnMetadata> cols, ref BinaryTupleReader reader) =>
            {
                var key = FormatterServices.GetUninitializedObject(keyType);
                var val = FormatterServices.GetUninitializedObject(valType);

                for (int i = 0; i < cols.Count; i++)
                {
                    var col = cols[i];
                    var colVal = Sql.ReadColumnValue(ref reader, col, i);

                    SetColumnValue(col, colVal, key, keyType);
                    SetColumnValue(col, colVal, val, valType);
                }

                return (T)Activator.CreateInstance(typeof(T), key, val)!;
            };
        }

        if (selectorExpression is QuerySourceReferenceExpression
            {
                ReferencedQuerySource: IFromClause { FromExpression: SubQueryExpression subQuery }
            })
        {
            // Select everything from a sub-query - use nested selector.
            return GetResultSelector<T>(columns, subQuery.QueryModel.SelectClause.Selector);
        }

        return (IReadOnlyList<IColumnMetadata> cols, ref BinaryTupleReader reader) =>
        {
            var res = (T)FormatterServices.GetUninitializedObject(typeof(T));

            for (int i = 0; i < cols.Count; i++)
            {
                var col = cols[i];
                var val = Sql.ReadColumnValue(ref reader, col, i);

                SetColumnValue(col, val, res, typeof(T));
            }

            return res;
        };
    }

    private static void SetColumnValue<T>(IColumnMetadata col, object? val, T res, Type type)
    {
        if (type.GetFieldByColumnName(col.Name) is {} field)
        {
            if (val != null)
            {
                val = Convert.ChangeType(val, field.FieldType, CultureInfo.InvariantCulture);
            }

            field.SetValue(res, val);
        }
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
                throw new InvalidOperationException(
                    "ResultSet is expected to have one row, but has more: " + GetQueryData(queryModel).QueryText);
            }

            res = row;
            count++;
        }

        if (count == 0 && !returnDefaultWhenEmpty)
        {
            throw new InvalidOperationException("ResultSet is empty: " + GetQueryData(queryModel).QueryText);
        }

        return res;
    }

    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Disposable is returned.")]
    private IResultSet<T> ExecuteResultSetInternal<T>(QueryModel queryModel) =>
        ExecuteResultSetInternalAsync<T>(queryModel).GetAwaiter().GetResult();
}
