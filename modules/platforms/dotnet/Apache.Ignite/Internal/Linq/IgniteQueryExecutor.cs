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

#pragma warning disable SA1615, SA1611, SA1405, SA1202, SA1600, CA2000 // TODO: Fix warnings.
namespace Apache.Ignite.Internal.Linq;

using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Ignite.Sql;
using Ignite.Transactions;
using Remotion.Linq;
using Sql;

/// <summary>
/// Fields query executor.
/// </summary>
internal class IgniteQueryExecutor : IQueryExecutor
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
    public static QueryData GetQueryData(QueryModel queryModel)
    {
        Debug.Assert(queryModel != null);

        return new IgniteQueryModelVisitor().GenerateQuery(queryModel);
    }

    /** <inheritdoc /> */
    public T? ExecuteScalar<T>(QueryModel queryModel)
    {
        return ExecuteSingle<T>(queryModel, false);
    }

    /** <inheritdoc /> */
    public T? ExecuteSingle<T>(QueryModel queryModel, bool returnDefaultWhenEmpty)
    {
        var col = ExecuteCollection<T>(queryModel);

        return returnDefaultWhenEmpty ? col.SingleOrDefault() : col.Single();
    }

    /** <inheritdoc /> */
    public IEnumerable<T> ExecuteCollection<T>(QueryModel queryModel)
    {
        // TODO: Async execution.
        // TODO: Async pagination.
        // TODO: ToResultSetAsync extension will solve all those requirements.
        IResultSet<T> resultSet = ExecuteResultSetAsyncInternal<T>(queryModel).GetAwaiter().GetResult();

        return resultSet.ToListAsync().AsTask().GetAwaiter().GetResult();
    }

    internal async Task<IResultSet<T>> ExecuteResultSetAsyncInternal<T>(QueryModel queryModel)
    {
        var qryData = GetQueryData(queryModel);

        Debug.WriteLine(
            "\nFields Query: {0} | {1}",
            qryData.QueryText,
            string.Join(", ", qryData.Parameters.Select(x => x == null ? "null" : x.ToString())));

        var statement = new SqlStatement(qryData.QueryText);

        IResultSet<T> resultSet = await _sql.ExecuteAsyncInternal<T>(
            _transaction,
            statement,
            _ => (cols, reader) => (T)Sql.ReadColumnValue(ref reader, cols[0], 0)!,
            qryData.Parameters)
            .ConfigureAwait(false);

        return resultSet;
    }
}
