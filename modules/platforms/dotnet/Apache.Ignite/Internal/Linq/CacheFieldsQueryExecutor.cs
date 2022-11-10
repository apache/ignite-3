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
using Ignite.Sql;
using Ignite.Table;
using Proto;
using Remotion.Linq;
using Sql;
using Table;

/// <summary>
/// Fields query executor.
/// </summary>
internal class CacheFieldsQueryExecutor : IQueryExecutor
{
    /** */
    private readonly Table _table;

    /** */
    private readonly Sql _sql;

    /// <summary>
    /// Initializes a new instance of the <see cref="CacheFieldsQueryExecutor" /> class.
    /// </summary>
    /// <param name="table">Table.</param>
    /// <param name="sql">SQL.</param>
    public CacheFieldsQueryExecutor(Table table, Sql sql)
    {
        _table = table;
        _sql = sql;
    }

    /// <summary>
    /// Generates <see cref="QueryData"/> from specified <see cref="QueryModel"/>.
    /// </summary>
    public static QueryData GetQueryData(QueryModel queryModel)
    {
        Debug.Assert(queryModel != null);

        return new CacheQueryModelVisitor().GenerateQuery(queryModel);
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
        var qryData = GetQueryData(queryModel);

        Debug.WriteLine(
            "\nFields Query: {0} | {1}",
            qryData.QueryText,
            string.Join(", ", qryData.Parameters.Select(x => x == null ? "null" : x.ToString())));

        var statement = new SqlStatement(qryData.QueryText);

        // TODO: Async execution.
        // TODO: Async pagination.
        // TODO: ToResultSetAsync extension will solve all those requirements.
        IResultSet<T> resultSet = _sql.ExecuteAsyncInternal<T>(
            null,
            statement,
            _ => (cols, reader) => (T)Sql.ReadColumnValue(ref reader, cols[0], 0)!,
            qryData.Parameters.ToArray()).GetAwaiter().GetResult();

        var rows = resultSet.ToListAsync().AsTask().GetAwaiter().GetResult();

        // TODO: Compile selector according to result schema and select expression
        // TODO: Generify ResultSet
        var selector = queryModel.SelectClause.Selector;
        return rows;
    }
}
