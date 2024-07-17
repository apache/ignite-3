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

using System.Linq;
using System.Linq.Expressions;
using Common;
using Remotion.Linq;

/// <summary>
/// Ignite queryable.
/// </summary>
/// <typeparam name="T">Query result type.</typeparam>
internal sealed class IgniteQueryable<T>
    : QueryableBase<T>, IIgniteQueryableInternal
{
    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteQueryable{T}"/> class.
    /// </summary>
    /// <param name="provider">Provider.</param>
    public IgniteQueryable(IQueryProvider provider)
        : base(provider)
    {
        // No-op.
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteQueryable{T}"/> class.
    /// </summary>
    /// <param name="provider">Provider.</param>
    /// <param name="expression">Expression.</param>
    public IgniteQueryable(IQueryProvider provider, Expression expression)
        : base(provider, expression)
    {
        // No-op.
    }

    /// <inheritdoc/>
    public string TableName => IgniteQueryProvider.TableName;

    /// <inheritdoc/>
    IgniteQueryProvider IIgniteQueryableInternal.Provider => IgniteQueryProvider;

    /// <summary>
    /// Gets the query provider.
    /// </summary>
    private IgniteQueryProvider IgniteQueryProvider => (IgniteQueryProvider)Provider;

    /// <inheritdoc/>
    public QueryData GetQueryData()
    {
        var model = GetQueryModel();

        return IgniteQueryExecutor.GetQueryData(model);
    }

    /// <summary>
    /// Gets the query model.
    /// </summary>
    /// <returns>Query model.</returns>
    public QueryModel GetQueryModel() => IgniteQueryProvider.GenerateQueryModel(Expression);

    /// <summary>
    /// Returns a <see cref="string" /> that represents this instance.
    /// </summary>
    /// <returns>
    /// A <see cref="string" /> that represents this instance.
    /// </returns>
    public override string ToString()
    {
        var queryData = GetQueryData();

        return new IgniteToStringBuilder(GetType())
            .Append(queryData.QueryText, "Query")
            .AppendList(queryData.Parameters, "Parameters")
            .Build();
    }
}
