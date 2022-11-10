﻿/*
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

#pragma warning disable SA1615, SA1611, SA1405, SA1202, SA1600 // TODO: Fix warnings.
namespace Apache.Ignite.Internal.Linq;

using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Remotion.Linq;

/// <summary>
/// Base class for cache queryables.
/// </summary>
/// <typeparam name="T">Query result type.</typeparam>
internal abstract class CacheQueryableBase<T>
    : QueryableBase<T>, ICacheQueryableInternal
{
    /// <summary>
    /// Initializes a new instance of the <see cref="CacheQueryableBase{T}"/> class.
    /// </summary>
    /// <param name="provider">Provider.</param>
    protected CacheQueryableBase(IQueryProvider provider)
        : base(provider)
    {
        // No-op.
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="CacheQueryableBase{T}"/> class.
    /// </summary>
    /// <param name="provider">Provider.</param>
    /// <param name="expression">Expression.</param>
    protected CacheQueryableBase(IQueryProvider provider, Expression expression)
        : base(provider, expression)
    {
        // No-op.
    }

    public string TableName => CacheQueryProvider.TableName;

    CacheFieldsQueryProvider ICacheQueryableInternal.Provider => CacheQueryProvider;

    /// <summary>
    /// Gets the cache query provider.
    /// </summary>
    private CacheFieldsQueryProvider CacheQueryProvider => (CacheFieldsQueryProvider)Provider;

    /// <summary>
    /// Gets the query model.
    /// </summary>
    /// <returns>Query model.</returns>
    public QueryModel GetQueryModel() => CacheQueryProvider.GenerateQueryModel(Expression);

    /// <summary>
    /// Returns a <see cref="string" /> that represents this instance.
    /// </summary>
    /// <returns>
    /// A <see cref="string" /> that represents this instance.
    /// </returns>
    public override string ToString()
    {
        var queryData = GetQueryData();
        var sb = new StringBuilder();

        sb.Append(GetType().Name);
        sb.Append(" [Query=").Append(queryData.QueryText);

        sb.Append(", Parameters=");
        var count = 0;

        foreach (var parameter in queryData.Parameters)
        {
            if (count > 0)
            {
                sb.Append(", ");
            }

            sb.Append(parameter);

            count++;
        }

        sb.Append(']');

        return queryData.QueryText;
    }

    /// <summary>
    /// Gets the query data.
    /// </summary>
    /// <returns>Query data.</returns>
    private QueryData GetQueryData()
    {
        var model = GetQueryModel();

        return CacheFieldsQueryExecutor.GetQueryData(model);
    }
}
