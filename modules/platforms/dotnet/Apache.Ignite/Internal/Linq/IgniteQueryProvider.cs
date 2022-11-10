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

#pragma warning disable SA1615, SA1611, SA1405, SA1202, SA1600 // TODO: Fix warnings.
namespace Apache.Ignite.Internal.Linq;

using System;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Remotion.Linq;
using Remotion.Linq.Clauses.StreamedData;
using Remotion.Linq.Parsing.Structure;
using Remotion.Linq.Utilities;

/// <summary>
/// Query provider for fields queries (projections).
/// </summary>
internal class IgniteQueryProvider : IQueryProvider
{
    /** */
    private static readonly MethodInfo GenericCreateQueryMethod =
        typeof(IgniteQueryProvider).GetMethods().Single(m => m.Name == "CreateQuery" && m.IsGenericMethod);

    /** */
    private readonly IQueryParser _parser;

    /** */
    private readonly IgniteQueryExecutor _executor;

    /** */
    private readonly string _tableName;

    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteQueryProvider"/> class.
    /// </summary>
    public IgniteQueryProvider(
        IQueryParser queryParser,
        IgniteQueryExecutor executor,
        string tableName,
        Type cacheValueType)
    {
        Debug.Assert(queryParser != null);
        Debug.Assert(executor != null);
        Debug.Assert(cacheValueType != null);

        _parser = queryParser;
        _executor = executor;
        _tableName = tableName;
    }

    /// <summary>
    /// Gets the name of the table.
    /// </summary>
    public string TableName
    {
        get { return _tableName; }
    }

    /// <summary>
    /// Gets the executor.
    /// </summary>
    public IgniteQueryExecutor Executor
    {
        get { return _executor; }
    }

    /// <summary>
    /// Generates the query model.
    /// </summary>
    public QueryModel GenerateQueryModel(Expression expression)
    {
        return _parser.GetParsedQuery(expression);
    }

    /** <inheritdoc /> */
    public IQueryable CreateQuery(Expression expression)
    {
        Debug.Assert(expression != null);

        var elementType = GetItemTypeOfClosedGenericIEnumerable(expression.Type, "expression");

        // Slow, but this method is never called during normal LINQ usage with generics.
        // TODO: When is this called?
        return (IQueryable) GenericCreateQueryMethod.MakeGenericMethod(elementType).Invoke(this, new object[] {expression})!;
    }

    /** <inheritdoc /> */
    public IQueryable<T> CreateQuery<T>(Expression expression)
    {
        return new IgniteQueryable<T>(this, expression);
    }

    /** <inheritdoc /> */
    object IQueryProvider.Execute(Expression expression)
    {
        return Execute(expression);
    }

    /** <inheritdoc /> */
    public TResult Execute<TResult>(Expression expression)
    {
        return (TResult) Execute(expression).Value;
    }

    /// <summary>
    /// Gets the item type of closed generic i enumerable.
    /// </summary>
    private static Type GetItemTypeOfClosedGenericIEnumerable(Type enumerableType, string argumentName)
    {
        if (!ItemTypeReflectionUtility.TryGetItemTypeOfClosedGenericIEnumerable(enumerableType, out var itemType))
        {
            var message = "Expected a closed generic type implementing IEnumerable<T>, " + $"but found '{enumerableType}'.";

            throw new ArgumentException(message, argumentName);
        }

        return itemType;
    }

    /// <summary>
    /// Executes the specified expression.
    /// </summary>
    private IStreamedData Execute(Expression expression)
    {
        var model = GenerateQueryModel(expression);

        return model.Execute(_executor);
    }
}
