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

using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.Expressions;
using Remotion.Linq.Clauses.ResultOperators;

/// <summary>
/// Alias dictionary.
/// </summary>
[SuppressMessage("Naming", "CA1711:Identifiers should not have incorrect suffix", Justification = "Reviewed.")]
internal sealed class AliasDictionary
{
    /** */
    private readonly Dictionary<Expression, string> _fieldAliases = new();

    /** */
    private readonly Dictionary<GroupResultOperator, string> _groupByAliases = new();

    /** */
    private readonly Stack<Dictionary<IQuerySource, string>> _stack = new();

    /** */
    private int _tableAliasIndex;

    /** */
    private Dictionary<IQuerySource, string> _tableAliases = new();

    /** */
    private int _fieldAliasIndex;

    /// <summary>
    /// Pushes current aliases to stack.
    /// </summary>
    /// <param name="copyAliases">Flag indicating that current aliases should be copied.</param>
    public void Push(bool copyAliases)
    {
        _stack.Push(_tableAliases);

        _tableAliases = copyAliases
            ? _tableAliases.ToDictionary(p => p.Key, p => p.Value)
            : new Dictionary<IQuerySource, string>();
    }

    /// <summary>
    /// Pops current aliases from stack.
    /// </summary>
    public void Pop()
    {
        _tableAliases = _stack.Pop();
    }

    /// <summary>
    /// Gets the table alias.
    /// </summary>
    /// <param name="expression">Expression.</param>
    /// <returns>Table alias.</returns>
    public string GetTableAlias(Expression expression) =>
        GetTableAlias(ExpressionWalker.GetQuerySource(expression)!);

    /// <summary>
    /// Gets the table alias.
    /// </summary>
    /// <param name="fromClause">FROM clause.</param>
    /// <returns>Table alias.</returns>
    public string GetTableAlias(IFromClause fromClause) =>
        GetTableAlias(ExpressionWalker.GetQuerySource(fromClause.FromExpression) ?? fromClause);

    /// <summary>
    /// Gets the table alias.
    /// </summary>
    /// <param name="joinClause">JOIN clause.</param>
    /// <returns>Table alias.</returns>
    public string GetTableAlias(JoinClause joinClause) =>
        GetTableAlias(ExpressionWalker.GetQuerySource(joinClause.InnerSequence) ?? joinClause);

    /// <summary>
    /// Gets the field alias.
    /// </summary>
    /// <param name="expression">Expression.</param>
    /// <returns>Alias.</returns>
    public string GetFieldAlias(Expression expression)
    {
        var referenceExpression = ExpressionWalker.GetQuerySourceReference(expression)!;

        return GetFieldAlias(referenceExpression);
    }

    /// <summary>
    /// Gets or creates the GROUP BY member alias.
    /// </summary>
    /// <param name="op">Group operator.</param>
    /// <returns>Alias.</returns>
    public (string Alias, bool Created) GetOrCreateGroupByMemberAlias(GroupResultOperator op)
    {
        if (_groupByAliases.TryGetValue(op, out var alias))
        {
            return (alias, false);
        }

        alias = "_G" + _groupByAliases.Count;

        _groupByAliases[op] = alias;

        return (alias, true);
    }

    /// <summary>
    /// Appends as clause.
    /// </summary>
    /// <param name="builder">Builder.</param>
    /// <param name="clause">Clause.</param>
    public void AppendAsClause(StringBuilder builder, IFromClause clause)
    {
        var queryable = ExpressionWalker.GetIgniteQueryable(clause)!;
        var tableName = ExpressionWalker.GetTableNameWithSchema(queryable);

        builder.AppendFormat(CultureInfo.InvariantCulture, "{0} as {1}", tableName, GetTableAlias(clause));
    }

    /// <summary>
    /// Gets the table alias.
    /// </summary>
    /// <returns>Table alias.</returns>
    private string GetTableAlias(IQuerySource querySource)
    {
        if (!_tableAliases.TryGetValue(querySource, out var alias))
        {
            alias = "_T" + _tableAliasIndex++;

            _tableAliases[querySource] = alias;
        }

        return alias;
    }

    /// <summary>
    /// Gets the fields alias.
    /// </summary>
    private string GetFieldAlias(QuerySourceReferenceExpression querySource)
    {
        if (!_fieldAliases.TryGetValue(querySource, out var alias))
        {
            alias = "F" + _fieldAliasIndex++;

            _fieldAliases[querySource] = alias;
        }

        return alias;
    }
}
