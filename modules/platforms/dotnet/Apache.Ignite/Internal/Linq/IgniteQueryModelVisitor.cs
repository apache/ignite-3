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
using System.Collections.ObjectModel;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Common;
using Remotion.Linq;
using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.Expressions;
using Remotion.Linq.Clauses.ResultOperators;

/// <summary>
/// Query visitor, transforms LINQ expression to SQL.
/// </summary>
internal sealed class IgniteQueryModelVisitor : QueryModelVisitorBase
{
    /** */
    private readonly StringBuilder _builder = new();

    /** */
    [SuppressMessage("Microsoft.Design", "CA1002:DoNotExposeGenericLists", Justification = "Private.")]
    private readonly List<object?> _parameters = new();

    /** */
    private readonly AliasDictionary _aliases = new();

    /// <summary>
    /// Gets the builder.
    /// </summary>
    public StringBuilder Builder => _builder;

    /// <summary>
    /// Gets the parameters.
    /// </summary>
    public IList<object?> Parameters => _parameters;

    /// <summary>
    /// Gets the aliases.
    /// </summary>
    public AliasDictionary Aliases => _aliases;

    /// <summary>
    /// Generates the query.
    /// </summary>
    /// <param name="queryModel">Query model.</param>
    /// <returns>Query data.</returns>
    public QueryData GenerateQuery(QueryModel queryModel)
    {
        VisitQueryModel(queryModel);

        var qryText = _builder.TrimEnd().ToString();

        return new QueryData(qryText, _parameters);
    }

    /** <inheritdoc /> */
    public override void VisitQueryModel(QueryModel queryModel)
    {
        VisitQueryModel(queryModel, includeAllFields: queryModel.MainFromClause.FromExpression is SubQueryExpression);
    }

    /** <inheritdoc /> */
    public override void VisitWhereClause(WhereClause whereClause, QueryModel queryModel, int index)
    {
        base.VisitWhereClause(whereClause, queryModel, index);

        VisitWhereClause(whereClause, index, false);
    }

    /** <inheritdoc /> */
    public override void VisitOrderByClause(OrderByClause orderByClause, QueryModel queryModel, int index)
    {
        base.VisitOrderByClause(orderByClause, queryModel, index);

        _builder.Append("order by ");

        for (int i = 0; i < orderByClause.Orderings.Count; i++)
        {
            var ordering = orderByClause.Orderings[i];

            if (i > 0)
            {
                _builder.Append(", ");
            }

            _builder.Append('(');

            BuildSqlExpression(ordering.Expression);

            _builder.TrimEnd().Append(')');

            _builder.Append(ordering.OrderingDirection == OrderingDirection.Asc ? " asc" : " desc");
        }

        _builder.Append(' ');
    }

    /** <inheritdoc /> */
    public override void VisitJoinClause(JoinClause joinClause, QueryModel queryModel, int index)
    {
        base.VisitJoinClause(joinClause, queryModel, index);
        var queryable = ExpressionWalker.GetIgniteQueryable(joinClause, false);

        if (queryable != null)
        {
            var subQuery = joinClause.InnerSequence as SubQueryExpression;

            if (subQuery != null)
            {
                var isOuter = subQuery.QueryModel.ResultOperators.OfType<DefaultIfEmptyResultOperator>().Any();

                _builder.AppendFormat(CultureInfo.InvariantCulture, "{0} join (", isOuter ? "left outer" : "inner");

                VisitQueryModel(subQuery.QueryModel, true);

                var alias = _aliases.GetTableAlias(subQuery.QueryModel.MainFromClause);
                _builder.AppendFormat(CultureInfo.InvariantCulture, ") as {0} on (", alias);
            }
            else
            {
                var tableName = ExpressionWalker.GetTableNameWithSchema(queryable);
                var alias = _aliases.GetTableAlias(joinClause);
                _builder.AppendFormat(CultureInfo.InvariantCulture, "inner join {0} as {1} on (", tableName, alias);
            }
        }
        else
        {
            VisitJoinWithLocalCollectionClause(joinClause);
        }

        BuildJoinCondition(joinClause.InnerKeySelector, joinClause.OuterKeySelector);

        _builder.Append(") ");
    }

    /** <inheritdoc /> */
    public override void VisitAdditionalFromClause(AdditionalFromClause fromClause, QueryModel queryModel, int index)
    {
        base.VisitAdditionalFromClause(fromClause, queryModel, index);

        var subQuery = fromClause.FromExpression as SubQueryExpression;
        if (subQuery != null)
        {
            _builder.Append('(');

            VisitQueryModel(subQuery.QueryModel, true);

            var alias = _aliases.GetTableAlias(subQuery.QueryModel.MainFromClause);
            _builder.AppendFormat(CultureInfo.InvariantCulture, ") as {0} ", alias);
        }
        else
        {
            _aliases.AppendAsClause(_builder, fromClause);
            _builder.Append(' ');
        }
    }

    /** <inheritdoc /> */
    public override void VisitMainFromClause(MainFromClause fromClause, QueryModel queryModel)
    {
        // GROUP BY is handled separately in ProcessGroupings and does not need to be handled here as SubQuery.
        if (fromClause.FromExpression is SubQueryExpression subQuery &&
            subQuery.QueryModel.ResultOperators.All(x => x is not GroupResultOperator))
        {
            _builder.Append("from (");

            VisitQueryModel(subQuery.QueryModel);

            _builder.TrimEnd()
                .Append(") as ")
                .Append(_aliases.GetTableAlias(subQuery.QueryModel.MainFromClause))
                .Append(' ');

            return;
        }

        // TODO: IGNITE-18137 DML: queryModel.ResultOperators.LastOrDefault() is UpdateAllResultOperator;
        var isUpdateQuery = false;
        if (!isUpdateQuery)
        {
            _builder.Append("from ");
        }

        ValidateFromClause(fromClause);
        _aliases.AppendAsClause(_builder, fromClause);
        _builder.Append(' ');

        var i = 0;
        foreach (var additionalFrom in queryModel.BodyClauses.OfType<AdditionalFromClause>())
        {
            _builder.Append(", ");
            ValidateFromClause(additionalFrom);

            VisitAdditionalFromClause(additionalFrom, queryModel, i++);
        }

        if (isUpdateQuery)
        {
            // TODO: IGNITE-18137 DML
            // BuildSetClauseForUpdateAll(queryModel);
        }
    }

    /// <summary>
    /// Visits the query model.
    /// </summary>
    /// <param name="queryModel">Query model.</param>
    /// <param name="includeAllFields">Whether to select all table fields.</param>
    /// <param name="copyAliases">Whether to copy aliases.</param>
    public void VisitQueryModel(QueryModel queryModel, bool includeAllFields, bool copyAliases = false)
    {
        _aliases.Push(copyAliases);

        // TODO: IGNITE-18137 DML
        // var lastResultOp = queryModel.ResultOperators.LastOrDefault();
        // if (lastResultOp is RemoveAllResultOperator)
        // {
        //     VisitRemoveOperator(queryModel);
        // }
        // else if (lastResultOp is UpdateAllResultOperator)
        // {
        //     VisitUpdateAllOperator(queryModel);
        // }
        // else
        if (queryModel.ResultOperators.Count == 1 && queryModel.ResultOperators[0] is AnyResultOperator or AllResultOperator)
        {
            // All is different from Any: it always has a predicate inside.
            // We use NOT EXISTS with reverted predicate to implement All.
            // Reverted predicate is added in VisitBodyClauses.
            _builder.Append(queryModel.ResultOperators[0] is AllResultOperator
                ? "select not exists (select 1 "
                : "select exists (select 1 ");

            // FROM ... WHERE ... JOIN ...
            base.VisitQueryModel(queryModel);

            // UNION ...
            ProcessResultOperatorsEnd(queryModel);

            _builder.TrimEnd().Append(')');
        }
        else
        {
            // SELECT
            _builder.Append("select ");

            // FLD1, FLD2
            VisitSelectors(queryModel, includeAllFields);

            // FROM ... WHERE ... JOIN ...
            base.VisitQueryModel(queryModel);

            // UNION ...
            ProcessResultOperatorsEnd(queryModel);
        }

        _aliases.Pop();
    }

    /// <summary>
    /// Visits selectors.
    /// </summary>
    /// <param name="queryModel">Query model.</param>
    /// <param name="includeAllFields">Whether to include all fields in the selector.</param>
    public void VisitSelectors(QueryModel queryModel, bool includeAllFields)
    {
        var parenCount = ProcessResultOperatorsBegin(queryModel);

        // FIELD1, FIELD2
        BuildSqlExpression(queryModel.SelectClause.Selector, parenCount > 0, includeAllFields);
        _builder.TrimEnd().Append(')', parenCount).Append(' ');
    }

    /** <inheritdoc /> */
    protected override void VisitBodyClauses(ObservableCollection<IBodyClause> bodyClauses, QueryModel queryModel)
    {
        var i = 0;
        foreach (var join in bodyClauses.OfType<JoinClause>())
        {
            VisitJoinClause(join, queryModel, i++);
        }

        var hasGroups = ProcessGroupings(queryModel);

        i = 0;
        foreach (var where in bodyClauses.OfType<WhereClause>())
        {
            VisitWhereClause(where, i++, hasGroups);
        }

        if (queryModel.ResultOperators.Count == 1 && queryModel.ResultOperators[0] is AllResultOperator allOp)
        {
            _builder.Append(i > 0
                ? "and not "
                : "where not ");

            BuildSqlExpression(allOp.Predicate);
        }

        i = 0;
        foreach (var orderBy in bodyClauses.OfType<OrderByClause>())
        {
            VisitOrderByClause(orderBy, queryModel, i++);
        }
    }

    /// <summary>
    /// Validates from clause.
    /// </summary>
    // ReSharper disable once UnusedParameter.Local
    private static void ValidateFromClause(IFromClause clause)
    {
        // Only IQueryable can be used in FROM clause. IEnumerable is not supported.
        if (!typeof(IQueryable).IsAssignableFrom(clause.FromExpression.Type))
        {
            throw new NotSupportedException("FROM clause must be IQueryable: " + clause);
        }
    }

    /// <summary>
    /// Processes the result operators that come right after SELECT: min/max/count/sum/distinct.
    /// </summary>
    private int ProcessResultOperatorsBegin(QueryModel queryModel)
    {
        int parenCount = 0;

        foreach (var op in queryModel.ResultOperators.Reverse())
        {
            if (op is CountResultOperator or LongCountResultOperator)
            {
                _builder.Append("count(");
                parenCount++;
            }
            else if (op is SumResultOperator)
            {
                _builder.Append("sum(");
                parenCount++;
            }
            else if (op is MinResultOperator)
            {
                _builder.Append("min(");
                parenCount++;
            }
            else if (op is MaxResultOperator)
            {
                _builder.Append("max(");
                parenCount++;
            }
            else if (op is AverageResultOperator)
            {
                _builder.Append("avg(");
                parenCount++;
            }
            else if (op is DistinctResultOperator)
            {
                _builder.Append("distinct ");
            }
            else if (op is UnionResultOperator or IntersectResultOperator or ExceptResultOperator or DefaultIfEmptyResultOperator or
                     SkipResultOperator or TakeResultOperator or FirstResultOperator or SingleResultOperator)
            {
                // Will be processed later
                break;
            }
            else if (op is ContainsResultOperator)
            {
                // Should be processed already
                break;
            }
            else
            {
                throw new NotSupportedException("Operator is not supported: " + op);
            }
        }

        return parenCount;
    }

    /// <summary>
    /// Processes the result operators that go in the end of the query: limit/offset/union/intersect/except.
    /// </summary>
    private void ProcessResultOperatorsEnd(QueryModel queryModel)
    {
        ProcessSkipTake(queryModel);

        for (var i = queryModel.ResultOperators.Count - 1; i >= 0; i--)
        {
            var op = queryModel.ResultOperators[i];
            string? keyword = null;
            Expression? source = null;

            if (op is UnionResultOperator union)
            {
                keyword = "union";
                source = union.Source2;
            }

            if (op is IntersectResultOperator intersect)
            {
                keyword = "intersect";
                source = intersect.Source2;
            }

            if (op is ExceptResultOperator except)
            {
                keyword = "except";
                source = except.Source2;
            }

            if (keyword == null)
            {
                continue;
            }

            _builder.Append(keyword).Append(" (");

            if (source is SubQueryExpression subQuery)
            {
                // Subquery union.
                VisitQueryModel(subQuery.QueryModel);
            }
            else
            {
                // Direct union, source is IIgniteQueryableInternal
                if (source is not ConstantExpression innerExpr)
                {
                    throw new NotSupportedException("Unexpected UNION inner sequence: " + source);
                }

                if (innerExpr.Value is not IIgniteQueryableInternal queryable)
                {
                    throw new NotSupportedException("Unexpected UNION inner sequence " +
                                                    "(only results of cache.ToQueryable() are supported): " +
                                                    innerExpr.Value);
                }

                VisitQueryModel(queryable.GetQueryModel());
            }

            _builder.TrimEnd().Append(')');
        }
    }

    /// <summary>
    /// Processes the pagination (skip/take).
    /// </summary>
    private void ProcessSkipTake(QueryModel queryModel)
    {
        // TODO Single loop
        if (queryModel.ResultOperators.Any(static x => x is FirstResultOperator))
        {
            _builder.Append("limit 1");
            return;
        }

        if (queryModel.ResultOperators.Any(static x => x is SingleResultOperator))
        {
            // Will fail in IgniteQueryExecutor.ExecuteSingleInternalAsync if there is more than 1 row.
            _builder.Append("limit 2");
            return;
        }

        if (queryModel.ResultOperators.OfType<TakeResultOperator>().FirstOrDefault() is { } limit)
        {
            _builder.Append("limit ");
            BuildSqlExpression(limit.Count);
        }

        if (queryModel.ResultOperators.OfType<SkipResultOperator>().FirstOrDefault() is { } offset)
        {
            _builder.AppendWithSpace("offset ");
            BuildSqlExpression(offset.Count);
        }
    }

    /// <summary>
    /// Processes the groupings.
    /// </summary>
    private bool ProcessGroupings(QueryModel queryModel)
    {
        var subQuery = queryModel.MainFromClause.FromExpression as SubQueryExpression;

        if (subQuery == null)
        {
            return false;
        }

        var groupBy = subQuery.QueryModel.ResultOperators.OfType<GroupResultOperator>().FirstOrDefault();

        if (groupBy == null)
        {
            return false;
        }

        // Visit inner joins before grouping
        var i = 0;
        foreach (var join in subQuery.QueryModel.BodyClauses.OfType<JoinClause>())
        {
            VisitJoinClause(join, queryModel, i++);
        }

        i = 0;
        foreach (var where in subQuery.QueryModel.BodyClauses.OfType<WhereClause>())
        {
            VisitWhereClause(where, i++, false);
        }

        // Append grouping
        _builder.Append("group by (");

        BuildSqlExpression(groupBy.KeySelector);

        _builder.Append(") ");

        return true;
    }

    /// <summary>
    /// Visits the where clause.
    /// </summary>
    private void VisitWhereClause(WhereClause whereClause, int index, bool hasGroups)
    {
        _builder.Append(index > 0
            ? "and "
            : hasGroups
                ? "having"
                : "where ");

        BuildSqlExpression(whereClause.Predicate);

        _builder.Append(' ');
    }

    /// <summary>
    /// Visits Join clause in case of join with local collection.
    /// </summary>
    private void VisitJoinWithLocalCollectionClause(JoinClause joinClause)
    {
        // Unlike Ignite 2.x, SQL engine does not support local collection joins.
        throw new NotSupportedException("Local collection joins are not supported, try `.Contains()` instead: " + joinClause);
    }

    /// <summary>
    /// Builds the join condition ('x=y AND foo=bar').
    /// </summary>
    /// <param name="innerKey">The inner key selector.</param>
    /// <param name="outerKey">The outer key selector.</param>
    private void BuildJoinCondition(Expression innerKey, Expression outerKey)
    {
        var innerNew = innerKey as NewExpression;
        var outerNew = outerKey as NewExpression;

        if (innerNew == null && outerNew == null)
        {
            BuildJoinSubCondition(innerKey, outerKey);
            return;
        }

        if (innerNew != null && outerNew != null)
        {
            if (innerNew.Constructor != outerNew.Constructor)
            {
                throw new NotSupportedException(
                    "Unexpected JOIN condition. Multi-key joins should have " +
                    $"the same initializers on both sides: '{innerKey} = {outerKey}'");
            }

            for (var i = 0; i < innerNew.Arguments.Count; i++)
            {
                if (i > 0)
                {
                    _builder.Append(" and ");
                }

                BuildJoinSubCondition(innerNew.Arguments[i], outerNew.Arguments[i]);
            }

            return;
        }

        throw new NotSupportedException(
            "Unexpected JOIN condition. Multi-key joins should have " +
            $"anonymous type instances on both sides: '{innerKey} = {outerKey}'");
    }

    /// <summary>
    /// Builds the join sub condition.
    /// </summary>
    /// <param name="innerKey">The inner key.</param>
    /// <param name="outerKey">The outer key.</param>
    private void BuildJoinSubCondition(Expression innerKey, Expression outerKey)
    {
        BuildSqlExpression(innerKey);
        _builder.Append(" = ");
        BuildSqlExpression(outerKey);
    }

    /// <summary>
    /// Builds the SQL expression.
    /// </summary>
    private void BuildSqlExpression(Expression expression, bool useStar = false, bool includeAllFields = false, bool visitSubqueryModel = false)
    {
        new IgniteQueryExpressionVisitor(this, useStar, includeAllFields, visitSubqueryModel).Visit(expression);
    }
}
