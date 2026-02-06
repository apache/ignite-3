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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Common;
using Remotion.Linq;
using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.Expressions;
using Remotion.Linq.Clauses.ResultOperators;
using Remotion.Linq.Parsing;
using Sql;
using Table.Serialization;

/// <summary>
/// Expression visitor, transforms query subexpressions (such as Where clauses) to SQL.
/// </summary>
[RequiresUnreferencedCode(IgniteQueryExecutor.TrimWarning)]
internal sealed class IgniteQueryExpressionVisitor : ThrowingExpressionVisitor
{
    /** */
    private static readonly ConcurrentDictionary<MemberInfo, string> ColumnNameMap = new();

    /** */
    private readonly bool _useStar;

    /** */
    private readonly IgniteQueryModelVisitor _modelVisitor;

    // ReSharper disable once NotAccessedField.Local
    private readonly bool _includeAllFields;

    /** */
    private readonly bool _visitEntireSubQueryModel;

    /** */
    private readonly bool _columnNameWithoutTable;

    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteQueryExpressionVisitor" /> class.
    /// </summary>
    /// <param name="modelVisitor">The _model visitor.</param>
    /// <param name="useStar">
    /// Flag indicating that star '*' qualifier should be used
    /// for the whole-table select instead of _key, _val.
    /// </param>
    /// <param name="includeAllFields">
    /// Flag indicating that star '*' qualifier should be used
    /// for the whole-table select as well as _key, _val.
    /// </param>
    /// <param name="visitEntireSubQueryModel">
    /// Flag indicating that subquery should be visited as full query.
    /// </param>
    /// <param name="columnNameWithoutTable">Whether to append column names without table name.</param>
    public IgniteQueryExpressionVisitor(
        IgniteQueryModelVisitor modelVisitor,
        bool useStar,
        bool includeAllFields,
        bool visitEntireSubQueryModel,
        bool columnNameWithoutTable)
    {
        _modelVisitor = modelVisitor;
        _useStar = useStar;
        _includeAllFields = includeAllFields;
        _visitEntireSubQueryModel = visitEntireSubQueryModel;
        _columnNameWithoutTable = columnNameWithoutTable;
    }

    /// <summary>
    /// Gets the result builder.
    /// </summary>
    public StringBuilder ResultBuilder => _modelVisitor.Builder;

    /// <summary>
    /// Gets the parameters.
    /// </summary>
    public IList<object?> Parameters => _modelVisitor.Parameters;

    /// <summary>
    /// Gets the aliases.
    /// </summary>
    private AliasDictionary Aliases => _modelVisitor.Aliases;

    /** <inheritdoc /> */
    public override Expression? Visit(Expression? expression)
    {
        if (expression is ParameterExpression paramExpr)
        {
            // This happens only with compiled queries, where parameters come from enclosing lambda.
            AppendParameter(paramExpr);
            return expression;
        }

        return base.Visit(expression);
    }

    /// <summary>
    /// Appends a parameter.
    /// </summary>
    /// <param name="value">Parameter value.</param>
    public void AppendParameter(object? value)
    {
        ResultBuilder.Append('?');

        if (value is char)
        {
            // Pass char params as string - protocol does not support char.
            value = value.ToString();
        }

        _modelVisitor.Parameters.Add(value);
    }

    /** <inheritdoc /> */
    protected override Expression VisitBinary(BinaryExpression expression)
    {
        // Either func or operator
        if (VisitBinaryFunc(expression))
        {
            return expression;
        }

        ResultBuilder.Append('(');

        Visit(expression.Left);

        switch (expression.NodeType)
        {
            case ExpressionType.Equal:
            {
                // Use `IS [NOT] DISTINCT FROM` for correct null comparison semantics.
                // E.g. when user says `.Where(x => x == null)`, it should work, but with `=` it does not.
                ResultBuilder.Append(" IS NOT DISTINCT FROM ");

                break;
            }

            case ExpressionType.NotEqual:
            {
                ResultBuilder.Append(" IS DISTINCT FROM ");

                break;
            }

            case ExpressionType.AndAlso:
            case ExpressionType.And:
                ResultBuilder.Append(" and ");
                break;

            case ExpressionType.OrElse:
            case ExpressionType.Or:
                ResultBuilder.Append(" or ");
                break;

            case ExpressionType.Add:
                ResultBuilder.Append(" + ");
                break;

            case ExpressionType.Subtract:
                ResultBuilder.Append(" - ");
                break;

            case ExpressionType.Multiply:
                ResultBuilder.Append(" * ");
                break;

            case ExpressionType.Modulo:
                ResultBuilder.Append(" % ");
                break;

            case ExpressionType.Divide:
                ResultBuilder.Append(" / ");
                break;

            case ExpressionType.GreaterThan:
                ResultBuilder.Append(" > ");
                break;

            case ExpressionType.GreaterThanOrEqual:
                ResultBuilder.Append(" >= ");
                break;

            case ExpressionType.LessThan:
                ResultBuilder.Append(" < ");
                break;

            case ExpressionType.LessThanOrEqual:
                ResultBuilder.Append(" <= ");
                break;

            case ExpressionType.Coalesce:
                break;

            default:
                base.VisitBinary(expression);
                break;
        }

        Visit(expression.Right);
        ResultBuilder.TrimEnd().Append(')');

        return expression;
    }

    /** <inheritdoc /> */
    protected override Expression VisitQuerySourceReference(QuerySourceReferenceExpression expression)
    {
        // In some cases of Join clause different handling should be introduced
        var joinClause = expression.ReferencedQuerySource as JoinClause;
        if (joinClause != null && ExpressionWalker.GetIgniteQueryable(expression, false) == null)
        {
            var tableName = Aliases.GetTableAlias(expression);
            var fieldName = Aliases.GetFieldAlias(expression);

            ResultBuilder.AppendFormat(CultureInfo.InvariantCulture, "{0}.{1}", tableName, fieldName);
        }
        else if (joinClause is { InnerSequence: SubQueryExpression })
        {
            var subQueryExpression = (SubQueryExpression) joinClause.InnerSequence;
            base.Visit(subQueryExpression.QueryModel.SelectClause.Selector);
        }
        else
        {
            if (_includeAllFields || _useStar)
            {
                ResultBuilder.Append('*');
            }
            else
            {
                var tableName = Aliases.GetTableAlias(expression);

                AppendColumnNames(expression.ReferencedQuerySource.ItemType, tableName);
            }
        }

        return expression;
    }

    /** <inheritdoc /> */
    protected override Expression VisitMember(MemberExpression expression)
    {
        // Field hierarchy is flattened (Person.Address.Street is just Street), append as is, do not call Visit.

        // Property call (string.Length, DateTime.Month, etc).
        if (MethodVisitor.VisitPropertyCall(expression, this))
        {
            return expression;
        }

        // Special case: grouping
        if (VisitGroupByMember(expression.Expression))
        {
            return expression;
        }

        var queryable = ExpressionWalker.GetIgniteQueryable(expression, false);

        if (queryable != null)
        {
            // Find where the projection comes from.
            expression = ExpressionWalker.GetProjectedMember(expression.Expression!, expression.Member) ?? expression;

            AppendColumnName(expression, Aliases.GetTableAlias(expression));
        }
        else
        {
            AppendParameter(ExpressionWalker.EvaluateExpression<object>(expression));
        }

        return expression;
    }

    /** <inheritdoc /> */
    protected override Expression VisitMemberInit(MemberInitExpression expression)
    {
        var first = true;

        if (expression.NewExpression.Arguments.Count != 0)
        {
            VisitNew(expression.NewExpression);
            first = false;
        }

        foreach (var memberBinding in expression.Bindings)
        {
            if (!first)
            {
                if (_useStar)
                {
                    throw new NotSupportedException("Aggregate functions do not support multiple fields");
                }

                ResultBuilder.TrimEnd().Append(", ");
            }

            if (memberBinding is not MemberAssignment arg)
            {
                throw new NotSupportedException($"{memberBinding.BindingType} binding type is not supported");
            }

            first = false;

            Visit(arg.Expression);

            ResultBuilder.AppendWithSpace("as ");
            ResultBuilder.Append(arg.Member.Name.ToUpperInvariant());
        }

        return expression;
    }

    /** <inheritdoc /> */
    protected override Expression VisitConstant(ConstantExpression expression)
    {
        if (MethodVisitor.VisitConstantCall(expression, this))
        {
            return expression;
        }

        AppendParameter(expression.Value);

        return expression;
    }

    /** <inheritdoc /> */
    protected override Expression VisitMethodCall(MethodCallExpression expression)
    {
        MethodVisitor.VisitMethodCall(expression, this);

        return expression;
    }

    /** <inheritdoc /> */
    protected override Expression VisitNew(NewExpression expression)
    {
        var first = true;

        for (var i = 0; i < expression.Arguments.Count; i++)
        {
            var arg = expression.Arguments[i];
            if (!first)
            {
                if (_useStar)
                {
                    throw new NotSupportedException("Aggregate functions do not support multiple fields");
                }

                ResultBuilder.TrimEnd().Append(", ");
            }

            first = false;

            Visit(arg);

            // When projection uses projection comes from a complex expression, append an alias.
            var param = expression.Members?[i];
            if (param != null && arg is not MemberExpression)
            {
                ResultBuilder.AppendWithSpace("as ").Append(param.Name.ToUpperInvariant());
            }
        }

        return expression;
    }

    /** <inheritdoc /> */
    protected override Expression VisitInvocation(InvocationExpression expression) =>
        throw new NotSupportedException(
            "The LINQ expression '" + expression +
            "' could not be translated. Either rewrite the query in a form that can be translated, " +
            "or switch to client evaluation explicitly by inserting a call to either AsEnumerable() or ToList().");

    /** <inheritdoc /> */
    protected override Expression VisitConditional(ConditionalExpression expression)
    {
        ResultBuilder.Append("case when(");

        Visit(expression.Test);

        ResultBuilder.Append(") then ");
        Visit(expression.IfTrue);

        ResultBuilder.Append(" else ");
        Visit(expression.IfFalse);

        ResultBuilder.Append(" end");

        return expression;
    }

    /** <inheritdoc /> */
    protected override Expression VisitSubQuery(SubQueryExpression expression)
    {
        var subQueryModel = expression.QueryModel;

        var contains = subQueryModel.ResultOperators.FirstOrDefault() as ContainsResultOperator;

        // Check if IEnumerable.Contains is used.
        if (subQueryModel.ResultOperators.Count == 1 && contains != null)
        {
            VisitContains(subQueryModel, contains);
        }
        else if (_visitEntireSubQueryModel)
        {
            ResultBuilder.Append('(');
            _modelVisitor.VisitQueryModel(subQueryModel, false, true);
            ResultBuilder.TrimEnd().Append(')');
        }
        else
        {
            // This happens when New expression uses a subquery, in a GroupBy.
            _modelVisitor.VisitSelectors(expression.QueryModel, false);
        }

        return expression;
    }

    /** <inheritdoc /> */
    protected override Exception CreateUnhandledItemException<T>(T unhandledItem, string visitMethod) =>
        new NotSupportedException($"The expression '{unhandledItem}' (type: {typeof(T)}) is not supported.");

    /** <inheritdoc /> */
    protected override Expression VisitUnary(UnaryExpression expression)
    {
        var closeBracket = false;

        switch (expression.NodeType)
        {
            case ExpressionType.Negate:
                ResultBuilder.Append("(-");
                closeBracket = true;
                break;

            case ExpressionType.Not:
                ResultBuilder.Append("(not ");
                closeBracket = true;
                break;

            case ExpressionType.Convert:
                ResultBuilder.Append("cast(");
                break;

            default:
                return base.VisitUnary(expression);
        }

        Visit(expression.Operand);

        if (closeBracket)
        {
            ResultBuilder.TrimEnd().Append(')');
        }
        else if (expression.NodeType is ExpressionType.Convert)
        {
            if (expression.Type == typeof(object))
            {
                // Special case for string concatenation.
                ResultBuilder.Append(" as varchar)");
            }
            else if ((Nullable.GetUnderlyingType(expression.Type) ?? expression.Type) == typeof(decimal))
            {
                // .NET decimal has 28-29 digit precision, Ignite CatalogUtils.MAX_DECIMAL_PRECISION = Short.MAX_VALUE = 32767.
                // Use (precision, scale) = (60, 30) to avoid rounding errors, but not greater to avoid performance issues.
                // If we do not specify the scale, SQL engine will use MAX_DECIMAL_SCALE = 32767,
                // causing unnecessary data transfer and CPU usage for conversion.
                ResultBuilder.Append(" as decimal(60, 30))");
            }
            else
            {
                ResultBuilder
                    .Append(" as ")
                    .Append(expression.Type.ToSqlTypeName())
                    .Append(')');
            }
        }

        return expression;
    }

    /// <summary>
    /// Appends the name of the column from a member expression, with quotes when necessary.
    /// </summary>
    private void AppendColumnName(MemberExpression expression, string tableName)
    {
        if (ColumnNameMap.TryGetValue(expression.Member, out var columnName))
        {
            AppendColumnName(tableName, columnName);

            return;
        }

        if (expression.Member.DeclaringType.IsKeyValuePair())
        {
            AppendColumnNames(((PropertyInfo)expression.Member).PropertyType, tableName);
            return;
        }

        // When there is a [Column] attribute with Name specified, use quoted identifier: exact match, allows whitespace.
        // Otherwise (most common case), use uppercase non-quoted identifier (case-insensitive).
        // NOTE: The same logic is used in AppendColumnNames below.
        columnName = expression.Member.GetCustomAttribute<ColumnAttribute>() is { Name: { } columnAttributeName }
            ? '"' + columnAttributeName + '"'
            : expression.Member.Name.ToUpperInvariant();

        ColumnNameMap.GetOrAdd(expression.Member, columnName);

        AppendColumnName(tableName, columnName);
    }

    private void AppendColumnName(string tableName, string columnName)
    {
        if (!_columnNameWithoutTable)
        {
            ResultBuilder.Append(tableName).Append('.');
        }

        ResultBuilder.Append(columnName);
    }

    /// <summary>
    /// Appends column names for all fields in the specified type.
    /// </summary>
    /// <param name="type">Type.</param>
    /// <param name="tableName">Table name.</param>
    /// <param name="first">Whether this is the first column and does not need a comma before.</param>
    /// <param name="toSkip">Names to skip.</param>
    /// <param name="populateToSkip">Whether to populate provided toSkip set.</param>
    private void AppendColumnNames(Type type, string tableName, bool first = true, HashSet<string>? toSkip = null, bool populateToSkip = false)
    {
        if (type.IsPrimitive)
        {
            throw new NotSupportedException(
                $"Primitive types are not supported in LINQ queries: {type}. " +
                "Use a custom type (class, record, struct) with a single field instead.");
        }

        if (type.GetKeyValuePairTypes() is var (keyType, valType))
        {
            var keyColumnNames = new HashSet<string>();

            AppendColumnNames(keyType, tableName, first: true, toSkip: keyColumnNames, populateToSkip: true);
            AppendColumnNames(valType, tableName, first: false, toSkip: keyColumnNames);

            return;
        }

        var columns = type.GetColumns();

        if (columns.Count == 0)
        {
            throw new NotSupportedException(
                $"Type '{type}' can not be mapped to SQL columns: it has no fields, or all fields are [NotMapped].");
        }

        foreach (var col in columns)
        {
            if (toSkip != null)
            {
                if (populateToSkip)
                {
                    toSkip.Add(col.Name);
                }
                else if (toSkip.Contains(col.Name))
                {
                    continue;
                }
            }

            if (!first)
            {
                ResultBuilder.TrimEnd().Append(", ");
            }

            first = false;

            if (!_columnNameWithoutTable)
            {
                ResultBuilder.Append(tableName).Append('.');
            }

            if (col.HasColumnNameAttribute)
            {
                // Exact quoted name.
                ResultBuilder.Append('"').Append(col.Name).Append('"');
            }
            else
            {
                // Case-insensitive, unquoted, upper-case name.
                ResultBuilder.Append(col.Name.ToUpperInvariant());
            }
        }
    }

    /// <summary>
    /// Visits IEnumerable.Contains.
    /// </summary>
    private void VisitContains(QueryModel subQueryModel, ContainsResultOperator contains)
    {
        ResultBuilder.Append('(');

        var fromExpression = subQueryModel.MainFromClause.FromExpression;

        var queryable = ExpressionWalker.GetIgniteQueryable(fromExpression, false);

        if (queryable != null)
        {
            Visit(contains.Item);

            ResultBuilder.Append(" IN (");
            if (_visitEntireSubQueryModel)
            {
                _modelVisitor.VisitQueryModel(subQueryModel, false, true);
            }
            else
            {
                _modelVisitor.VisitQueryModel(subQueryModel);
            }

            ResultBuilder.Append(')');
        }
        else
        {
            var inValues = ExpressionWalker.EvaluateEnumerableValues(fromExpression).ToArray();

            var hasNulls = inValues.Any(o => o == null);

            if (hasNulls)
            {
                ResultBuilder.Append('(');
            }

            Visit(contains.Item);

            ResultBuilder.Append(" IN (");
            AppendInParameters(inValues);
            ResultBuilder.Append(')');

            if (hasNulls)
            {
                ResultBuilder.Append(") OR ");
                Visit(contains.Item);
                ResultBuilder.Append(" IS NULL");
            }
        }

        ResultBuilder.Append(')');
    }

    /// <summary>
    /// Appends not null parameters using ", " as delimeter.
    /// </summary>
    private void AppendInParameters(IEnumerable<object?> values)
    {
        var first = true;

        foreach (var val in values)
        {
            if (val == null)
            {
                continue;
            }

            if (!first)
            {
                ResultBuilder.Append(", ");
            }

            first = false;

            AppendParameter(val);
        }
    }

    /// <summary>
    /// Visits the group by member.
    /// </summary>
    private bool VisitGroupByMember(Expression? expression)
    {
        var srcRef = expression as QuerySourceReferenceExpression;
        if (srcRef == null)
        {
            return false;
        }

        var from = srcRef.ReferencedQuerySource as IFromClause;
        if (from == null)
        {
            return false;
        }

        var subQry = from.FromExpression as SubQueryExpression;
        if (subQry == null)
        {
            return false;
        }

        var grpBy = subQry.QueryModel.ResultOperators.OfType<GroupResultOperator>().FirstOrDefault();
        if (grpBy == null)
        {
            return false;
        }

        var (alias, aliasCreated) = Aliases.GetOrCreateGroupByMemberAlias(grpBy);

        if (aliasCreated)
        {
            Visit(grpBy.KeySelector);
            ResultBuilder.Append(" as ").Append(alias);
        }
        else
        {
            ResultBuilder.Append(alias);
        }

        return true;
    }

    /// <summary>
    /// Visits the binary function.
    /// </summary>
    /// <param name="expression">The expression.</param>
    /// <returns>True if function detected, otherwise false.</returns>
    private bool VisitBinaryFunc(BinaryExpression expression)
    {
        if (expression.NodeType == ExpressionType.Add && expression.Left.Type == typeof(string))
        {
            ResultBuilder.Append("concat(");
        }
        else if (expression.NodeType == ExpressionType.Coalesce)
        {
            ResultBuilder.Append("coalesce(");
        }
        else
        {
            return false;
        }

        Visit(expression.Left);
        ResultBuilder.Append(", ");
        Visit(expression.Right);
        ResultBuilder.TrimEnd().Append(')');

        return true;
    }
}
