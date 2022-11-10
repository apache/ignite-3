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

namespace Apache.Ignite.Internal.Linq;

using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.Expressions;

/// <summary>
/// Walks expression trees to extract query and table name info.
/// </summary>
internal static class ExpressionWalker
{
    /** Compiled member readers. */
    private static readonly ConcurrentDictionary<MemberInfo, Func<object?, object?>> MemberReaders = new();

    /// <summary>
    /// Gets the queryable.
    /// </summary>
    /// <param name="fromClause">FROM clause.</param>
    /// <param name="throwWhenNotFound">Whether to throw when not found or return null.</param>
    /// <returns>Ignite internal queryable.</returns>
    public static IIgniteQueryableInternal? GetIgniteQueryable(IFromClause fromClause, bool throwWhenNotFound = true)
    {
        return GetIgniteQueryable(fromClause.FromExpression, throwWhenNotFound);
    }

    /// <summary>
    /// Gets the queryable.
    /// </summary>
    /// <param name="joinClause">JOIN clause.</param>
    /// <param name="throwWhenNotFound">Whether to throw when not found or return null.</param>
    /// <returns>Ignite internal queryable.</returns>
    public static IIgniteQueryableInternal? GetIgniteQueryable(JoinClause joinClause, bool throwWhenNotFound = true)
    {
        return GetIgniteQueryable(joinClause.InnerSequence, throwWhenNotFound);
    }

    /// <summary>
    /// Gets the queryable.
    /// </summary>
    /// <param name="expression">Expression.</param>
    /// <param name="throwWhenNotFound">Whether to throw when not found or return null.</param>
    /// <returns>Ignite internal queryable.</returns>
    public static IIgniteQueryableInternal? GetIgniteQueryable(Expression expression, bool throwWhenNotFound = true)
    {
        if (expression is SubQueryExpression subQueryExp)
        {
            return GetIgniteQueryable(subQueryExp.QueryModel.MainFromClause, throwWhenNotFound);
        }

        if (expression is QuerySourceReferenceExpression srcRefExp)
        {
            if (srcRefExp.ReferencedQuerySource is IFromClause fromSource)
            {
                return GetIgniteQueryable(fromSource, throwWhenNotFound);
            }

            if (srcRefExp.ReferencedQuerySource is JoinClause joinSource)
            {
                return GetIgniteQueryable(joinSource, throwWhenNotFound);
            }

            throw new NotSupportedException("Unexpected query source: " + srcRefExp.ReferencedQuerySource);
        }

        if (expression is MemberExpression memberExpr)
        {
            if (memberExpr.Type.IsGenericType &&
                memberExpr.Type.GetGenericTypeDefinition() == typeof(IQueryable<>))
            {
                return EvaluateExpression<IIgniteQueryableInternal>(memberExpr);
            }

            return GetIgniteQueryable(memberExpr.Expression!, throwWhenNotFound);
        }

        if (expression is ConstantExpression { Value: IIgniteQueryableInternal queryable })
        {
            return queryable;
        }

        if (expression is MethodCallExpression callExpr)
        {
            // This is usually a nested query with a call to AsCacheQueryable().
            return (IIgniteQueryableInternal) Expression.Lambda(callExpr).Compile().DynamicInvoke()!;
        }

        if (throwWhenNotFound)
        {
            throw new NotSupportedException("Unexpected query source: " + expression);
        }

        return null;
    }

    /// <summary>
    /// Gets the projected member.
    /// Queries can have multiple projections, e.g. <c>qry.Select(person => new {Foo = person.Name})</c>.
    /// This method finds the original member expression from the given projected expression, e.g finds
    /// <c>Person.Name</c> from <c>Foo</c>.
    /// </summary>
    /// <param name="expression">Expression.</param>
    /// <param name="memberHint">Member info.</param>
    /// <returns>Projected member.</returns>
    public static MemberExpression? GetProjectedMember(Expression expression, MemberInfo memberHint)
    {
        if (expression is SubQueryExpression subQueryExp)
        {
            var selector = subQueryExp.QueryModel.SelectClause.Selector;

            if (selector is NewExpression { Members: { } } newExpr)
            {
                Debug.Assert(newExpr.Members.Count == newExpr.Arguments.Count, "newExpr.Members.Count == newExpr.Arguments.Count");

                for (var i = 0; i < newExpr.Members.Count; i++)
                {
                    var member = newExpr.Members[i];

                    if (member == memberHint)
                    {
                        return newExpr.Arguments[i] as MemberExpression;
                    }
                }
            }

            if (selector is MemberInitExpression initExpr)
            {
                foreach (var binding in initExpr.Bindings)
                {
                    if (binding.Member == memberHint && binding.BindingType == MemberBindingType.Assignment)
                    {
                        return ((MemberAssignment)binding).Expression as MemberExpression;
                    }
                }
            }

            return GetProjectedMember(subQueryExp.QueryModel.MainFromClause.FromExpression, memberHint);
        }

        if (expression is QuerySourceReferenceExpression { ReferencedQuerySource: IFromClause fromSource })
        {
            return GetProjectedMember(fromSource.FromExpression, memberHint);
        }

        return null;
    }

    /// <summary>
    /// Gets the original QuerySourceReferenceExpression.
    /// </summary>
    public static QuerySourceReferenceExpression? GetQuerySourceReference(
        Expression expression,
        bool throwWhenNotFound = true)
    {
        var reference = expression as QuerySourceReferenceExpression;
        if (reference != null)
        {
            return reference;
        }

        var unary = expression as UnaryExpression;
        if (unary != null)
        {
            return GetQuerySourceReference(unary.Operand, false);
        }

        var binary = expression as BinaryExpression;
        if (binary != null)
        {
            return GetQuerySourceReference(binary.Left, false) ?? GetQuerySourceReference(binary.Right, false);
        }

        if (throwWhenNotFound)
        {
            throw new NotSupportedException("Unexpected query source: " + expression);
        }

        return null;
    }

    /// <summary>
    /// Gets the query source.
    /// </summary>
    public static IQuerySource? GetQuerySource(Expression expression, MemberExpression? memberHint = null)
    {
        if (memberHint != null)
        {
            var newExpr = expression as NewExpression;

            if (newExpr != null && newExpr.Members != null)
            {
                for (var i = 0; i < newExpr.Members.Count; i++)
                {
                    var member = newExpr.Members[i];

                    if (member == memberHint.Member)
                    {
                        return GetQuerySource(newExpr.Arguments[i]);
                    }
                }
            }
        }

        var subQueryExp = expression as SubQueryExpression;

        if (subQueryExp != null)
        {
            var source = GetQuerySource(subQueryExp.QueryModel.SelectClause.Selector, memberHint);
            if (source != null)
            {
                return source;
            }

            return subQueryExp.QueryModel.MainFromClause;
        }

        var srcRefExp = expression as QuerySourceReferenceExpression;

        if (srcRefExp != null)
        {
            var fromSource = srcRefExp.ReferencedQuerySource as IFromClause;

            if (fromSource != null)
            {
                var source = GetQuerySource(fromSource.FromExpression, memberHint);
                if (source != null)
                {
                    return source;
                }

                return fromSource;
            }

            var joinSource = srcRefExp.ReferencedQuerySource as JoinClause;

            if (joinSource != null)
            {
                return GetQuerySource(joinSource.InnerSequence, memberHint) ?? joinSource;
            }

            throw new NotSupportedException("Unexpected query source: " + srcRefExp.ReferencedQuerySource);
        }

        var memberExpr = expression as MemberExpression;

        if (memberExpr != null && memberExpr.Expression != null)
        {
            return GetQuerySource(memberExpr.Expression, memberExpr);
        }

        return null;
    }

    /// <summary>
    /// Evaluates the expression.
    /// </summary>
    /// <param name="expr">Expression.</param>
    /// <typeparam name="T">Expression type.</typeparam>
    /// <returns>Evaluation result.</returns>
    public static T? EvaluateExpression<T>(Expression expr)
    {
        var constExpr = expr as ConstantExpression;

        if (constExpr != null)
        {
            return (T)constExpr.Value!;
        }

        var memberExpr = expr as MemberExpression;

        if (memberExpr != null)
        {
            var targetExpr = memberExpr.Expression as ConstantExpression;

            if (memberExpr.Expression == null || targetExpr != null)
            {
                // Instance or static member
                var target = targetExpr == null ? null : targetExpr.Value;

                if (MemberReaders.TryGetValue(memberExpr.Member, out var reader))
                {
                    return (T) reader(target)!;
                }

                return (T?) MemberReaders.GetOrAdd(memberExpr.Member, _ => CompileMemberReader(memberExpr))(target);
            }
        }

        // Case for compiled queries: return unchanged.
        // ReSharper disable once CanBeReplacedWithTryCastAndCheckForNull
        if (expr is ParameterExpression)
        {
            return (T) (object) expr;
        }

        throw new NotSupportedException("Expression not supported: " + expr);
    }

    /// <summary>
    /// Gets the values from IEnumerable expression.
    /// </summary>
    public static IEnumerable<object?> EvaluateEnumerableValues(Expression fromExpression)
    {
        IEnumerable? result;

        switch (fromExpression.NodeType)
        {
            case ExpressionType.MemberAccess:
                var memberExpression = (MemberExpression)fromExpression;
                result = EvaluateExpression<IEnumerable>(memberExpression);
                break;

            case ExpressionType.ListInit:
                var listInitExpression = (ListInitExpression)fromExpression;
                result = listInitExpression.Initializers
                    .SelectMany(init => init.Arguments)
                    .Select(EvaluateExpression<object>);
                break;

            case ExpressionType.NewArrayInit:
                var newArrayExpression = (NewArrayExpression)fromExpression;
                result = newArrayExpression.Expressions
                    .Select(EvaluateExpression<object>);
                break;

            case ExpressionType.Parameter:
                // This should happen only when 'IEnumerable.Contains' is called on parameter of compiled query
                throw new NotSupportedException("'Contains' clause on compiled query parameter is not supported.");

            default:
                result = Expression.Lambda(fromExpression).Compile().DynamicInvoke() as IEnumerable;
                break;
        }

        result ??= Enumerable.Empty<object>();

        return result
            .Cast<object>()
            .ToArray();
    }

    /// <summary>
    /// Compiles the member reader.
    /// </summary>
    private static Func<object?, object?> CompileMemberReader(MemberExpression memberExpr)
    {
        // Field or property
        var fld = memberExpr.Member as FieldInfo;

        if (fld != null)
        {
            // TODO: Compiled delegate.
            return o => fld.GetValue(o);
        }

        var prop = memberExpr.Member as PropertyInfo;

        if (prop != null)
        {
            // TODO: Compiled delegate.
            return o => prop.GetValue(o, null);
        }

        throw new NotSupportedException("Expression not supported: " + memberExpr);
    }

    /// <summary>
    /// Gets the table name with schema.
    /// <para />
    /// Only PUBLIC schema is supported for now by the SQL engine.
    /// </summary>
    public static string GetTableNameWithSchema(IIgniteQueryableInternal queryable) => $"PUBLIC.{queryable.TableName}";
}
