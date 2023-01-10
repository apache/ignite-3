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

namespace Apache.Ignite.Internal.Linq.Dml;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using Ignite.Sql;
using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.Expressions;
using Remotion.Linq.Parsing.ExpressionVisitors;
using Remotion.Linq.Parsing.Structure.IntermediateModel;

/// <summary>
/// Represents a <see cref="MethodCallExpression" /> for <see cref="IgniteQueryableExtensions.ExecuteUpdateAsync{T}" />.
/// </summary>
internal sealed class ExecuteUpdateExpressionNode : ResultOperatorExpressionNodeBase
{
    /// <summary>
    /// ExecuteUpdate method.
    /// </summary>
    public static readonly MethodInfo MethodInfo = typeof(IgniteQueryableExtensions)
        .GetMethod(nameof(IgniteQueryableExtensions.ExecuteUpdateAsync))!;

    /// <summary>
    /// All supported methods.
    /// </summary>
    public static readonly IReadOnlyList<MethodInfo> MethodInfos = new[] { MethodInfo };

    private readonly LambdaExpression _updateDescription;

    /// <summary>
    /// Initializes a new instance of the <see cref="ExecuteUpdateExpressionNode" /> class.
    /// </summary>
    /// <param name="parseInfo">Parse information.</param>
    /// <param name="updateDescription">Expression with update description info.</param>
    public ExecuteUpdateExpressionNode(
        MethodCallExpressionParseInfo parseInfo,
        LambdaExpression updateDescription)
        : base(parseInfo, null, null)
    {
        _updateDescription = updateDescription;
    }

    /** <inheritdoc /> */
    [ExcludeFromCodeCoverage]
    public override Expression Resolve(
        ParameterExpression inputParameter,
        Expression expressionToBeResolved,
        ClauseGenerationContext clauseGenerationContext)
    {
        throw CreateResolveNotSupportedException();
    }

    /** <inheritdoc /> */
    protected override ResultOperatorBase CreateResultOperator(ClauseGenerationContext clauseGenerationContext)
    {
        if (_updateDescription.Parameters.Count != 1)
        {
            throw new NotSupportedException(
                $"Expression is not supported for {nameof(IgniteQueryableExtensions.ExecuteUpdateAsync)}: " + _updateDescription);
        }

        var querySourceRefExpression = (QuerySourceReferenceExpression)Source.Resolve(
            _updateDescription.Parameters[0],
            _updateDescription.Parameters[0],
            clauseGenerationContext);

        var methodCall = _updateDescription.Body as MethodCallExpression;

        if (methodCall == null)
        {
            throw new NotSupportedException(
                $"Expression is not supported for {nameof(IgniteQueryableExtensions.ExecuteUpdateAsync)}: " + _updateDescription.Body);
        }

        var updates = new List<MemberUpdateContainer>();

        while (methodCall != null)
        {
            if (methodCall.Arguments.Count != 2)
            {
                throw new NotSupportedException(
                    $"Method is not supported for {nameof(IgniteQueryableExtensions.ExecuteUpdateAsync)}: " + methodCall);
            }

            var selectorLambda = (LambdaExpression)methodCall.Arguments[0];
            var selector = ReplacingExpressionVisitor.Replace(selectorLambda.Parameters[0], querySourceRefExpression, selectorLambda.Body);

            var newValue = methodCall.Arguments[1];
            switch (newValue.NodeType)
            {
                case ExpressionType.Constant:
                    break;

                case ExpressionType.Lambda:
                    var newValueLambda = (LambdaExpression)newValue;

                    newValue = ReplacingExpressionVisitor.Replace(
                        replacedExpression: newValueLambda.Parameters[0],
                        replacementExpression: querySourceRefExpression,
                        sourceTree: newValueLambda.Body);

                    break;

                default:
                    throw new NotSupportedException(
                        $"Value expression is not supported for {nameof(IgniteQueryableExtensions.ExecuteUpdateAsync)}: " + newValue);
            }

            updates.Add(new MemberUpdateContainer(selector, newValue));

            methodCall = methodCall.Object as MethodCallExpression;
        }

        return new ExecuteUpdateResultOperator(updates);
    }
}
