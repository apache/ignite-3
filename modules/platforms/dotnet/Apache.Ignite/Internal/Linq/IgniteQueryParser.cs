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
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using Dml;
using Remotion.Linq.Clauses.Expressions;
using Remotion.Linq.Parsing.ExpressionVisitors.Transformation;
using Remotion.Linq.Parsing.ExpressionVisitors.TreeEvaluation;
using Remotion.Linq.Parsing.Structure;
using Remotion.Linq.Parsing.Structure.ExpressionTreeProcessors;
using Remotion.Linq.Parsing.Structure.NodeTypeProviders;

/// <summary>
/// Query parser.
/// </summary>
internal static class IgniteQueryParser
{
    /** */
    private static readonly ThreadLocal<QueryParser> ThreadLocalInstance = new(CreateParser);

    /// <summary>
    /// Gets the default instance for current thread.
    /// </summary>
    public static QueryParser Instance => ThreadLocalInstance.Value!;

    /// <summary>
    /// Creates the parser.
    /// </summary>
    private static QueryParser CreateParser()
    {
        var transformerRegistry = ExpressionTransformerRegistry.CreateDefault();
        transformerRegistry.Register(new MyCass());

        var proc = CreateCompoundProcessor(transformerRegistry);

        var parser = new ExpressionTreeParser(CreateNodeTypeProvider(), proc);

        return new QueryParser(parser);
    }

    /// <summary>
    /// Creates the node type provider.
    /// </summary>
    private static CompoundNodeTypeProvider CreateNodeTypeProvider()
    {
        var methodInfoRegistry = MethodInfoBasedNodeTypeRegistry.CreateFromRelinqAssembly();

        methodInfoRegistry.Register(ExecuteDeleteExpressionNode.MethodInfos, typeof(ExecuteDeleteExpressionNode));
        methodInfoRegistry.Register(ExecuteUpdateExpressionNode.MethodInfos, typeof(ExecuteUpdateExpressionNode));

        // methodInfoRegistry.Register(MemoryExtensionsContainsExpressionNode.MethodInfos, typeof(MemoryExtensionsContainsExpressionNode));
        return new CompoundNodeTypeProvider(new INodeTypeProvider[]
        {
            methodInfoRegistry,
            MethodNameBasedNodeTypeRegistry.CreateFromRelinqAssembly()
        });
    }

    /// <summary>
    /// Creates CompoundExpressionTreeProcessor.
    /// </summary>
    private static CompoundExpressionTreeProcessor CreateCompoundProcessor(
        IExpressionTranformationProvider transformationProvider)
    {
        return new CompoundExpressionTreeProcessor(
            new IExpressionTreeProcessor[]
            {
                new PartialEvaluatingExpressionTreeProcessor(new NullEvaluatableExpressionFilter()),
                new TransformingExpressionTreeProcessor(transformationProvider)
            });
    }

    /// <summary>
    /// Empty implementation of IEvaluatableExpressionFilter.
    /// </summary>
    private sealed class NullEvaluatableExpressionFilter : EvaluatableExpressionFilterBase
    {
        // No-op.
    }

    /// <summary>
    /// Empty implementation of IEvaluatableExpressionFilter.
    /// </summary>
    private sealed class MyCass : IExpressionTransformer<MethodCallExpression>
    {
        private static readonly MethodInfo SourceMethodInfo = typeof(MemoryExtensions)
            .GetMethod(nameof(MemoryExtensions.Contains), [
                typeof(ReadOnlySpan<>).MakeGenericType(Type.MakeGenericMethodParameter(0)),
                Type.MakeGenericMethodParameter(0)
            ])!;

        private static readonly MethodInfo TargetMethodInfo = typeof(Enumerable)
            .GetMethod(nameof(Enumerable.Contains), [
                typeof(IEnumerable<>).MakeGenericType(Type.MakeGenericMethodParameter(0)),
                Type.MakeGenericMethodParameter(0)
            ])!;

        public Expression Transform(MethodCallExpression expression)
        {
            if (expression.Method.IsConstructedGenericMethod && expression.Method.GetGenericMethodDefinition() == SourceMethodInfo)
            {
                var genericType = expression.Method.GetGenericArguments()[0];
                var target = TargetMethodInfo.MakeGenericMethod(genericType);

                var enumerable = expression.Arguments[0];
                var exceptionExpression = enumerable as PartialEvaluationExceptionExpression;
                var exceptionExpressionEvaluatedExpression = exceptionExpression?.EvaluatedExpression;
                var argument = (exceptionExpressionEvaluatedExpression as MethodCallExpression)?.Arguments[0]!;

                var targetProp = expression.Arguments[1];

                return Expression.Call(target, argument, targetProp);
            }

            return expression;
        }

#pragma warning disable CA1819
#pragma warning disable SA1201
        public ExpressionType[] SupportedExpressionTypes => [ExpressionType.Call];
#pragma warning restore SA1201
#pragma warning restore CA1819
    }
}
