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
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using Dml;
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
        transformerRegistry.Register(new MemoryExtensionsContainsExpressionTransformer());

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
                new PartialEvaluatingExpressionTreeProcessor(new IgniteEvaluatableExpressionFilter()),
                new TransformingExpressionTreeProcessor(transformationProvider)
            });
    }

    /// <summary>
    /// Implementation of IEvaluatableExpressionFilter.
    /// </summary>
    private sealed class IgniteEvaluatableExpressionFilter : EvaluatableExpressionFilterBase
    {
        // Ignores implicit ReadOnlySpan conversion to support C# 14 first class span Contains.
        public override bool IsEvaluatableMethodCall(MethodCallExpression node)
        {
            ArgumentNullException.ThrowIfNull(node);
            if (MemoryExtensionsContainsExpressionTransformer.IsSpanImplicitConversion(node))
            {
                return false;
            }

            return base.IsEvaluatableMethodCall(node);
        }
    }

    /// <summary>
    /// Implementation of IExpressionTransformer handling C# 14 first class span Contains.
    /// </summary>
    private sealed class MemoryExtensionsContainsExpressionTransformer : IExpressionTransformer<MethodCallExpression>
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

        [SuppressMessage("Performance", "CA1819:Properties should not return arrays", Justification = "Interface impl")]
        public ExpressionType[] SupportedExpressionTypes => [ExpressionType.Call];

        public static bool IsSpanImplicitConversion([NotNullWhen(true)] MethodCallExpression? node) =>
            node?.Method is { IsSpecialName: true, Name: "op_Implicit" }
            && node.Method.DeclaringType is { IsGenericType: true }
            && node.Method.DeclaringType.GetGenericTypeDefinition() == typeof(ReadOnlySpan<>);

        public Expression Transform(MethodCallExpression expression)
        {
            if (expression.Method.IsConstructedGenericMethod && expression.Method.GetGenericMethodDefinition() == SourceMethodInfo)
            {
                var genericType = expression.Method.GetGenericArguments()[0];

                var enumerableParameter = expression.Arguments[0] as MethodCallExpression;
                if (!IsSpanImplicitConversion(enumerableParameter))
                {
                    throw new NotSupportedException("Was not able to process parameter for MemoryExtensions.Contains, " +
                                                    "expected implicit conversion, got: " + expression.Arguments[0]);
                }

                var argument = enumerableParameter.Arguments[0]!;

                var targetProp = expression.Arguments[1];

                var target = TargetMethodInfo.MakeGenericMethod(genericType);
                return Expression.Call(target, argument, targetProp);
            }

            return expression;
        }
    }
}
