// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace Apache.Ignite.EntityFrameworkCore.Query.Internal;

using System;
using System.Linq;
using System.Linq.Expressions;
using Common;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

public class IgniteQueryTranslationPostprocessor : RelationalQueryTranslationPostprocessor
{
    private readonly ApplyValidatingVisitor _applyValidator = new();

    public IgniteQueryTranslationPostprocessor(
        QueryTranslationPostprocessorDependencies dependencies,
        RelationalQueryTranslationPostprocessorDependencies relationalDependencies,
        QueryCompilationContext queryCompilationContext)
        : base(dependencies, relationalDependencies, queryCompilationContext)
    {
    }

    public override Expression Process(Expression query)
    {
        var result = base.Process(query);
        _applyValidator.Visit(result);

        return result;
    }

    private sealed class ApplyValidatingVisitor : ExpressionVisitor
    {
        protected override Expression VisitExtension(Expression extensionExpression)
        {
            if (extensionExpression is ShapedQueryExpression shapedQueryExpression)
            {
                Visit(shapedQueryExpression.QueryExpression);
                Visit(shapedQueryExpression.ShaperExpression);

                return extensionExpression;
            }

            if (extensionExpression is SelectExpression selectExpression
                && selectExpression.Tables.Any(t => t is CrossApplyExpression or OuterApplyExpression))
            {
                throw new InvalidOperationException(IgniteStrings.ApplyNotSupported);
            }

            return base.VisitExtension(extensionExpression);
        }
    }
}
