
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
