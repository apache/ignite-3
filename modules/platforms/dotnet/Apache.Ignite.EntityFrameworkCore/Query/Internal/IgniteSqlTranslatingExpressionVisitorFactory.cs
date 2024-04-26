
namespace Apache.Ignite.EntityFrameworkCore.Query.Internal;

using Microsoft.EntityFrameworkCore.Query;

public class IgniteSqlTranslatingExpressionVisitorFactory : IRelationalSqlTranslatingExpressionVisitorFactory
{
    public IgniteSqlTranslatingExpressionVisitorFactory(
        RelationalSqlTranslatingExpressionVisitorDependencies dependencies)
    {
        Dependencies = dependencies;
    }

    protected virtual RelationalSqlTranslatingExpressionVisitorDependencies Dependencies { get; }

    public virtual RelationalSqlTranslatingExpressionVisitor Create(
        QueryCompilationContext queryCompilationContext,
        QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor)
        => new IgniteSqlTranslatingExpressionVisitor(
            Dependencies,
            queryCompilationContext,
            queryableMethodTranslatingExpressionVisitor);
}
