
namespace Apache.Ignite.EntityFrameworkCore.Query.Internal;

using Microsoft.EntityFrameworkCore.Query;

public class IgniteQueryTranslationPostprocessorFactory : IQueryTranslationPostprocessorFactory
{
    public IgniteQueryTranslationPostprocessorFactory(
        QueryTranslationPostprocessorDependencies dependencies,
        RelationalQueryTranslationPostprocessorDependencies relationalDependencies)
    {
        Dependencies = dependencies;
        RelationalDependencies = relationalDependencies;
    }

    protected virtual QueryTranslationPostprocessorDependencies Dependencies { get; }

    protected virtual RelationalQueryTranslationPostprocessorDependencies RelationalDependencies { get; }

    public virtual QueryTranslationPostprocessor Create(QueryCompilationContext queryCompilationContext)
        => new IgniteQueryTranslationPostprocessor(
            Dependencies,
            RelationalDependencies,
            queryCompilationContext);
}
