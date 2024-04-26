
namespace Apache.Ignite.EntityFrameworkCore.Query.Internal;

using Microsoft.EntityFrameworkCore.Query;

public class IgniteQuerySqlGeneratorFactory : IQuerySqlGeneratorFactory
{
    public IgniteQuerySqlGeneratorFactory(QuerySqlGeneratorDependencies dependencies)
    {
        Dependencies = dependencies;
    }

    /// <summary>
    ///     Relational provider-specific dependencies for this service.
    /// </summary>
    protected virtual QuerySqlGeneratorDependencies Dependencies { get; }

    public virtual QuerySqlGenerator Create()
        => new IgniteQuerySqlGenerator(Dependencies);
}
