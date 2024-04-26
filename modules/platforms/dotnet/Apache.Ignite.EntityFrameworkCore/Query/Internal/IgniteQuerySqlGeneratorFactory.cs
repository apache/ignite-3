
namespace Apache.Ignite.EntityFrameworkCore.Query.Internal;

using Microsoft.EntityFrameworkCore.Query;

public class IgniteQuerySqlGeneratorFactory : IQuerySqlGeneratorFactory
{
    public IgniteQuerySqlGeneratorFactory(QuerySqlGeneratorDependencies dependencies)
    {
        Dependencies = dependencies;
    }

    protected virtual QuerySqlGeneratorDependencies Dependencies { get; }

    public virtual QuerySqlGenerator Create()
        => new IgniteQuerySqlGenerator(Dependencies);
}
