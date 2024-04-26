namespace Apache.Ignite.EntityFrameworkCore.Storage.Internal;

using Microsoft.EntityFrameworkCore.Storage;

public class IgniteRelationalCommandBuilderFactory : IRelationalCommandBuilderFactory
{
    public IgniteRelationalCommandBuilderFactory(
        RelationalCommandBuilderDependencies dependencies)
    {
        Dependencies = dependencies;
    }

    protected virtual RelationalCommandBuilderDependencies Dependencies { get; }

    public virtual IRelationalCommandBuilder Create()
        => new IgniteRelationalCommandBuilder(Dependencies);
}
