namespace Apache.Ignite.EntityFrameworkCore.Metadata.Conventions;

using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;

public class IgniteSharedTableConvention : SharedTableConvention
{
    public IgniteSharedTableConvention(
        ProviderConventionSetBuilderDependencies dependencies,
        RelationalConventionSetBuilderDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
    }

    /// <inheritdoc />
    protected override bool CheckConstraintsUniqueAcrossTables
        => false;
}
