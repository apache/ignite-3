namespace Apache.Ignite.EntityFrameworkCore.Metadata.Conventions;

using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;

public class IgniteConventionSetBuilder : RelationalConventionSetBuilder
{
    public IgniteConventionSetBuilder(
        ProviderConventionSetBuilderDependencies dependencies,
        RelationalConventionSetBuilderDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
    }

    public override ConventionSet CreateConventionSet()
    {
        var conventionSet = base.CreateConventionSet();

        conventionSet.Replace<SharedTableConvention>(new IgniteSharedTableConvention(Dependencies, RelationalDependencies));

        return conventionSet;
    }
}
