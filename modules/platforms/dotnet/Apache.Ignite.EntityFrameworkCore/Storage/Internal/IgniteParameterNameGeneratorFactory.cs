namespace Apache.Ignite.EntityFrameworkCore.Storage.Internal;

using Microsoft.EntityFrameworkCore.Storage;

public class IgniteParameterNameGeneratorFactory : IParameterNameGeneratorFactory
{
    public IgniteParameterNameGeneratorFactory(ParameterNameGeneratorDependencies dependencies)
    {
        Dependencies = dependencies;
    }

    protected virtual ParameterNameGeneratorDependencies Dependencies { get; }

    public virtual ParameterNameGenerator Create()
        => new IgniteParameterNameGenerator();
}
