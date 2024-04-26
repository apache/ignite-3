namespace Apache.Ignite.EntityFrameworkCore.Storage.Internal;

using Microsoft.EntityFrameworkCore.Storage;

public class IgniteParameterNameGeneratorFactory : IParameterNameGeneratorFactory
{
    public IgniteParameterNameGeneratorFactory(ParameterNameGeneratorDependencies dependencies)
    {
        Dependencies = dependencies;
    }

    /// <summary>
    ///     Relational provider-specific dependencies for this service.
    /// </summary>
    protected virtual ParameterNameGeneratorDependencies Dependencies { get; }

    /// <summary>
    ///     Creates a new <see cref="ParameterNameGenerator" />.
    /// </summary>
    /// <returns>The newly created generator.</returns>
    public virtual ParameterNameGenerator Create()
        => new IgniteParameterNameGenerator();
}
