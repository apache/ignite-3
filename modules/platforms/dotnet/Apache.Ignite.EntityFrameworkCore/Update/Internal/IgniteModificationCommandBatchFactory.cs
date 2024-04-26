
namespace Apache.Ignite.EntityFrameworkCore.Update.Internal;

using Microsoft.EntityFrameworkCore.Update;

public class IgniteModificationCommandBatchFactory : IModificationCommandBatchFactory
{
    public IgniteModificationCommandBatchFactory(
        ModificationCommandBatchFactoryDependencies dependencies)
    {
        Dependencies = dependencies;
    }

    protected virtual ModificationCommandBatchFactoryDependencies Dependencies { get; }

    public virtual ModificationCommandBatch Create()
        => new SingularModificationCommandBatch(Dependencies);
}
