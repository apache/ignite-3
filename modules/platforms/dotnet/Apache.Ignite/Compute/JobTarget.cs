namespace Apache.Ignite.Compute;

using System.Collections.Generic;
using Network;

/// <summary>
/// Compute job target.
/// </summary>
public class JobTarget
{
    static IJobTarget<IClusterNode> Node(IClusterNode node)
    {
        return null!;
    }

    static IJobTarget<IEnumerable<IClusterNode>> AnyNode(IEnumerable<IClusterNode> nodes)
    {
        return null!;
    }

    static IJobTarget<IEnumerable<IClusterNode>> AnyNode(params IClusterNode[] nodes)
    {
        return null!;
    }

    static IJobTarget<ColocationKey<T>> Colocated<T>(string tableName, T key)
    {
        return null!;
    }
}
