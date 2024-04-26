namespace Apache.Ignite.EntityFrameworkCore.Storage.Internal;

using Microsoft.EntityFrameworkCore.Storage;

public class IgniteGuidTypeMapping : GuidTypeMapping
{
    public IgniteGuidTypeMapping(string storeType)
        : base(storeType, System.Data.DbType.Guid)
    {
    }
}
