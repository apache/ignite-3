namespace Apache.Ignite.EntityFrameworkCore.Storage.Internal;

using System.Data;
using Microsoft.EntityFrameworkCore.Storage;

public class IgniteGuidTypeMapping : GuidTypeMapping
{
    public static readonly IgniteGuidTypeMapping Default = new(
        IgniteTypeMappingSource.GuidTypeName,
        System.Data.DbType.Guid);

    public IgniteGuidTypeMapping(string storeType, DbType? dbType)
        : base(storeType, dbType)
    {
    }
}
