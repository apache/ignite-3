
namespace Apache.Ignite.EntityFrameworkCore.Storage.Internal;

using System.Data;
using Microsoft.EntityFrameworkCore.Storage;

public class IgniteULongTypeMapping : ULongTypeMapping
{
    public static new IgniteULongTypeMapping Default { get; } = new(IgniteTypeMappingSource.IntegerTypeName);

    public IgniteULongTypeMapping(string storeType, DbType? dbType = System.Data.DbType.UInt64)
        : base(storeType, dbType)
    {
    }

    protected IgniteULongTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new IgniteULongTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
        => new LongTypeMapping(StoreType).GenerateSqlLiteral((long)(ulong)value);
}
