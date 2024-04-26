
namespace Apache.Ignite.EntityFrameworkCore.Storage.Internal;

using System.Data;
using Microsoft.EntityFrameworkCore.Storage;

public class IgniteByteArrayTypeMapping : ByteArrayTypeMapping
{
    public static new IgniteByteArrayTypeMapping Default { get; } = new(IgniteTypeMappingSource.BlobTypeName);

    public IgniteByteArrayTypeMapping(string storeType, DbType? dbType = System.Data.DbType.Binary)
        : this(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(byte[])),
                storeType,
                dbType: dbType))
    {
    }

    protected IgniteByteArrayTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new IgniteByteArrayTypeMapping(parameters);
}
