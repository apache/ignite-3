
namespace Apache.Ignite.EntityFrameworkCore.Storage.Internal;

using System.Data;
using Microsoft.EntityFrameworkCore.Storage;

public class IgniteDateOnlyTypeMapping : DateOnlyTypeMapping
{
    private const string DateOnlyFormatConst = @"'{0:yyyy\-MM\-dd}'";

    public static new IgniteDateOnlyTypeMapping Default { get; } = new(IgniteTypeMappingSource.TextTypeName);

    public IgniteDateOnlyTypeMapping(
        string storeType,
        DbType? dbType = System.Data.DbType.Date)
        : base(storeType, dbType)
    {
    }

    protected IgniteDateOnlyTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new IgniteDateOnlyTypeMapping(parameters);

    protected override string SqlLiteralFormatString
        => DateOnlyFormatConst;
}
