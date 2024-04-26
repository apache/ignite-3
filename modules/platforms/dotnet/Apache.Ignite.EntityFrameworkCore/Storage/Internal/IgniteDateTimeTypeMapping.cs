
namespace Apache.Ignite.EntityFrameworkCore.Storage.Internal;

using System;
using System.Data;
using Microsoft.EntityFrameworkCore.Storage;

public class IgniteDateTimeTypeMapping : DateTimeTypeMapping
{
    private const string DateTimeFormatConst = @"'{0:yyyy\-MM\-dd HH\:mm\:ss.FFFFFFF}'";

    public static new IgniteDateTimeTypeMapping Default { get; } = new(IgniteTypeMappingSource.TextTypeName);

    public IgniteDateTimeTypeMapping(
        string storeType,
        DbType? dbType = System.Data.DbType.DateTime)
        : this(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(typeof(DateTime)),
                storeType,
                dbType: dbType))
    {
    }

    protected IgniteDateTimeTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new IgniteDateTimeTypeMapping(parameters);

    protected override string SqlLiteralFormatString
        => DateTimeFormatConst;
}
