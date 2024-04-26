
namespace Apache.Ignite.EntityFrameworkCore.Storage.Internal;

using System;
using System.Data;
using Microsoft.EntityFrameworkCore.Storage;

public class IgniteDateTimeOffsetTypeMapping : DateTimeOffsetTypeMapping
{
    private const string DateTimeOffsetFormatConst = @"'{0:yyyy\-MM\-dd HH\:mm\:ss.FFFFFFFzzz}'";

    public static new IgniteDateTimeOffsetTypeMapping Default { get; } = new(IgniteTypeMappingSource.TextTypeName);

    public IgniteDateTimeOffsetTypeMapping(
        string storeType,
        DbType? dbType = System.Data.DbType.DateTimeOffset)
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(typeof(DateTimeOffset)),
                storeType,
                dbType: dbType))
    {
    }

    protected IgniteDateTimeOffsetTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new IgniteDateTimeOffsetTypeMapping(parameters);

    protected override string SqlLiteralFormatString
        => DateTimeOffsetFormatConst;
}
