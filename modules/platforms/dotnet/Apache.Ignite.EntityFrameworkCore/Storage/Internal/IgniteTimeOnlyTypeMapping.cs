
namespace Apache.Ignite.EntityFrameworkCore.Storage.Internal;

using System;
using System.Data;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;

public class IgniteTimeOnlyTypeMapping : TimeOnlyTypeMapping
{
    public static new IgniteTimeOnlyTypeMapping Default { get; } = new(IgniteTypeMappingSource.TextTypeName);

    public IgniteTimeOnlyTypeMapping(
        string storeType,
        DbType? dbType = System.Data.DbType.Time)
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(typeof(TimeOnly), jsonValueReaderWriter: JsonTimeOnlyReaderWriter.Instance),
                storeType,
                dbType: dbType))
    {
    }

    protected IgniteTimeOnlyTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new IgniteTimeOnlyTypeMapping(parameters);

    /// <inheritdoc />
    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var timeOnly = (TimeOnly)value;

        return timeOnly.Ticks % TimeSpan.TicksPerSecond == 0
            ? FormattableString.Invariant($@"'{value:HH\:mm\:ss}'")
            : FormattableString.Invariant($@"'{value:HH\:mm\:ss\.fffffff}'");
    }
}
