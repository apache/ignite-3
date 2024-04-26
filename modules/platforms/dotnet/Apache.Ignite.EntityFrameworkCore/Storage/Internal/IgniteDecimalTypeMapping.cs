
namespace Apache.Ignite.EntityFrameworkCore.Storage.Internal;

using System.Data;
using Microsoft.EntityFrameworkCore.Storage;

public class IgniteDecimalTypeMapping : DecimalTypeMapping
{
    public static new IgniteDecimalTypeMapping Default { get; } = new(IgniteTypeMappingSource.TextTypeName);

    public IgniteDecimalTypeMapping(string storeType, DbType? dbType = System.Data.DbType.Decimal)
        : this(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(decimal)),
                storeType,
                dbType: dbType))
    {
    }

    protected IgniteDecimalTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    /// <summary>
    ///     Creates a copy of this mapping.
    /// </summary>
    /// <param name="parameters">The parameters for this mapping.</param>
    /// <returns>The newly created mapping.</returns>
    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new IgniteDecimalTypeMapping(parameters);

    protected override string SqlLiteralFormatString
        => "'" + base.SqlLiteralFormatString + "'";
}
