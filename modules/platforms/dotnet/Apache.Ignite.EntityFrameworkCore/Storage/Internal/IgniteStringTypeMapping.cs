
namespace Apache.Ignite.EntityFrameworkCore.Storage.Internal;

using System.Data;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;

public class IgniteStringTypeMapping : StringTypeMapping
{
    public static new IgniteStringTypeMapping Default { get; } = new(IgniteTypeMappingSource.TextTypeName);

    public IgniteStringTypeMapping(
        string storeType,
        DbType? dbType = null,
        bool unicode = false,
        int? size = null)
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(string), jsonValueReaderWriter: JsonStringReaderWriter.Instance), storeType, StoreTypePostfix.None, dbType,
                unicode, size))
    {
    }

    protected IgniteStringTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new IgniteStringTypeMapping(parameters);
}
