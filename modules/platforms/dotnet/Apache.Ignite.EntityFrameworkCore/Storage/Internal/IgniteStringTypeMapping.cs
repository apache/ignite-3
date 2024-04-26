
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

    /// <summary>
    ///     Initializes a new instance of the <see cref="IgniteStringTypeMapping" /> class.
    /// </summary>
    /// <param name="parameters">Parameter object for <see cref="RelationalTypeMapping" />.</param>
    protected IgniteStringTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    /// <summary>
    ///     Creates a copy of this mapping.
    /// </summary>
    /// <param name="parameters">The parameters for this mapping.</param>
    /// <returns>The newly created mapping.</returns>
    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new IgniteStringTypeMapping(parameters);
}
