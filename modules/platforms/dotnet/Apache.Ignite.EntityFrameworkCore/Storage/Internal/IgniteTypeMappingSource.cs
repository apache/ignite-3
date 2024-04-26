
namespace Apache.Ignite.EntityFrameworkCore.Storage.Internal;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using Common;
using Microsoft.EntityFrameworkCore.Storage;

public class IgniteTypeMappingSource : RelationalTypeMappingSource
{
    internal const string IntegerTypeName = "INTEGER";
    internal const string RealTypeName = "REAL";
    internal const string BlobTypeName = "BLOB";
    internal const string TextTypeName = "VARCHAR";
    internal const string GuidTypeName = "UUID";

    private static readonly LongTypeMapping Integer = new(IntegerTypeName);
    private static readonly DoubleTypeMapping Real = new(RealTypeName);
    private static readonly IgniteByteArrayTypeMapping Blob = IgniteByteArrayTypeMapping.Default;
    private static readonly IgniteStringTypeMapping Text = IgniteStringTypeMapping.Default;

    private readonly Dictionary<Type, RelationalTypeMapping> _clrTypeMappings = new()
    {
        { typeof(string), Text },
        { typeof(byte[]), Blob },
        { typeof(bool), new BoolTypeMapping(IntegerTypeName) },
        { typeof(byte), new ByteTypeMapping(IntegerTypeName) },
        { typeof(char), new CharTypeMapping(TextTypeName) },
        { typeof(int), new IntTypeMapping(IntegerTypeName) },
        { typeof(long), Integer },
        { typeof(sbyte), new SByteTypeMapping(IntegerTypeName) },
        { typeof(short), new ShortTypeMapping(IntegerTypeName) },
        { typeof(uint), new UIntTypeMapping(IntegerTypeName) },
        { typeof(ulong), IgniteULongTypeMapping.Default },
        { typeof(ushort), new UShortTypeMapping(IntegerTypeName) },
        { typeof(DateTime), IgniteDateTimeTypeMapping.Default },
        { typeof(DateTimeOffset), IgniteDateTimeOffsetTypeMapping.Default },
        { typeof(TimeSpan), new TimeSpanTypeMapping(TextTypeName) },
        { typeof(DateOnly), IgniteDateOnlyTypeMapping.Default },
        { typeof(TimeOnly), IgniteTimeOnlyTypeMapping.Default },
        { typeof(decimal), IgniteDecimalTypeMapping.Default },
        { typeof(double), Real },
        { typeof(float), new FloatTypeMapping(RealTypeName) },
        { typeof(Guid), IgniteGuidTypeMapping.Default },
    };

    private readonly Dictionary<string, RelationalTypeMapping> _storeTypeMappings = new(StringComparer.OrdinalIgnoreCase)
    {
        { IntegerTypeName, Integer },
        { RealTypeName, Real },
        { BlobTypeName, Blob },
        { TextTypeName, Text }
    };

    public IgniteTypeMappingSource(
        TypeMappingSourceDependencies dependencies,
        RelationalTypeMappingSourceDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
    }

    protected override RelationalTypeMapping? FindMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        var mapping = base.FindMapping(mappingInfo)
            ?? FindRawMapping(mappingInfo);

        return mapping != null
            && mappingInfo.StoreTypeName != null
                ? mapping.WithStoreTypeAndSize(mappingInfo.StoreTypeName, null)
                : mapping;
    }

    private RelationalTypeMapping? FindRawMapping(RelationalTypeMappingInfo mappingInfo)
    {
        var clrType = mappingInfo.ClrType;
        if (clrType == typeof(byte[]) && mappingInfo.ElementTypeMapping != null)
        {
            return null;
        }

        if (clrType != null
            && _clrTypeMappings.TryGetValue(clrType, out var mapping))
        {
            return mapping;
        }

        var storeTypeName = mappingInfo.StoreTypeName;
        if (storeTypeName != null
            && _storeTypeMappings.TryGetValue(storeTypeName, out mapping)
            && (clrType == null || mapping.ClrType.UnwrapNullableType() == clrType))
        {
            return mapping;
        }

        if (storeTypeName != null)
        {
            var affinityTypeMapping = _typeRules.Select(r => r(storeTypeName)).FirstOrDefault(r => r != null);

            if (affinityTypeMapping != null)
            {
                return clrType == null || affinityTypeMapping.ClrType.UnwrapNullableType() == clrType
                    ? affinityTypeMapping
                    : null;
            }

            if (clrType == null || clrType == typeof(byte[]))
            {
                return Blob;
            }
        }

        return null;
    }

    private readonly Func<string, RelationalTypeMapping?>[] _typeRules =
    {
        name => Contains(name, "INT")
            ? Integer
            : null,
        name => Contains(name, "CHAR")
            || Contains(name, "CLOB")
            || Contains(name, "TEXT")
                ? Text
                : null,
        name => Contains(name, "BLOB")
            ? Blob
            : null,
        name => Contains(name, "REAL")
            || Contains(name, "FLOA")
            || Contains(name, "DOUB")
                ? Real
                : null
    };

    private static bool Contains(string haystack, string needle)
        => haystack.IndexOf(needle, StringComparison.OrdinalIgnoreCase) >= 0;
}
