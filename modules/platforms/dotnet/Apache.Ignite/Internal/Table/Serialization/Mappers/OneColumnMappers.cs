/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Internal.Table.Serialization.Mappers;

using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using Ignite.Table.Mapper;

/// <summary>
/// Primitive mapper helper.
/// </summary>
internal static class OneColumnMappers
{
    private static readonly OneColumnMapper<sbyte> SByteMapper = new(
        (ref RowReader reader, IMapperSchema _) => reader.ReadByte()!.Value,
        (sbyte obj, ref RowWriter writer, IMapperSchema _) => writer.WriteByte(obj));

    private static readonly OneColumnMapper<sbyte?> SByteNullableMapper = new(
        (ref RowReader reader, IMapperSchema _) => reader.ReadByte(),
        (sbyte? obj, ref RowWriter writer, IMapperSchema _) => writer.WriteByte(obj));

    private static readonly OneColumnMapper<bool> BoolMapper = new(
        (ref RowReader reader, IMapperSchema _) => reader.ReadBool()!.Value,
        (bool obj, ref RowWriter writer, IMapperSchema _) => writer.WriteBool(obj));

    private static readonly OneColumnMapper<bool?> BoolNullableMapper = new(
        (ref RowReader reader, IMapperSchema _) => reader.ReadBool(),
        (bool? obj, ref RowWriter writer, IMapperSchema _) => writer.WriteBool(obj));

    private static readonly OneColumnMapper<short> ShortMapper = new(
        (ref RowReader reader, IMapperSchema _) => reader.ReadShort()!.Value,
        (short obj, ref RowWriter writer, IMapperSchema _) => writer.WriteShort(obj));

    private static readonly OneColumnMapper<short?> ShortNullableMapper = new(
        (ref RowReader reader, IMapperSchema _) => reader.ReadShort(),
        (short? obj, ref RowWriter writer, IMapperSchema _) => writer.WriteShort(obj));

    private static readonly OneColumnMapper<int> IntMapper = new(
        (ref RowReader reader, IMapperSchema _) => reader.ReadInt()!.Value,
        (int obj, ref RowWriter writer, IMapperSchema _) => writer.WriteInt(obj));

    private static readonly OneColumnMapper<int?> IntNullableMapper = new(
        (ref RowReader reader, IMapperSchema _) => reader.ReadInt(),
        (int? obj, ref RowWriter writer, IMapperSchema _) => writer.WriteInt(obj));

    private static readonly OneColumnMapper<long> LongMapper = new(
        (ref RowReader reader, IMapperSchema _) => reader.ReadLong()!.Value,
        (long obj, ref RowWriter writer, IMapperSchema _) => writer.WriteLong(obj));

    private static readonly OneColumnMapper<long?> LongNullableMapper = new(
        (ref RowReader reader, IMapperSchema _) => reader.ReadLong(),
        (long? obj, ref RowWriter writer, IMapperSchema _) => writer.WriteLong(obj));

    private static readonly OneColumnMapper<float> FloatMapper = new(
        (ref RowReader reader, IMapperSchema _) => reader.ReadFloat()!.Value,
        (float obj, ref RowWriter writer, IMapperSchema _) => writer.WriteFloat(obj));

    private static readonly OneColumnMapper<float?> FloatNullableMapper = new(
        (ref RowReader reader, IMapperSchema _) => reader.ReadFloat(),
        (float? obj, ref RowWriter writer, IMapperSchema _) => writer.WriteFloat(obj));

    private static readonly OneColumnMapper<double> DoubleMapper = new(
        (ref RowReader reader, IMapperSchema _) => reader.ReadDouble()!.Value,
        (double obj, ref RowWriter writer, IMapperSchema _) => writer.WriteDouble(obj));

    private static readonly OneColumnMapper<double?> DoubleNullableMapper = new(
        (ref RowReader reader, IMapperSchema _) => reader.ReadDouble(),
        (double? obj, ref RowWriter writer, IMapperSchema _) => writer.WriteDouble(obj));

    private static readonly OneColumnMapper<string?> StringMapper = new(
        (ref RowReader reader, IMapperSchema _) => reader.ReadString(),
        (string? obj, ref RowWriter writer, IMapperSchema _) => writer.WriteString(obj));

    private static readonly OneColumnMapper<byte[]?> ByteArrayMapper = new(
        (ref RowReader reader, IMapperSchema _) => reader.ReadBytes(),
        (byte[]? obj, ref RowWriter writer, IMapperSchema _) => writer.WriteBytes(obj));

    private static readonly OneColumnMapper<Guid> GuidMapper = new(
        (ref RowReader reader, IMapperSchema _) => reader.ReadGuid()!.Value,
        (Guid obj, ref RowWriter writer, IMapperSchema _) => writer.WriteGuid(obj));

    private static readonly OneColumnMapper<Guid?> GuidNullableMapper = new(
        (ref RowReader reader, IMapperSchema _) => reader.ReadGuid(),
        (Guid? obj, ref RowWriter writer, IMapperSchema _) => writer.WriteGuid(obj));

    private static readonly OneColumnMapper<decimal> DecimalMapper = new(
        (ref RowReader reader, IMapperSchema _) => reader.ReadDecimal()!.Value,
        (decimal obj, ref RowWriter writer, IMapperSchema _) => writer.WriteDecimal(obj));

    private static readonly OneColumnMapper<decimal?> DecimalNullableMapper = new(
        (ref RowReader reader, IMapperSchema _) => reader.ReadDecimal(),
        (decimal? obj, ref RowWriter writer, IMapperSchema _) => writer.WriteDecimal(obj));

    private static readonly FrozenDictionary<Type, object> Mappers = new Dictionary<Type, object>
    {
        { typeof(sbyte), SByteMapper },
        { typeof(sbyte?), SByteNullableMapper },
        { typeof(bool), BoolMapper },
        { typeof(bool?), BoolNullableMapper },
        { typeof(short), ShortMapper },
        { typeof(short?), ShortNullableMapper },
        { typeof(int), IntMapper },
        { typeof(int?), IntNullableMapper },
        { typeof(long), LongMapper },
        { typeof(long?), LongNullableMapper },
        { typeof(float), FloatMapper },
        { typeof(float?), FloatNullableMapper },
        { typeof(double), DoubleMapper },
        { typeof(double?), DoubleNullableMapper },
        { typeof(string), StringMapper },
        { typeof(byte[]), ByteArrayMapper },
        { typeof(Guid), GuidMapper },
        { typeof(Guid?), GuidNullableMapper },
        { typeof(decimal), DecimalMapper },
        { typeof(decimal?), DecimalNullableMapper }
    }.ToFrozenDictionary();

    /// <summary>
    /// Creates a primitive mapper for the specified type if supported; otherwise, returns null.
    /// </summary>
    /// <typeparam name="T">Type.</typeparam>
    /// <returns>Mapper or null.</returns>
    public static OneColumnMapper<T>? TryCreate<T>() => Mappers.GetValueOrDefault(typeof(T)) as OneColumnMapper<T>;
}
