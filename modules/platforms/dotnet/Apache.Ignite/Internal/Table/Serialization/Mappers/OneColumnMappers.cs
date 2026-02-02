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
using NodaTime;

/// <summary>
/// Primitive mapper helper.
/// </summary>
internal static class OneColumnMappers
{
    private static readonly OneColumnMapper<sbyte> SByteMapper = new(
        (ref RowReader reader, IMapperColumn column) => UnwrapNullable(reader.ReadByte(), column),
        (sbyte obj, ref RowWriter writer, IMapperColumn _) => writer.WriteByte(obj));

    private static readonly OneColumnMapper<sbyte?> SByteNullableMapper = new(
        (ref RowReader reader, IMapperColumn _) => reader.ReadByte(),
        (sbyte? obj, ref RowWriter writer, IMapperColumn _) => writer.WriteByte(obj));

    private static readonly OneColumnMapper<bool> BoolMapper = new(
        (ref RowReader reader, IMapperColumn column) => UnwrapNullable(reader.ReadBool(), column),
        (bool obj, ref RowWriter writer, IMapperColumn _) => writer.WriteBool(obj));

    private static readonly OneColumnMapper<bool?> BoolNullableMapper = new(
        (ref RowReader reader, IMapperColumn _) => reader.ReadBool(),
        (bool? obj, ref RowWriter writer, IMapperColumn _) => writer.WriteBool(obj));

    private static readonly OneColumnMapper<short> ShortMapper = new(
        (ref RowReader reader, IMapperColumn column) => UnwrapNullable(reader.ReadShort(), column),
        (short obj, ref RowWriter writer, IMapperColumn _) => writer.WriteShort(obj));

    private static readonly OneColumnMapper<short?> ShortNullableMapper = new(
        (ref RowReader reader, IMapperColumn _) => reader.ReadShort(),
        (short? obj, ref RowWriter writer, IMapperColumn _) => writer.WriteShort(obj));

    private static readonly OneColumnMapper<int> IntMapper = new(
        (ref RowReader reader, IMapperColumn column) => UnwrapNullable(reader.ReadInt(), column),
        (int obj, ref RowWriter writer, IMapperColumn _) => writer.WriteInt(obj));

    private static readonly OneColumnMapper<int?> IntNullableMapper = new(
        (ref RowReader reader, IMapperColumn _) => reader.ReadInt(),
        (int? obj, ref RowWriter writer, IMapperColumn _) => writer.WriteInt(obj));

    private static readonly OneColumnMapper<long> LongMapper = new(
        (ref RowReader reader, IMapperColumn column) => UnwrapNullable(reader.ReadLong(), column),
        (long obj, ref RowWriter writer, IMapperColumn _) => writer.WriteLong(obj));

    private static readonly OneColumnMapper<long?> LongNullableMapper = new(
        (ref RowReader reader, IMapperColumn _) => reader.ReadLong(),
        (long? obj, ref RowWriter writer, IMapperColumn _) => writer.WriteLong(obj));

    private static readonly OneColumnMapper<float> FloatMapper = new(
        (ref RowReader reader, IMapperColumn column) => UnwrapNullable(reader.ReadFloat(), column),
        (float obj, ref RowWriter writer, IMapperColumn _) => writer.WriteFloat(obj));

    private static readonly OneColumnMapper<float?> FloatNullableMapper = new(
        (ref RowReader reader, IMapperColumn _) => reader.ReadFloat(),
        (float? obj, ref RowWriter writer, IMapperColumn _) => writer.WriteFloat(obj));

    private static readonly OneColumnMapper<double> DoubleMapper = new(
        (ref RowReader reader, IMapperColumn column) => UnwrapNullable(reader.ReadDouble(), column),
        (double obj, ref RowWriter writer, IMapperColumn _) => writer.WriteDouble(obj));

    private static readonly OneColumnMapper<double?> DoubleNullableMapper = new(
        (ref RowReader reader, IMapperColumn _) => reader.ReadDouble(),
        (double? obj, ref RowWriter writer, IMapperColumn _) => writer.WriteDouble(obj));

    private static readonly OneColumnMapper<string?> StringMapper = new(
        (ref RowReader reader, IMapperColumn _) => reader.ReadString(),
        (string? obj, ref RowWriter writer, IMapperColumn _) => writer.WriteString(obj));

    private static readonly OneColumnMapper<byte[]?> ByteArrayMapper = new(
        (ref RowReader reader, IMapperColumn _) => reader.ReadBytes(),
        (byte[]? obj, ref RowWriter writer, IMapperColumn _) => writer.WriteBytes(obj));

    private static readonly OneColumnMapper<Guid> GuidMapper = new(
        (ref RowReader reader, IMapperColumn column) => UnwrapNullable(reader.ReadGuid(), column),
        (Guid obj, ref RowWriter writer, IMapperColumn _) => writer.WriteGuid(obj));

    private static readonly OneColumnMapper<Guid?> GuidNullableMapper = new(
        (ref RowReader reader, IMapperColumn _) => reader.ReadGuid(),
        (Guid? obj, ref RowWriter writer, IMapperColumn _) => writer.WriteGuid(obj));

    private static readonly OneColumnMapper<decimal> DecimalMapper = new(
        (ref RowReader reader, IMapperColumn column) => UnwrapNullable(reader.ReadDecimal(), column),
        (decimal obj, ref RowWriter writer, IMapperColumn _) => writer.WriteDecimal(obj));

    private static readonly OneColumnMapper<decimal?> DecimalNullableMapper = new(
        (ref RowReader reader, IMapperColumn _) => reader.ReadDecimal(),
        (decimal? obj, ref RowWriter writer, IMapperColumn _) => writer.WriteDecimal(obj));

    private static readonly OneColumnMapper<BigDecimal> BigDecimalMapper = new(
        (ref RowReader reader, IMapperColumn column) => UnwrapNullable(reader.ReadBigDecimal(), column),
        (BigDecimal obj, ref RowWriter writer, IMapperColumn _) => writer.WriteBigDecimal(obj));

    private static readonly OneColumnMapper<BigDecimal?> BigDecimalNullableMapper = new(
        (ref RowReader reader, IMapperColumn _) => reader.ReadBigDecimal(),
        (BigDecimal? obj, ref RowWriter writer, IMapperColumn _) => writer.WriteBigDecimal(obj));

    private static readonly OneColumnMapper<LocalDate> LocalDateMapper = new(
        (ref RowReader reader, IMapperColumn column) => UnwrapNullable(reader.ReadDate(), column),
        (LocalDate obj, ref RowWriter writer, IMapperColumn _) => writer.WriteDate(obj));

    private static readonly OneColumnMapper<LocalDate?> LocalDateNullableMapper = new(
        (ref RowReader reader, IMapperColumn _) => reader.ReadDate(),
        (LocalDate? obj, ref RowWriter writer, IMapperColumn _) => writer.WriteDate(obj));

    private static readonly OneColumnMapper<LocalTime> LocalTimeMapper = new(
        (ref RowReader reader, IMapperColumn column) => UnwrapNullable(reader.ReadTime(), column),
        (LocalTime obj, ref RowWriter writer, IMapperColumn _) => writer.WriteTime(obj));

    private static readonly OneColumnMapper<LocalTime?> LocalTimeNullableMapper = new(
        (ref RowReader reader, IMapperColumn _) => reader.ReadTime(),
        (LocalTime? obj, ref RowWriter writer, IMapperColumn _) => writer.WriteTime(obj));

    private static readonly OneColumnMapper<LocalDateTime> LocalDateTimeMapper = new(
        (ref RowReader reader, IMapperColumn column) => UnwrapNullable(reader.ReadDateTime(), column),
        (LocalDateTime obj, ref RowWriter writer, IMapperColumn _) => writer.WriteDateTime(obj));

    private static readonly OneColumnMapper<LocalDateTime?> LocalDateTimeNullableMapper = new(
        (ref RowReader reader, IMapperColumn _) => reader.ReadDateTime(),
        (LocalDateTime? obj, ref RowWriter writer, IMapperColumn _) => writer.WriteDateTime(obj));

    private static readonly OneColumnMapper<Instant> InstantMapper = new(
        (ref RowReader reader, IMapperColumn column) => UnwrapNullable(reader.ReadTimestamp(), column),
        (Instant obj, ref RowWriter writer, IMapperColumn _) => writer.WriteTimestamp(obj));

    private static readonly OneColumnMapper<Instant?> InstantNullableMapper = new(
        (ref RowReader reader, IMapperColumn _) => reader.ReadTimestamp(),
        (Instant? obj, ref RowWriter writer, IMapperColumn _) => writer.WriteTimestamp(obj));

    private static readonly OneColumnMapper<Duration> DurationMapper = new(
        (ref RowReader reader, IMapperColumn column) => UnwrapNullable(reader.ReadDuration(), column),
        (Duration obj, ref RowWriter writer, IMapperColumn _) => writer.WriteDuration(obj));

    private static readonly OneColumnMapper<Duration?> DurationNullableMapper = new(
        (ref RowReader reader, IMapperColumn _) => reader.ReadDuration(),
        (Duration? obj, ref RowWriter writer, IMapperColumn _) => writer.WriteDuration(obj));

    private static readonly OneColumnMapper<Period?> PeriodMapper = new(
        (ref RowReader reader, IMapperColumn _) => reader.ReadPeriod(),
        (Period? obj, ref RowWriter writer, IMapperColumn _) => writer.WritePeriod(obj));

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
        { typeof(decimal?), DecimalNullableMapper },
        { typeof(BigDecimal), BigDecimalMapper },
        { typeof(BigDecimal?), BigDecimalNullableMapper },
        { typeof(LocalDate), LocalDateMapper },
        { typeof(LocalDate?), LocalDateNullableMapper },
        { typeof(LocalTime), LocalTimeMapper },
        { typeof(LocalTime?), LocalTimeNullableMapper },
        { typeof(LocalDateTime), LocalDateTimeMapper },
        { typeof(LocalDateTime?), LocalDateTimeNullableMapper },
        { typeof(Instant), InstantMapper },
        { typeof(Instant?), InstantNullableMapper },
        { typeof(Duration), DurationMapper },
        { typeof(Duration?), DurationNullableMapper },
        { typeof(Period), PeriodMapper }
    }.ToFrozenDictionary();

    /// <summary>
    /// Creates a primitive mapper for the specified type if supported; otherwise, returns null.
    /// </summary>
    /// <typeparam name="T">Type.</typeparam>
    /// <returns>Mapper or null.</returns>
    public static OneColumnMapper<T>? TryCreate<T>() => Mappers.GetValueOrDefault(typeof(T)) as OneColumnMapper<T>;

    private static T UnwrapNullable<T>(T? value, IMapperColumn column)
        where T : struct
    {
        if (value.HasValue)
        {
            return value.Value;
        }

        var message = $"Can't map '{typeof(T)}' to column '{column.Name}' - column is nullable, but field is not.";

        throw new IgniteClientException(ErrorGroups.Client.Configuration, message);
    }
}
