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

namespace Apache.Ignite.Internal.Table.Serialization;

using System;
using Ignite.Sql;
using Ignite.Table.Mapper;
using NodaTime;

/// <summary>
/// Primitive mapper.
/// </summary>
/// <typeparam name="T">Type.</typeparam>
internal sealed class OneColumnMapper<T> : IMapper<T>
{
    /// <inheritdoc/>
    public void Write(T obj, ref RowWriter rowWriter, IMapperSchema schema)
    {
        ValidateSchema(schema);

        var column = schema.Columns[0];

        switch (column.Type)
        {
            case ColumnType.Boolean:
                rowWriter.WriteBool((bool?)(object?)obj!);
                break;
            case ColumnType.Int8:
                rowWriter.WriteByte((sbyte?)(object?)obj!);
                break;
            case ColumnType.Int16:
                rowWriter.WriteShort((short?)(object?)obj!);
                break;
            case ColumnType.Int32:
                rowWriter.WriteInt((int?)(object?)obj!);
                break;
            case ColumnType.Int64:
                rowWriter.WriteLong((long?)(object?)obj!);
                break;
            case ColumnType.Float:
                rowWriter.WriteFloat((float?)(object?)obj!);
                break;
            case ColumnType.Double:
                rowWriter.WriteDouble((double?)(object?)obj!);
                break;
            case ColumnType.Decimal:
                rowWriter.WriteDecimal((decimal?)(object?)obj!);
                break;
            case ColumnType.Date:
                rowWriter.WriteDate((LocalDate?)(object?)obj!);
                break;
            case ColumnType.Time:
                rowWriter.WriteTime((LocalTime?)(object?)obj!);
                break;
            case ColumnType.Datetime:
                rowWriter.WriteDateTime((LocalDateTime?)(object?)obj!);
                break;
            case ColumnType.Timestamp:
                rowWriter.WriteTimestamp((Instant?)(object?)obj!);
                break;
            case ColumnType.Uuid:
                rowWriter.WriteGuid((Guid?)(object?)obj!);
                break;
            case ColumnType.String:
                rowWriter.WriteString((string?)(object?)obj!);
                break;
            case ColumnType.ByteArray:
                rowWriter.WriteBytes((byte[]?)(object?)obj!);
                break;
            case ColumnType.Period:
                rowWriter.WritePeriod((Period?)(object?)obj!);
                break;
            case ColumnType.Duration:
                rowWriter.WriteDuration((Duration?)(object?)obj!);
                break;
            default:
                throw new NotSupportedException("Unsupported column type: " + column.Type);
        }
    }

    /// <inheritdoc/>
    public T Read(ref RowReader rowReader, IMapperSchema schema)
    {
        ValidateSchema(schema);

        var column = schema.Columns[0];

        var res = column.Type switch
        {
            ColumnType.Boolean => (T?)(object?)rowReader.ReadBool(),
            ColumnType.Int8 => (T?)(object?)rowReader.ReadByte(),
            ColumnType.Int16 => (T?)(object?)rowReader.ReadShort(),
            ColumnType.Int32 => (T?)(object?)rowReader.ReadInt(),
            ColumnType.Int64 => (T?)(object?)rowReader.ReadLong(),
            ColumnType.Float => (T?)(object?)rowReader.ReadFloat(),
            ColumnType.Double => (T?)(object?)rowReader.ReadDouble(),
            ColumnType.Decimal => (T?)(object?)rowReader.ReadDecimal(),
            ColumnType.Date => (T?)(object?)rowReader.ReadDate(),
            ColumnType.Time => (T?)(object?)rowReader.ReadTime(),
            ColumnType.Datetime => (T?)(object?)rowReader.ReadDateTime(),
            ColumnType.Timestamp => (T?)(object?)rowReader.ReadTimestamp(),
            ColumnType.Uuid => (T?)(object?)rowReader.ReadGuid(),
            ColumnType.String => (T?)(object?)rowReader.ReadString(),
            ColumnType.ByteArray => (T?)(object?)rowReader.ReadBytes(),
            ColumnType.Period => (T?)(object?)rowReader.ReadPeriod(),
            ColumnType.Duration => (T?)(object?)rowReader.ReadDuration(),
            ColumnType.Null => default,
            _ => throw new NotSupportedException("Unsupported column type: " + column.Type)
        };

        return res!;
    }

    private static void ValidateSchema(IMapperSchema schema)
    {
        if (schema.Columns.Count > 1)
        {
            // TODO: Is this consistent with auto-generated mapper?
            throw new InvalidOperationException(
                $"Primitive mapper can only be used with single-column schemas, but schema has {schema.Columns.Count} columns.");
        }
    }
}
