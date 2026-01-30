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
            default:
                throw new NotSupportedException("Unsupported column type: " + column.Type);
        }
    }

    public T Read(ref RowReader rowReader, IMapperSchema schema)
    {
        ValidateSchema(schema);

        var column = schema.Columns[0];

        return column.Type switch
        {
            ColumnType.Boolean => (T?)(object?)rowReader.ReadBool(),
            ColumnType.Int8 => (T?)(object?)rowReader.ReadByte(),
            ColumnType.Null => expr,
            ColumnType.Int16 => expr,
            ColumnType.Int32 => expr,
            ColumnType.Int64 => expr,
            ColumnType.Float => expr,
            ColumnType.Double => expr,
            ColumnType.Decimal => expr,
            ColumnType.Date => expr,
            ColumnType.Time => expr,
            ColumnType.Datetime => expr,
            ColumnType.Timestamp => expr,
            ColumnType.Uuid => expr,
            ColumnType.String => expr,
            ColumnType.ByteArray => expr,
            ColumnType.Period => expr,
            ColumnType.Duration => expr,
            _ => throw new NotSupportedException("Unsupported column type: " + column.Type)
        };
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
