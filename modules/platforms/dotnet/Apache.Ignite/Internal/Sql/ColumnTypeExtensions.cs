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

namespace Apache.Ignite.Internal.Sql;

using System;
using System.Collections.Generic;
using System.Linq;
using Ignite.Sql;
using NodaTime;

/// <summary>
/// Extension methods for <see cref="ColumnType"/>.
/// </summary>
internal static class ColumnTypeExtensions
{
    private static readonly IReadOnlyDictionary<Type, ColumnType> ClrToSql =
        Enum.GetValues<ColumnType>()
            .ToDictionary(x => x.ToClrType(), x => x);

    private static readonly IReadOnlyDictionary<Type, string> ClrToSqlName =
        Enum.GetValues<ColumnType>()
            .ToDictionary(x => x.ToClrType(), x => x.ToSqlTypeName());

    /// <summary>
    /// Gets corresponding .NET type.
    /// </summary>
    /// <param name="columnType">SQL column type.</param>
    /// <returns>CLR type.</returns>
    public static Type ToClrType(this ColumnType columnType) => columnType switch
    {
        ColumnType.Null => typeof(void),
        ColumnType.Boolean => typeof(bool),
        ColumnType.Int8 => typeof(sbyte),
        ColumnType.Int16 => typeof(short),
        ColumnType.Int32 => typeof(int),
        ColumnType.Int64 => typeof(long),
        ColumnType.Float => typeof(float),
        ColumnType.Double => typeof(double),
        ColumnType.Decimal => typeof(decimal),
        ColumnType.Date => typeof(LocalDate),
        ColumnType.Time => typeof(LocalTime),
        ColumnType.Datetime => typeof(LocalDateTime),
        ColumnType.Timestamp => typeof(Instant),
        ColumnType.Uuid => typeof(Guid),
        ColumnType.String => typeof(string),
        ColumnType.ByteArray => typeof(byte[]),
        ColumnType.Period => typeof(Period),
        ColumnType.Duration => typeof(Duration),
        _ => throw new InvalidOperationException($"Invalid {nameof(ColumnType)}: {columnType}")
    };

    /// <summary>
    /// Gets corresponding .NET type.
    /// </summary>
    /// <param name="columnType">SQL column type.</param>
    /// <param name="nullable">Whether the SQL column is nullable.</param>
    /// <returns>CLR type.</returns>
    public static Type ToClrType(this ColumnType columnType, bool nullable)
    {
        var clrType = columnType.ToClrType();

        return nullable && clrType.IsValueType ? typeof(Nullable<>).MakeGenericType(clrType) : clrType;
    }

    /// <summary>
    /// Gets corresponding SQL type name.
    /// </summary>
    /// <param name="columnType">SQL column type.</param>
    /// <returns>SQL type name.</returns>
    public static string ToSqlTypeName(this ColumnType columnType) => columnType switch
    {
        ColumnType.Null => "null",
        ColumnType.Boolean => "boolean",
        ColumnType.Int8 => "tinyint",
        ColumnType.Int16 => "smallint",
        ColumnType.Int32 => "int",
        ColumnType.Int64 => "bigint",
        ColumnType.Float => "real",
        ColumnType.Double => "double",
        ColumnType.Decimal => "decimal",
        ColumnType.Date => "date",
        ColumnType.Time => "time",
        ColumnType.Datetime => "timestamp",
        ColumnType.Timestamp => "timestamp_tz",
        ColumnType.Uuid => "uuid",
        ColumnType.String => "varchar",
        ColumnType.ByteArray => "varbinary",
        ColumnType.Period => "interval",
        ColumnType.Duration => "duration",
        _ => throw new InvalidOperationException($"Unsupported {nameof(ColumnType)}: {columnType}")
    };

    /// <summary>
    /// Gets corresponding SQL type name.
    /// </summary>
    /// <param name="type">CLR type.</param>
    /// <returns>SQL type name.</returns>
    public static string ToSqlTypeName(this Type type) =>
        ClrToSqlName.TryGetValue(Nullable.GetUnderlyingType(type) ?? type, out var sqlTypeName)
            ? sqlTypeName
            : throw new InvalidOperationException($"Type is not supported in SQL: {type}");

    /// <summary>
    /// Gets corresponding <see cref="ColumnType"/>.
    /// </summary>
    /// <param name="type">Type.</param>
    /// <returns>SQL column type, or null.</returns>
    public static ColumnType? ToColumnType(this Type type) =>
        ClrToSql.TryGetValue(Nullable.GetUnderlyingType(type) ?? type, out var sqlType) ? sqlType : null;

    /// <summary>
    /// Gets a value indicating whether specified column type is an integer of any size (int8 to int64).
    /// </summary>
    /// <param name="columnType">SQL column type.</param>
    /// <returns>Whether the type is integer.</returns>
    public static bool IsAnyInt(this ColumnType columnType) =>
        columnType is ColumnType.Int8 or ColumnType.Int16 or ColumnType.Int32 or ColumnType.Int64;

    /// <summary>
    /// Gets a value indicating whether specified column type is a floating point of any size (float32 to float64).
    /// </summary>
    /// <param name="columnType">SQL column type.</param>
    /// <returns>Whether the type is floating point.</returns>
    public static bool IsAnyFloat(this ColumnType columnType) =>
        columnType is ColumnType.Float or ColumnType.Double;
}
