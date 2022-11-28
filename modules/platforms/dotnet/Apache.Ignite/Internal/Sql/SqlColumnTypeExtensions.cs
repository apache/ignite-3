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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using Ignite.Sql;
using NodaTime;

/// <summary>
/// Extension methods for <see cref="SqlColumnType"/>.
/// </summary>
internal static class SqlColumnTypeExtensions
{
    private static readonly IReadOnlyDictionary<Type, SqlColumnType> ClrToSql =
        Enum.GetValues<SqlColumnType>().ToDictionary(x => x.ToClrType(), x => x);

    private static readonly IReadOnlyDictionary<Type, string> ClrToSqlName =
        Enum.GetValues<SqlColumnType>()
            .Where(x => x != SqlColumnType.Period && x != SqlColumnType.Duration)
            .ToDictionary(x => x.ToClrType(), x => x.ToSqlTypeName());

    /// <summary>
    /// Gets corresponding .NET type.
    /// </summary>
    /// <param name="sqlColumnType">SQL column type.</param>
    /// <returns>CLR type.</returns>
    public static Type ToClrType(this SqlColumnType sqlColumnType) => sqlColumnType switch
    {
        SqlColumnType.Boolean => typeof(bool),
        SqlColumnType.Int8 => typeof(sbyte),
        SqlColumnType.Int16 => typeof(short),
        SqlColumnType.Int32 => typeof(int),
        SqlColumnType.Int64 => typeof(long),
        SqlColumnType.Float => typeof(float),
        SqlColumnType.Double => typeof(double),
        SqlColumnType.Decimal => typeof(decimal),
        SqlColumnType.Date => typeof(LocalDate),
        SqlColumnType.Time => typeof(LocalTime),
        SqlColumnType.Datetime => typeof(LocalDateTime),
        SqlColumnType.Timestamp => typeof(Instant),
        SqlColumnType.Uuid => typeof(Guid),
        SqlColumnType.Bitmask => typeof(BitArray),
        SqlColumnType.String => typeof(string),
        SqlColumnType.ByteArray => typeof(byte[]),
        SqlColumnType.Period => typeof(Period),
        SqlColumnType.Duration => typeof(Duration),
        SqlColumnType.Number => typeof(BigInteger),
        _ => throw new InvalidOperationException($"Invalid {nameof(SqlColumnType)}: {sqlColumnType}")
    };

    /// <summary>
    /// Gets corresponding SQL type name.
    /// </summary>
    /// <param name="sqlColumnType">SQL column type.</param>
    /// <returns>SQL type name.</returns>
    public static string ToSqlTypeName(this SqlColumnType sqlColumnType) => sqlColumnType switch
    {
        SqlColumnType.Boolean => "boolean",
        SqlColumnType.Int8 => "tinyint",
        SqlColumnType.Int16 => "smallint",
        SqlColumnType.Int32 => "int",
        SqlColumnType.Int64 => "bigint",
        SqlColumnType.Float => "real",
        SqlColumnType.Double => "double",
        SqlColumnType.Decimal => "decimal",
        SqlColumnType.Date => "date",
        SqlColumnType.Time => "time",
        SqlColumnType.Datetime => "timestamp",
        SqlColumnType.Timestamp => "timestamp_tz",
        SqlColumnType.Uuid => "uuid",
        SqlColumnType.Bitmask => "bitmap",
        SqlColumnType.String => "varchar",
        SqlColumnType.ByteArray => "varbinary",
        SqlColumnType.Number => "numeric",
        _ => throw new InvalidOperationException($"Unsupported {nameof(SqlColumnType)}: {sqlColumnType}")
    };

    /// <summary>
    /// Gets corresponding SQL type name.
    /// </summary>
    /// <param name="type">CLR type.</param>
    /// <returns>SQL type name.</returns>
    public static string ToSqlTypeName(this Type type) =>
        ClrToSqlName.TryGetValue(type, out var sqlTypeName)
            ? sqlTypeName
            : throw new InvalidOperationException($"Type is not supported in SQL: {type}");

    /// <summary>
    /// Gets corresponding <see cref="SqlColumnType"/>.
    /// </summary>
    /// <param name="type">Type.</param>
    /// <returns>SQL column type, or null.</returns>
    public static SqlColumnType? ToSqlColumnType(this Type type) =>
        ClrToSql.TryGetValue(type, out var sqlType) ? sqlType : null;
}
