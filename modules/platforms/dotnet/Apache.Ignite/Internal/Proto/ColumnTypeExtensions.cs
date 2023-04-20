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

namespace Apache.Ignite.Internal.Proto;

using System;
using System.Collections;
using System.Numerics;
using Ignite.Sql;
using NodaTime;

/// <summary>
/// Extension methods for <see cref="ColumnType"/>.
/// </summary>
internal static class ColumnTypeExtensions
{
    /// <summary>
    /// Converts client data type to <see cref="Type"/>.
    /// </summary>
    /// <param name="columnType">Client data type.</param>
    /// <returns>Corresponding CLR type.</returns>
    public static Type ToType(this ColumnType columnType) => columnType switch
    {
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
        ColumnType.Bitmask => typeof(BitArray),
        ColumnType.String => typeof(string),
        ColumnType.ByteArray => typeof(byte[]),
        ColumnType.Period => typeof(Period),
        ColumnType.Duration => typeof(Duration),
        ColumnType.Number => typeof(BigInteger),
        _ => throw new ArgumentOutOfRangeException(nameof(columnType), columnType, null)
    };
}
