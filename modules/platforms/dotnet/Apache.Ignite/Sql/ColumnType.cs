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

namespace Apache.Ignite.Sql
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using NodaTime;

    /// <summary>
    /// SQL column type.
    /// </summary>
    [SuppressMessage(
        "Microsoft.Naming",
        "CA1720:IdentifiersShouldNotContainTypeNames",
        Justification = "Ignite-specific SQL column type names are required.")]
    [SuppressMessage("Design", "CA1027:Mark enums with FlagsAttribute", Justification = "Not a flags enum.")]
    public enum ColumnType
    {
        /// <summary>
        /// Null.
        /// </summary>
        Null = 0,

        /// <summary>
        /// Boolean.
        /// <para />
        /// SQL type: <c>BOOLEAN</c>, .NET type: <see cref="bool"/>.
        /// </summary>
        Boolean = 1,

        /// <summary>
        /// 8-bit signed integer.
        /// <para />
        /// SQL type: <c>TINYINT</c>, .NET type: <see cref="sbyte"/>.
        /// </summary>
        Int8 = 2,

        /// <summary>
        /// 16-bit signed integer.
        /// <para />
        /// SQL type: <c>SMALLINT</c>, .NET type: <see cref="short"/>.
        /// </summary>
        Int16 = 3,

        /// <summary>
        /// 32-bit signed integer.
        /// <para />
        /// SQL type: <c>INTEGER</c>, .NET type: <see cref="int"/>.
        /// </summary>
        Int32 = 4,

        /// <summary>
        /// 64-bit signed integer.
        /// <para />
        /// SQL type: <c>BIGINT</c>, .NET type: <see cref="long"/>.
        /// </summary>
        Int64 = 5,

        /// <summary>
        /// 32-bit single-precision floating-point number.
        /// <para />
        /// SQL type: <c>REAL</c>, .NET type: <see cref="float"/>.
        /// </summary>
        Float = 6,

        /// <summary>
        /// 64-bit double-precision floating-point number.
        /// <para />
        /// SQL type: <c>DOUBLE</c>, .NET type: <see cref="double"/>.
        /// </summary>
        Double = 7,

        /// <summary>
        /// Arbitrary-precision signed decimal number.
        /// <para />
        /// SQL type: <c>DECIMAL</c>, .NET type: <see cref="BigDecimal"/>.
        /// </summary>
        Decimal = 8,

        /// <summary>
        /// Timezone-free date (year, month, day).
        /// <para />
        /// SQL type: <c>DATE</c>, .NET type: <see cref="LocalDate"/>.
        /// </summary>
        Date = 9,

        /// <summary>
        /// Timezone-free time (hour, minute, second, nanosecond).
        /// <para />
        /// SQL type: <c>TIME</c>, .NET type: <see cref="LocalTime"/>.
        /// </summary>
        Time = 10,

        /// <summary>
        /// Timezone-free datetime (<see cref="Date"/> + <see cref="Time"/>).
        /// <para />
        /// SQL type: <c>TIMESTAMP</c>, .NET type: <see cref="LocalDateTime"/>.
        /// </summary>
        Datetime = 11,

        /// <summary>
        /// Number of ticks since Jan 1, 1970 00:00:00.000 (with no timezone).
        /// <para />
        /// SQL type: <c>TIMESTAMP WITH LOCAL TIME ZONE</c>, .NET type: <see cref="Instant"/>.
        /// </summary>
        Timestamp = 12,

        /// <summary>
        /// 128-bit UUID.
        /// <para />
        /// SQL type: <c>UUID</c>, .NET type: <see cref="Guid"/>.
        /// </summary>
        Uuid = 13,

        /// <summary>
        /// String.
        /// <para />
        /// SQL type: <c>VARCHAR</c>, .NET type: <see cref="string"/>.
        /// </summary>
        String = 15,

        /// <summary>
        /// Byte array.
        /// <para />
        /// SQL type: <c>VARBINARY</c>, .NET type: <c>byte[]</c>.
        /// </summary>
        ByteArray = 16,

        /// <summary>
        /// Date interval.
        /// <para />
        /// SQL type: none, .NET type: <see cref="Period"/>.
        /// </summary>
        Period = 17,

        /// <summary>
        /// Time interval.
        /// <para />
        /// SQL type: none, .NET type: <see cref="Duration"/>.
        /// </summary>
        Duration = 18
    }
}
