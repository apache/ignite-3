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
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// SQL column type.
    /// </summary>
    [SuppressMessage(
        "Microsoft.Naming",
        "CA1720:IdentifiersShouldNotContainTypeNames",
        Justification = "Ignite-specific SQL column type names are required.")]
    public enum ColumnType
    {
        /** Null. */
        Null = 0,

        /** Boolean. */
        Boolean = 1,

        /** 8-bit signed integer. */
        Int8 = 2,

        /** 16-bit signed integer. */
        Int16 = 3,

        /** 32-bit signed integer. */
        Int32 = 4,

        /** 64-bit signed integer. */
        Int64 = 5,

        /** 32-bit single-precision floating-point number. */
        Float = 6,

        /** 64-bit double-precision floating-point number. */
        Double = 7,

        /** A decimal floating-point number. */
        Decimal = 8,

        /** Timezone-free date. */
        Date = 9,

        /** Timezone-free time with precision. */
        Time = 10,

        /** Timezone-free datetime. */
        Datetime = 11,

        /** Number of ticks since Jan 1, 1970 00:00:00.000 (with no timezone). Tick unit depends on precision. */
        Timestamp = 12,

        /** 128-bit UUID. */
        Uuid = 13,

        /** Bit mask. */
        Bitmask = 14,

        /** String. */
        String = 15,

        /** Binary data. */
        ByteArray = 16,

        /** Date interval. */
        Period = 17,

        /** Time interval. */
        Duration = 18,

        /** Number. */
        Number = 19
    }
}
