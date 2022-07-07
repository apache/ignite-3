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
    public enum SqlColumnType
    {
        /** Boolean. */
        Boolean = 0,

        /** 8-bit signed integer. */
        Int8 = 1,

        /** 16-bit signed integer. */
        Int16 = 2,

        /** 32-bit signed integer. */
        Int32 = 3,

        /** 64-bit signed integer. */
        Int64 = 4,

        /** 32-bit single-precision floating-point number. */
        Float = 5,

        /** 64-bit double-precision floating-point number. */
        Double = 6,

        /** A decimal floating-point number. */
        Decimal = 7,

        /** Timezone-free date. */
        Date = 8,

        /** Timezone-free time with precision. */
        Time = 9,

        /** Timezone-free datetime. */
        Datetime = 10,

        /** Number of ticks since Jan 1, 1970 00:00:00.000 (with no timezone). Tick unit depends on precision. */
        Timestamp = 11,

        /** 128-bit UUID. */
        Uuid = 12,

        /** Bit mask. */
        Bitmask = 13,

        /** String. */
        String = 14,

        /** Binary data. */
        ByteArray = 15,

        /** Date interval. */
        Period = 16,

        /** Time interval. */
        Duration = 17,

        /** Number. */
        Number = 18
    }
}
