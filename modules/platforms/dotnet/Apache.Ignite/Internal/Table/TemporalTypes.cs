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

namespace Apache.Ignite.Internal.Table;

using System;
using Ignite.Sql;
using NodaTime;

/// <summary>
/// Temporal type utils.
/// </summary>
internal static class TemporalTypes
{
    /// <summary>
    /// Max <see cref="ColumnType.Time"/> type precision.
    /// </summary>
    public const int MaxTimePrecision = 3;

    /// <summary>
    /// Normalize nanoseconds regarding the precision.
    /// </summary>
    /// <param name="nanos">Nanoseconds.</param>
    /// <param name="precision">Precision.</param>
    /// <returns>Normalized nanoseconds.</returns>
    public static int NormalizeNanos(int nanos, int precision) =>
        precision switch
        {
            0 => 0,
            1 => (nanos / 100_000_000) * 100_000_000, // 100ms precision.
            2 => (nanos / 10_000_000) * 10_000_000, // 10ms precision.
            3 => (nanos / 1_000_000) * 1_000_000, // 1ms precision.
            4 => (nanos / 100_000) * 100_000, // 100us precision.
            5 => (nanos / 10_000) * 10_000, // 10us precision.
            6 => (nanos / 1_000) * 1_000, // 1us precision.
            7 => (nanos / 100) * 100, // 100ns precision.
            8 => (nanos / 10) * 10, // 10ns precision.
            9 => nanos, // 1ns precision
            _ => throw new ArgumentException("Unsupported fractional seconds precision: " + precision)
        };

    /// <summary>
    /// Deconstructs Instant into seconds and nanoseconds.
    /// </summary>
    /// <param name="value">Value.</param>
    /// <param name="precision">Column precision.</param>
    /// <returns>Seconds and nanos.</returns>
    public static (long Seconds, int Nanos) ToSecondsAndNanos(this Instant value, int precision)
    {
        // Logic taken from
        // https://github.com/nodatime/nodatime.serialization/blob/main/src/NodaTime.Serialization.Protobuf/NodaExtensions.cs#L69
        // (Apache License).
        // See discussion: https://github.com/nodatime/nodatime/issues/1644#issuecomment-1260524451
        long seconds = value.ToUnixTimeSeconds();
        Duration remainder = value - Instant.FromUnixTimeSeconds(seconds);
        int nanos = NormalizeNanos((int)remainder.NanosecondOfDay, precision);

        return (seconds, nanos);
    }
}
