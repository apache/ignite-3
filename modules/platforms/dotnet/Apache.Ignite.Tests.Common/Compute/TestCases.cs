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

namespace Apache.Ignite.Tests.Common.Compute;

using System;
using System.Collections.Generic;
using Apache.Ignite.Table;
using NodaTime;

/// <summary>
/// Compute test cases.
/// </summary>
public static class TestCases
{
    public static List<object> SupportedArgs => [
        sbyte.MinValue,
        sbyte.MaxValue,
        short.MinValue,
        short.MaxValue,
        int.MinValue,
        int.MaxValue,
        long.MinValue,
        long.MaxValue,
        float.MinValue,
        float.MaxValue,
        double.MinValue,
        double.MaxValue,
        123.456m,
        -123.456m,
        decimal.MinValue,
        decimal.MaxValue,
        new BigDecimal(long.MinValue, 10),
        new BigDecimal(long.MaxValue, 20),
        new byte[] { 1, 255 },
        "Ignite 🔥",
        LocalDate.MinIsoValue,
        LocalTime.Noon,
        LocalDateTime.MaxIsoValue,
        Instant.FromUtc(2001, 3, 4, 5, 6),
        Guid.Empty,
        new Guid(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 }),
    ];

    public static IIgniteTuple GetTupleWithAllFieldTypes(Func<object, bool>? filter = null)
    {
        var res = new IgniteTuple();

        for (var i = 0; i < SupportedArgs.Count; i++)
        {
            var val = SupportedArgs[i];

            if (filter != null && !filter(val))
            {
                continue;
            }

            res[$"v{i}"] = val;
        }

        return res;
    }

    public static IIgniteTuple GetNestedTuple(int depth)
    {
        var res = new IgniteTuple { ["id"] = "root" };
        var current = res;

        for (var i = 1; i <= depth; i++)
        {
            var nested = new IgniteTuple { ["id"] = i };
            current[$"child{i}"] = nested;
            current = nested;
        }

        return res;
    }
}
