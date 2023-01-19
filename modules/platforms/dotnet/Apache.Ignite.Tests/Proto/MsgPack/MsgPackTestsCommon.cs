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

namespace Apache.Ignite.Tests.Proto.MsgPack;

using System.Collections.Generic;

/// <summary>
/// Common MsgPack test logic.
/// </summary>
public static class MsgPackTestsCommon
{
    public static readonly string?[] TestStrings =
    {
        "foo",
        string.Empty,
        null,
        "тест",
        "ascii0123456789",
        "的的abcdкириллица",
        new(new[] { (char)0xD801, (char)0xDC37 }),
    };

    public static IEnumerable<long> GetNumbers(long max = long.MaxValue, bool unsignedOnly = false)
    {
        yield return 0;

        for (int i = 1; i < 63; i++)
        {
            var num = 1L << i;

            if (num > max)
            {
                yield break;
            }

            yield return num;

            if (num > 0)
            {
                yield return num - 1;
            }

            if (!unsignedOnly)
            {
                yield return -num;
                yield return 1 - num;
            }
        }
    }
}
