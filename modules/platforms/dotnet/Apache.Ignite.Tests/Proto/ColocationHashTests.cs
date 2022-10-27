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

namespace Apache.Ignite.Tests.Proto;

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using Internal.Proto.BinaryTuple;
using NodaTime;
using NUnit.Framework;

/// <summary>
/// Tests that colocation hash calculation is consistent with server logic.
/// </summary>
public class ColocationHashTests : IgniteTestsBase
{
    private const string ColocationHashJob = "org.apache.ignite.internal.runner.app.PlatformTestNodeRunner$ColocationHashJob";

    private static readonly object[] TestCases =
    {
        sbyte.MinValue,
        (sbyte)1,
        (sbyte)-1,
        sbyte.MaxValue,
        short.MinValue,
        (short)1,
        (short)-1,
        short.MaxValue,
        int.MinValue,
        1,
        0,
        -1,
        int.MaxValue,
        long.MinValue,
        1L,
        -1L,
        long.MaxValue,
        float.MinValue,
        -1.1f,
        1.1f,
        float.Epsilon,
        float.MaxValue,
        double.MinValue,
        -1.1d,
        1.1d,
        double.Epsilon,
        double.MaxValue,
        decimal.MinValue,
        -1.1m,
        1.1m,
        123.45678m,
        decimal.MaxValue,
        string.Empty,
        "abc αβγ 🔥",
        Guid.Empty,
        Guid.NewGuid(),
        BigInteger.One,
        BigInteger.Zero,
        BigInteger.MinusOne,
        (BigInteger)int.MaxValue,
        (BigInteger)int.MinValue,
        (BigInteger)ulong.MaxValue,
        BigInteger.Pow(123, 100),
        new BitArray(1, false),
        new BitArray(new byte[] {0, 5, 0}),
        new BitArray(17, true),
        new LocalDate(9876, 7, 30),
        new LocalDate(2, 1, 1),
        new LocalDate(1, 1, 1),
        default(LocalDate),
        new LocalTime(9, 8, 7),
        LocalTime.Midnight,
        LocalTime.Noon,
        LocalDateTime.FromDateTime(DateTime.UtcNow).TimeOfDay,
        default(LocalTime),
        new LocalDateTime(year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, millisecond: 1),
        new LocalDateTime(year: 2022, month: 10, day: 22, hour: 10, minute: 30, second: 55, millisecond: 123),
        LocalDateTime.FromDateTime(DateTime.UtcNow),
        default(LocalDateTime),
        Instant.FromUnixTimeSeconds(0),
        default(Instant)
    };

    [Test]
    [TestCaseSource(nameof(TestCases))]
    public async Task TestSingleKeyColocationHashIsSameOnServerAndClient(object key) =>
        await AssertClientAndServerHashesAreEqual(key);

    [Test]
    public async Task TestMultiKeyColocationHashIsSameOnServerAndClient()
    {
        for (var i = 0; i < TestCases.Length; i++)
        {
            await AssertClientAndServerHashesAreEqual(TestCases.Take(i + 1).ToArray());
            await AssertClientAndServerHashesAreEqual(TestCases.Skip(i).ToArray());
        }
    }

    private static (byte[] Bytes, int Hash) WriteAsBinaryTuple(IReadOnlyCollection<object> arr)
    {
        using var builder = new BinaryTupleBuilder(arr.Count * 3, hashedColumnsPredicate: new TestIndexProvider());

        foreach (var obj in arr)
        {
            builder.AppendObjectWithType(obj);
        }

        return (builder.Build().ToArray(), builder.Hash);
    }

    private async Task AssertClientAndServerHashesAreEqual(params object[] keys)
    {
        var (bytes, hash) = WriteAsBinaryTuple(keys);

        var serverHash = await GetServerHash(bytes, keys.Length);

        Assert.AreEqual(serverHash, hash, string.Join(", ", keys));
    }

    private async Task<int> GetServerHash(byte[] bytes, int count)
    {
        var nodes = await Client.GetClusterNodesAsync();

        return await Client.Compute.ExecuteAsync<int>(nodes, ColocationHashJob, count, bytes);
    }

    private class TestIndexProvider : IHashedColumnIndexProvider
    {
        public bool IsHashedColumnIndex(int index) => index % 3 == 2;
    }
}
