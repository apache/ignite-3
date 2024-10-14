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

namespace Apache.Ignite.Tests;

using System;
using System.Globalization;
using System.Numerics;
using NUnit.Framework;

/// <summary>
/// Add tests for <see cref="BigDecimal"/>.
/// </summary>
public class BigDecimalTests
{
    [Test]
    public void TestToDecimal()
    {
        var bigDecimal = new BigDecimal(BigInteger.Parse("1234567890"), 5);

        Assert.AreEqual(12345.6789m, bigDecimal.ToDecimal());
    }

    [Test]
    public void TestFromDecimalToDecimal()
    {
        for (int i = 0; i < 100; i++)
        {
            var unscaled = Random.Shared.NextInt64(long.MinValue, long.MaxValue);
            var scale = Random.Shared.Next(0, 25);

            var decimalVal = new decimal(unscaled) / (decimal)Math.Pow(10, scale);
            var bigDecimal = new BigDecimal(decimalVal);
            var result = bigDecimal.ToDecimal();

            Assert.AreEqual(decimalVal, result);
        }
    }

    [Test]
    [TestCase("0", 0, null, "0")]
    [TestCase("0", 1, null, "0")]
    [TestCase("1", 0, null, "1")]
    [TestCase("1", 1, null, ".1")]
    [TestCase("1", 5, null, ".00001")]
    [TestCase("123456789", 5, null, "1234.56789")]
    [TestCase("123456789", 5, "", "1234.56789")]
    [TestCase("123456789", 5, "en-US", "1234.56789")]
    [TestCase("123456789", 5, "de-DE", "1234,56789")]
    public void TestToString(string unscaled, short scale, string? cultureName, string expected)
    {
        var bigDecimal = new BigDecimal(BigInteger.Parse(unscaled), scale);

        var str = cultureName == null
            ? bigDecimal.ToString()
            : bigDecimal.ToString(CultureInfo.GetCultureInfo(cultureName));

        Assert.AreEqual(expected, str);
    }
}
