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
    [TestCase(0, 0, 0)]
    [TestCase(1, 0, 1)]
    [TestCase(1, 1, 0.1)]
    [TestCase(1, -1, 10)]
    [TestCase(1, 5, 0.00001)]
    [TestCase(12345678912345, 6, 12345678.912345)]
    [TestCase(987, -6, 987000000)]
    public void TestToDecimal(long unscaled, short scale, decimal expected)
    {
        var bigDecimal = new BigDecimal(new BigInteger(unscaled), scale);

        Assert.AreEqual(expected, bigDecimal.ToDecimal());
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

            Assert.AreEqual(decimalVal, result, $"Unscaled={unscaled}, Scale={scale}");
        }
    }

    [Test]
    public void TestToDecimalOutOfRange()
    {
        var bigDecimal = new BigDecimal(BigInteger.Parse("123456789123456789123456789123456789123456789"), 3);

        var ex = Assert.Throws<OverflowException>(() => bigDecimal.ToDecimal());
        Assert.AreEqual("Value was either too large or too small for a Decimal.", ex.Message);
    }

    [Test]
    [TestCase("0", 0, null, "0")]
    [TestCase("0", 1, null, "0")]
    [TestCase("0", -1, null, "0")]
    [TestCase("1", 0, null, "1")]
    [TestCase("1", 1, null, ".1")]
    [TestCase("1", -1, null, "10")]
    [TestCase("1", 5, null, ".00001")]
    [TestCase("123", -2, null, "12300")]
    [TestCase("123", 0, null, "123")]
    [TestCase("123", 1, null, "12.3")]
    [TestCase("123", 2, null, "1.23")]
    [TestCase("123", 3, null, ".123")]
    [TestCase("123", 4, null, ".0123")]
    [TestCase("123", 5, null, ".00123")]
    [TestCase("123456789", 5, null, "1234.56789")]
    [TestCase("123456789", 5, "", "1234.56789")]
    [TestCase("123456789", 5, "en-US", "1234.56789")]
    [TestCase("123456789", 5, "de-DE", "1234,56789")]
    [TestCase("123456789123456789123456789123456789123456789", 35, "de-DE", "1234567891,23456789123456789123456789123456789")]
    public void TestToString(string unscaled, short scale, string? cultureName, string expected)
    {
        var bigDecimal = new BigDecimal(BigInteger.Parse(unscaled), scale);

        var str = cultureName == null
            ? bigDecimal.ToString()
            : bigDecimal.ToString(CultureInfo.GetCultureInfo(cultureName));

        Assert.AreEqual(expected, str);
    }

    [Test]
    [TestCase(0, 0, 0)]
    [TestCase(1, 1, 0)]
    [TestCase(12.3456, 123456, 4)]
    [TestCase(12.34560, 123456, 4)]
    [TestCase(.1, 1, 1)]
    public void TestUnscaledValueAndScale(decimal val, long expectedUnscaled, short expectedScale)
    {
        var bigDecimal = new BigDecimal(val);

        Assert.AreEqual(expectedUnscaled, (long)bigDecimal.UnscaledValue);
        Assert.AreEqual(expectedScale, bigDecimal.Scale);
    }

    [Test]
    public void TestCompareTo()
    {
        Assert.Fail("TODO");
    }

    [Test]
    public void TestNegativeScale()
    {
        Assert.Fail("TODO");
    }
}
