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
    [TestCase("1", 1, null, "0.1")]
    [TestCase("1", -1, null, "10")]
    [TestCase("1", 5, null, "0.00001")]
    [TestCase("123", -2, null, "12300")]
    [TestCase("123", 0, null, "123")]
    [TestCase("123", 1, null, "12.3")]
    [TestCase("123", 2, null, "1.23")]
    [TestCase("123", 3, null, "0.123")]
    [TestCase("123", 4, null, "0.0123")]
    [TestCase("123", 5, null, "0.00123")]
    [TestCase("123456789", 5, null, "1234.56789")]
    [TestCase("123456789", 5, "", "1234.56789")]
    [TestCase("123456789", 5, "en-US", "1234.56789")]
    [TestCase("123456789", 5, "de-DE", "1234,56789")]
    [TestCase("123456789123456789123456789123456789123456789", 35, "de-DE", "1234567891,23456789123456789123456789123456789")]
    public void TestToString(string unscaled, short scale, string? cultureName, string expected)
    {
        var bigDecimal = new BigDecimal(BigInteger.Parse(unscaled), scale);

        var culture = cultureName == null
            ? CultureInfo.InvariantCulture
            : CultureInfo.GetCultureInfo(cultureName);

        var str = bigDecimal.ToString(culture);

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
    public void TestEquality([Values(0, 1, -1, 0.1, -0.1, 1234567, -456.789)] decimal d)
    {
        var x = new BigDecimal(d);
        var y = new BigDecimal(d);

        Assert.AreEqual(x, y);
        Assert.AreEqual(x.ToDecimal(), y.ToDecimal());

        Assert.AreEqual(x.UnscaledValue, y.UnscaledValue);
        Assert.AreEqual(x.Scale, y.Scale);

        Assert.AreEqual(0, x.CompareTo(y));
        Assert.AreEqual(0, y.CompareTo(x));

        Assert.IsFalse(x < y);
        Assert.IsFalse(x > y);

        Assert.IsTrue(x <= y);
        Assert.IsTrue(x >= y);
    }

    [Test]
    public void TestSameValueDifferentScaleAreNotEqual()
    {
        var x = new BigDecimal(100, 0);
        var y = new BigDecimal(10, -1);

        // Similar to Java BigDecimal.
        Assert.AreNotEqual(x, y);
        Assert.AreEqual(0, x.CompareTo(y));
        Assert.AreEqual(x.ToString(), y.ToString());
        Assert.AreEqual(100, x.ToDecimal());
        Assert.AreEqual(100, y.ToDecimal());
    }

    [Test]
    public void TestCompareTo()
    {
        var x = new BigDecimal(1234, 1000);
        var y = new BigDecimal(1235, 1000);

        Assert.AreEqual(-1, x.CompareTo(y));
        Assert.AreEqual(1, y.CompareTo(x));
        Assert.IsTrue(x < y);
        Assert.IsTrue(x <= y);
        Assert.IsTrue(y > x);
        Assert.IsTrue(y >= x);
    }
}
