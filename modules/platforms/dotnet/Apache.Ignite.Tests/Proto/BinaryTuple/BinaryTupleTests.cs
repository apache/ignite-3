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

namespace Apache.Ignite.Tests.Proto.BinaryTuple
{
    using System;
    using System.Collections;
    using System.Linq;
    using System.Numerics;
    using Internal.Proto;
    using Internal.Proto.BinaryTuple;
    using NodaTime;
    using NUnit.Framework;

    /// <summary>
    /// Tests for binary tuple.
    /// </summary>
    public class BinaryTupleTests
    {
        private delegate void BinaryTupleBuilderAction(ref BinaryTupleBuilder builder);

        [Test]
        public void TestNullValue()
        {
            // Header: 1 byte with null map flag.
            // NullMap: 1 byte with first bit set.
            // Offset table: 1 zero byte
            byte[] bytes = { BinaryTupleCommon.NullmapFlag, 1, 0 };
            var reader = new BinaryTupleReader(bytes, 1);

            Assert.IsTrue(reader.HasNullMap);
            Assert.IsTrue(reader.IsNull(0));
        }

        [Test]
        public void TestGetValueThrowsOnNull()
        {
            var getters = new Action[]
            {
                () => BuildAndRead((ref BinaryTupleBuilder b) => b.AppendNull()).GetString(0),
                () => BuildAndRead((ref BinaryTupleBuilder b) => b.AppendNull()).GetByte(0),
                () => BuildAndRead((ref BinaryTupleBuilder b) => b.AppendNull()).GetShort(0),
                () => BuildAndRead((ref BinaryTupleBuilder b) => b.AppendNull()).GetInt(0),
                () => BuildAndRead((ref BinaryTupleBuilder b) => b.AppendNull()).GetLong(0),
                () => BuildAndRead((ref BinaryTupleBuilder b) => b.AppendNull()).GetFloat(0),
                () => BuildAndRead((ref BinaryTupleBuilder b) => b.AppendNull()).GetDouble(0),
                () => BuildAndRead((ref BinaryTupleBuilder b) => b.AppendNull()).GetGuid(0)
            };

            foreach (var getter in getters)
            {
                var ex = Assert.Throws<InvalidOperationException>(() => getter());
                Assert.AreEqual("Binary tuple element with index 0 is null.", ex!.Message);
            }
        }

        [Test]
        public void TestAppendNull()
        {
            var reader = BuildAndRead((ref BinaryTupleBuilder b) => b.AppendNull());

            Assert.IsTrue(reader.HasNullMap);
            Assert.IsTrue(reader.IsNull(0));
        }

        [Test]
        public void TestDefaultValue()
        {
            // Header: 1 zero byte.
            // Offset table: 1 zero byte.
            byte[] bytes1 = { 0, 0 };

            // Header: 1 byte with null map flag.
            // NullMap: 1 byte with no bit set.
            // Offset table: 1 zero byte
            byte[] bytes2 = { BinaryTupleCommon.NullmapFlag, 0, 0 };

            byte[][] bytesArray = { bytes1, bytes2 };

            foreach (var bytes in bytesArray)
            {
                var reader = new BinaryTupleReader(bytes, 1);

                if (bytes.Length == bytes1.Length)
                {
                    Assert.IsFalse(reader.HasNullMap);
                }
                else
                {
                    Assert.IsTrue(reader.HasNullMap);
                }

                Assert.IsFalse(reader.IsNull(0));
                Assert.AreEqual(string.Empty, reader.GetString(0));
                Assert.AreEqual(Guid.Empty, reader.GetGuid(0));
                Assert.AreEqual(0, reader.GetByte(0));
                Assert.AreEqual(0, reader.GetShort(0));
                Assert.AreEqual(0, reader.GetInt(0));
                Assert.AreEqual(0L, reader.GetLong(0));
            }
        }

        [Test]
        public void TestByte([Values(0, 1, sbyte.MaxValue, sbyte.MinValue)] sbyte value)
        {
            var res = Build((ref BinaryTupleBuilder b) => b.AppendByte(value));

            Assert.AreEqual(value != 0 ? 1 : 0, res[1]);
            Assert.AreEqual(value != 0 ? 3 : 2, res.Length);

            var reader = new BinaryTupleReader(res, 1);
            Assert.AreEqual(value, reader.GetByte(0));
        }

        [Test]
        public void TestShort()
        {
            short[] values = {sbyte.MinValue, -1, 0, 1, sbyte.MaxValue};

            foreach (var value in values)
            {
                var bytes = Build((ref BinaryTupleBuilder b) => b.AppendShort(value));

                Assert.AreEqual(value != 0 ? 1 : 0, bytes[1]);
                Assert.AreEqual(value != 0 ? 3 : 2, bytes.Length);

                var reader = new BinaryTupleReader(bytes, 1);
                Assert.AreEqual(value, reader.GetShort(0));
            }

            values = new short[] { short.MinValue, sbyte.MinValue - 1, sbyte.MaxValue + 1, short.MaxValue };

            foreach (var value in values)
            {
                var bytes = Build((ref BinaryTupleBuilder b) => b.AppendShort(value));

                Assert.AreEqual(2, bytes[1]);
                Assert.AreEqual(4, bytes.Length);

                var reader = new BinaryTupleReader(bytes, 1);
                Assert.AreEqual(value, reader.GetShort(0));
            }
        }

        [Test]
        public void TestInt()
        {
            int[] values = { sbyte.MinValue, -1, 0, 1, sbyte.MaxValue };
            foreach (var value in values)
            {
                var bytes = Build((ref BinaryTupleBuilder b) => b.AppendInt(value));

                Assert.AreEqual(value != 0 ? 1 : 0, bytes[1]);
                Assert.AreEqual(value != 0 ? 3 : 2, bytes.Length);

                var reader = new BinaryTupleReader(bytes, 1);
                Assert.AreEqual(value, reader.GetInt(0));
            }

            values = new[] { short.MinValue, sbyte.MinValue - 1, sbyte.MaxValue + 1, short.MaxValue };
            foreach (var value in values)
            {
                var bytes = Build((ref BinaryTupleBuilder b) => b.AppendInt(value));

                Assert.AreEqual(2, bytes[1]);
                Assert.AreEqual(4, bytes.Length);

                var reader = new BinaryTupleReader(bytes, 1);
                Assert.AreEqual(value, reader.GetInt(0));
            }

            values = new[] { int.MinValue, short.MinValue - 1, short.MaxValue + 1, int.MaxValue };
            foreach (var value in values)
            {
                var bytes = Build((ref BinaryTupleBuilder b) => b.AppendInt(value));

                Assert.AreEqual(4, bytes[1]);
                Assert.AreEqual(6, bytes.Length);

                BinaryTupleReader reader = new BinaryTupleReader(bytes, 1);
                Assert.AreEqual(value, reader.GetInt(0));
            }
        }

        [Test]
        public void TestLong()
        {
            long[] values = { sbyte.MinValue, -1, 0, 1, sbyte.MaxValue };
            foreach (var value in values)
            {
                var bytes = Build((ref BinaryTupleBuilder b) => b.AppendLong(value));

                Assert.AreEqual(value != 0 ? 1 : 0, bytes[1]);
                Assert.AreEqual(value != 0 ? 3 : 2, bytes.Length);

                BinaryTupleReader reader = new BinaryTupleReader(bytes, 1);
                Assert.AreEqual(value, reader.GetLong(0));
            }

            values = new long[] { short.MinValue, sbyte.MinValue - 1, sbyte.MaxValue + 1, short.MaxValue };
            foreach (var value in values)
            {
                var bytes = Build((ref BinaryTupleBuilder b) => b.AppendLong(value));

                Assert.AreEqual(2, bytes[1]);
                Assert.AreEqual(4, bytes.Length);

                BinaryTupleReader reader = new BinaryTupleReader(bytes, 1);
                Assert.AreEqual(value, reader.GetLong(0));
            }

            values = new long[] { int.MinValue, short.MinValue - 1, short.MaxValue + 1, int.MaxValue };
            foreach (var value in values)
            {
                var bytes = Build((ref BinaryTupleBuilder b) => b.AppendLong(value));

                Assert.AreEqual(4, bytes[1]);
                Assert.AreEqual(6, bytes.Length);

                BinaryTupleReader reader = new BinaryTupleReader(bytes, 1);
                Assert.AreEqual(value, reader.GetLong(0));
            }

            values = new[] { long.MinValue, int.MinValue - 1L, int.MaxValue + 1L, long.MaxValue };
            foreach (var value in values)
            {
                var bytes = Build((ref BinaryTupleBuilder b) => b.AppendLong(value));

                Assert.AreEqual(8, bytes[1]);
                Assert.AreEqual(10, bytes.Length);

                BinaryTupleReader reader = new BinaryTupleReader(bytes, 1);
                Assert.AreEqual(value, reader.GetLong(0));
            }
        }

        [Test]
        public void TestFloat()
        {
            {
                float value = 0.0F;
                var bytes = Build((ref BinaryTupleBuilder b) => b.AppendFloat(value));

                Assert.AreEqual(0, bytes[1]);
                Assert.AreEqual(2, bytes.Length);

                var reader = new BinaryTupleReader(bytes, 1);
                Assert.AreEqual(value, reader.GetFloat(0));
            }

            {
                float value = 0.5F;
                var bytes = Build((ref BinaryTupleBuilder b) => b.AppendFloat(value));

                Assert.AreEqual(4, bytes[1]);
                Assert.AreEqual(6, bytes.Length);

                var reader = new BinaryTupleReader(bytes, 1);
                Assert.AreEqual(value, reader.GetFloat(0));
            }
        }

        [Test]
        public void TestDouble()
        {
            {
                double value = 0.0;
                var bytes = Build((ref BinaryTupleBuilder b) => b.AppendDouble(value));

                Assert.AreEqual(0, bytes[1]);
                Assert.AreEqual(2, bytes.Length);

                var reader = new BinaryTupleReader(bytes, 1);
                Assert.AreEqual(value, reader.GetDouble(0));
            }

            {
                double value = 0.5;
                var bytes = Build((ref BinaryTupleBuilder b) => b.AppendDouble(value));

                Assert.AreEqual(4, bytes[1]);
                Assert.AreEqual(6, bytes.Length);

                var reader = new BinaryTupleReader(bytes, 1);
                Assert.AreEqual(value, reader.GetDouble(0));
            }

            {
                double value = 0.1;
                var bytes = Build((ref BinaryTupleBuilder b) => b.AppendDouble(value));

                Assert.AreEqual(8, bytes[1]);
                Assert.AreEqual(10, bytes.Length);

                var reader = new BinaryTupleReader(bytes, 1);
                Assert.AreEqual(value, reader.GetDouble(0));
            }
        }

        [Test]
        public void TestString()
        {
            var values = new[] {"ascii", "我愛Java", string.Empty, "a string with a bit more characters"};

            var reader = BuildAndRead(
                (ref BinaryTupleBuilder b) =>
                {
                    foreach (var value in values)
                    {
                        b.AppendString(value);
                    }
                },
                numElements: values.Length);

            for (var i = 0; i < values.Length; i++)
            {
               Assert.AreEqual(values[i], reader.GetString(i));
            }
        }

        [Test]
        public void TestStringNullable()
        {
            var values = new[] {"ascii", string.Empty, null};

            var reader = BuildAndRead(
                (ref BinaryTupleBuilder b) =>
                {
                    foreach (var value in values)
                    {
                        b.AppendStringNullable(value);
                    }
                },
                numElements: values.Length);

            for (var i = 0; i < values.Length; i++)
            {
               Assert.AreEqual(values[i], reader.GetStringNullable(i));
            }
        }

        [Test]
        public void TestGuid()
        {
            var guid = Guid.NewGuid();

            var reader = BuildAndRead(
                (ref BinaryTupleBuilder b) =>
                {
                    b.AppendGuid(Guid.Empty);
                    b.AppendGuid(guid);
                },
                numElements: 2);

            Assert.AreEqual(Guid.Empty, reader.GetGuid(0));
            Assert.AreEqual(guid, reader.GetGuid(1));
        }

        [Test]
        public void TestBytes([Values(0, 1, 123)] int count)
        {
            var bytes = Enumerable.Range(1, count).Select(x => (byte)x).ToArray();
            var reader = BuildAndRead((ref BinaryTupleBuilder b) => b.AppendBytes(bytes));
            var res = reader.GetBytes(0);

            CollectionAssert.AreEqual(bytes, res);
        }

        [Test]
        public void TestBitMask([Values(0, 1, 123)] int count)
        {
            var bitMask = new BitArray(count);

            for (var i = 0; i < count; i++)
            {
                bitMask.Set(i, i % 2 == 0);
            }

            var reader = BuildAndRead((ref BinaryTupleBuilder b) => b.AppendBitmask(bitMask));
            var res = reader.GetBitmask(0);

            Assert.GreaterOrEqual(res.Length, bitMask.Length); // Resulting bitmask may be padded with false bits to the byte boundary.

            for (var i = 0; i < count; i++)
            {
                Assert.AreEqual(i % 2 == 0, res.Get(i));
            }
        }

        [Test]
        public void TestBigInteger([Values(0, 15, 123)] long val, [Values(1, 33, 456, 9876)] int exp)
        {
            var bigInt = BigInteger.Pow(val, exp);

            var reader = BuildAndRead((ref BinaryTupleBuilder b) => b.AppendNumber(bigInt));
            var res = reader.GetNumber(0);

            Assert.AreEqual(bigInt, res);
        }

        [Test]
        public void TestDecimal()
        {
            Test(0, 3);
            Test(0, 0);

            Test(decimal.MaxValue, 0);
            Test(decimal.MinValue, 0);

            Test(12345.6789m, 4);
            Test(12345.678m, 4);
            Test(12345.67m, 4);

            Test(-12345.6789m, 4);
            Test(-12345.678m, 4);
            Test(-12345.67m, 4);

            Test(12345.6789m, 2, 12345.67m);
            Test(12345.6789m, 0, 12345m);

            Test(-12345.6789m, 2, -12345.67m);
            Test(-12345.6789m, 0, -12345m);

            static void Test(decimal val, int scale, decimal? expected = null)
            {
                var reader = BuildAndRead((ref BinaryTupleBuilder b) => b.AppendDecimal(val, scale));
                var res = reader.GetDecimal(0, scale);

                Assert.AreEqual(expected ?? val, res);
            }
        }

        [Test]
        public void TestDecimalScaleOverflow()
        {
            const int scale = 100;

            var ex = Assert.Throws<OverflowException>(
                () => BuildAndRead((ref BinaryTupleBuilder b) => b.AppendDecimal(12.34m, scale)).GetDecimal(0, scale));

            Assert.AreEqual("Value was either too large or too small for a Decimal.", ex!.Message);
        }

        [Test]
        public void TestDecimalMagnitudeOverflow()
        {
            var magnitude = Enumerable.Range(1, 100).Select(_ => (byte)250).ToArray();

            var ex = Assert.Throws<OverflowException>(
                () => BuildAndRead((ref BinaryTupleBuilder b) => b.AppendBytes(magnitude)).GetDecimal(0, 0));

            Assert.AreEqual("Value was either too large or too small for a Decimal.", ex!.Message);
        }

        [Test]
        public void TestDate()
        {
            var val = LocalDate.FromDateTime(DateTime.UtcNow);

            var reader = BuildAndRead(
                (ref BinaryTupleBuilder b) =>
                {
                    b.AppendDate(default);
                    b.AppendDate(val);
                    b.AppendDate(LocalDate.MaxIsoValue);
                    b.AppendDate(LocalDate.MinIsoValue);
                },
                4);

            Assert.AreEqual(default(LocalDate), reader.GetDate(0));
            Assert.AreEqual(val, reader.GetDate(1));
            Assert.AreEqual(LocalDate.MaxIsoValue, reader.GetDate(2));
            Assert.AreEqual(LocalDate.MinIsoValue, reader.GetDate(3));
        }

        [Test]
        public void TestTime()
        {
            var val = LocalDateTime.FromDateTime(DateTime.UtcNow).TimeOfDay;

            var reader = BuildAndRead(
                (ref BinaryTupleBuilder b) =>
                {
                    b.AppendTime(default);
                    b.AppendTime(val);
                    b.AppendTime(LocalTime.MinValue);
                    b.AppendTime(LocalTime.MaxValue);
                    b.AppendTime(LocalTime.Midnight);
                    b.AppendTime(LocalTime.Noon);
                },
                6);

            Assert.AreEqual(default(LocalTime), reader.GetTime(0));
            Assert.AreEqual(val, reader.GetTime(1));
            Assert.AreEqual(LocalTime.MinValue, reader.GetTime(2));
            Assert.AreEqual(LocalTime.MaxValue, reader.GetTime(3));
            Assert.AreEqual(LocalTime.Midnight, reader.GetTime(4));
            Assert.AreEqual(LocalTime.Noon, reader.GetTime(5));
        }

        [Test]
        public void TestDateTime()
        {
            var val = LocalDateTime.FromDateTime(DateTime.UtcNow);

            var reader = BuildAndRead(
                (ref BinaryTupleBuilder b) =>
                {
                    b.AppendDateTime(default);
                    b.AppendDateTime(val);
                    b.AppendDateTime(LocalDateTime.MaxIsoValue);
                    b.AppendDateTime(LocalDateTime.MinIsoValue);
                },
                4);

            Assert.AreEqual(default(LocalDateTime), reader.GetDateTime(0));
            Assert.AreEqual(val, reader.GetDateTime(1));
            Assert.AreEqual(LocalDateTime.MaxIsoValue, reader.GetDateTime(2));
            Assert.AreEqual(LocalDateTime.MinIsoValue, reader.GetDateTime(3));
        }

        [Test]
        public void TestTimestamp()
        {
            var val = Instant.FromDateTimeUtc(DateTime.UtcNow);

            var reader = BuildAndRead(
                (ref BinaryTupleBuilder b) =>
                {
                    b.AppendTimestamp(default);
                    b.AppendTimestamp(val);
                    b.AppendTimestamp(Instant.MaxValue);
                    b.AppendTimestamp(Instant.MinValue);
                    b.AppendTimestamp(NodaConstants.BclEpoch);
                    b.AppendTimestamp(NodaConstants.JulianEpoch);
                },
                6);

            Assert.AreEqual(NodaConstants.UnixEpoch, reader.GetTimestamp(0));
            Assert.AreEqual(val, reader.GetTimestamp(1));
            Assert.AreEqual(Instant.MaxValue, reader.GetTimestamp(2));
            Assert.AreEqual(Instant.MinValue, reader.GetTimestamp(3));
            Assert.AreEqual(NodaConstants.BclEpoch, reader.GetTimestamp(4));
            Assert.AreEqual(NodaConstants.JulianEpoch, reader.GetTimestamp(5));
        }

        [Test]
        public void TestDuration()
        {
            var val = Duration.FromSeconds(Instant.FromDateTimeUtc(DateTime.UtcNow).ToUnixTimeSeconds()) +
                      Duration.FromNanoseconds(long.MaxValue);

            var reader = BuildAndRead(
                (ref BinaryTupleBuilder b) =>
                {
                    b.AppendDuration(default);
                    b.AppendDuration(val);
                    b.AppendDuration(Duration.MaxValue);
                    b.AppendDuration(Duration.MinValue);
                },
                4);

            Assert.AreEqual(Duration.Zero, reader.GetDuration(0));
            Assert.AreEqual(val, reader.GetDuration(1));
            Assert.AreEqual(Duration.MaxValue, reader.GetDuration(2));
            Assert.AreEqual(Duration.MinValue, reader.GetDuration(3));
        }

        [Test]
        public void TestPeriod()
        {
            var val1 = Period.FromYears(1) + Period.FromMonths(2) + Period.FromDays(3);
            var val2 = Period.FromYears(-100) + Period.FromMonths(-200) + Period.FromDays(-300);
            var val3 = Period.FromYears(int.MinValue) + Period.FromMonths(int.MaxValue) + Period.FromDays(short.MinValue);

            var reader = BuildAndRead(
                (ref BinaryTupleBuilder b) =>
                {
                    b.AppendPeriod(Period.Zero);
                    b.AppendPeriod(val1);
                    b.AppendPeriod(val2);
                    b.AppendPeriod(val3);
                },
                4);

            Assert.AreEqual(Period.Zero, reader.GetPeriod(0));
            Assert.AreEqual(val1, reader.GetPeriod(1));
            Assert.AreEqual(val2, reader.GetPeriod(2));
            Assert.AreEqual(val3, reader.GetPeriod(3));
        }

        [Test]
        public void TestPeriodWithWeeksOrTimeComponentIsNotSupported()
        {
            AssertNotSupported(Period.FromWeeks(1));
            AssertNotSupported(Period.FromHours(1));
            AssertNotSupported(Period.FromMinutes(1));
            AssertNotSupported(Period.FromSeconds(1));
            AssertNotSupported(Period.FromMilliseconds(1));
            AssertNotSupported(Period.FromTicks(1));
            AssertNotSupported(Period.FromNanoseconds(1));

            static void AssertNotSupported(Period p) =>
                Assert.Throws<NotSupportedException>(() => BuildAndRead((ref BinaryTupleBuilder b) => b.AppendPeriod(p)));
        }

        [Test]
        public void TestDefault()
        {
            var reader = BuildAndRead((ref BinaryTupleBuilder b) => b.AppendDefault());

            Assert.AreEqual(0, reader.GetInt(0));
            Assert.AreEqual(0, reader.GetByte(0));
            Assert.AreEqual(0, reader.GetShort(0));
            Assert.AreEqual(0, reader.GetLong(0));
            Assert.AreEqual(0, reader.GetDouble(0));
            Assert.AreEqual(0, reader.GetFloat(0));
            Assert.AreEqual(0, reader.GetDecimal(0, 123));
            Assert.AreEqual(BigInteger.Zero, reader.GetNumber(0));
            Assert.AreEqual(string.Empty, reader.GetString(0));
            Assert.AreEqual(string.Empty, reader.GetStringNullable(0));
            Assert.AreEqual(new BitArray(0), reader.GetBitmask(0));
            Assert.AreEqual(Guid.Empty, reader.GetGuid(0));
            Assert.AreEqual(Array.Empty<byte>(), reader.GetBytes(0));
            Assert.AreEqual(Duration.Zero, reader.GetDuration(0));
            Assert.AreEqual(Period.Zero, reader.GetPeriod(0));
            Assert.AreEqual(LocalTime.Midnight, reader.GetTime(0));
            Assert.AreEqual(default(LocalDate), reader.GetDate(0));
            Assert.AreEqual(default(LocalDateTime), reader.GetDateTime(0));
        }

        [Test]
        public void TestObject()
        {
            var guid = Guid.NewGuid();
            var utcNow = DateTime.UtcNow;
            var date = LocalDate.FromDateTime(utcNow);
            var dateTime = LocalDateTime.FromDateTime(utcNow);
            var bitArray = new BitArray(new[] { byte.MaxValue });
            var bytes = new byte[] { 1, 2 };

            var reader = BuildAndRead(
                (ref BinaryTupleBuilder b) =>
                {
                    b.AppendObject(null, ClientDataType.String);
                    b.AppendObject(sbyte.MaxValue, ClientDataType.Int8);
                    b.AppendObject(short.MaxValue, ClientDataType.Int16);
                    b.AppendObject(int.MaxValue, ClientDataType.Int32);
                    b.AppendObject(long.MaxValue, ClientDataType.Int64);
                    b.AppendObject(float.MaxValue, ClientDataType.Float);
                    b.AppendObject(double.MaxValue, ClientDataType.Double);
                    b.AppendObject(decimal.One, ClientDataType.Decimal);
                    b.AppendObject(BigInteger.One, ClientDataType.Number);
                    b.AppendObject("foo", ClientDataType.String);
                    b.AppendObject(bitArray, ClientDataType.BitMask);
                    b.AppendObject(guid, ClientDataType.Uuid);
                    b.AppendObject(bytes, ClientDataType.Bytes);
                    b.AppendObject(LocalTime.FromMinutesSinceMidnight(123), ClientDataType.Time);
                    b.AppendObject(date, ClientDataType.Date);
                    b.AppendObject(dateTime, ClientDataType.DateTime);
                    b.AppendObject(Instant.FromDateTimeUtc(utcNow), ClientDataType.Timestamp);
                },
                17);

            Assert.IsNull(reader.GetObject(0, ClientDataType.String));
            Assert.AreEqual(sbyte.MaxValue, reader.GetObject(1, ClientDataType.Int8));
            Assert.AreEqual(short.MaxValue, reader.GetObject(2, ClientDataType.Int16));
            Assert.AreEqual(int.MaxValue, reader.GetObject(3, ClientDataType.Int32));
            Assert.AreEqual(long.MaxValue, reader.GetObject(4, ClientDataType.Int64));
            Assert.AreEqual(float.MaxValue, reader.GetObject(5, ClientDataType.Float));
            Assert.AreEqual(double.MaxValue, reader.GetObject(6, ClientDataType.Double));
            Assert.AreEqual(decimal.One, reader.GetObject(7, ClientDataType.Decimal));
            Assert.AreEqual(BigInteger.One, reader.GetObject(8, ClientDataType.Number));
            Assert.AreEqual("foo", reader.GetObject(9, ClientDataType.String));
            Assert.AreEqual(bitArray, reader.GetObject(10, ClientDataType.BitMask));
            Assert.AreEqual(guid, reader.GetObject(11, ClientDataType.Uuid));
            Assert.AreEqual(bytes, reader.GetObject(12, ClientDataType.Bytes));
            Assert.AreEqual(LocalTime.FromMinutesSinceMidnight(123), reader.GetObject(13, ClientDataType.Time));
            Assert.AreEqual(date, reader.GetObject(14, ClientDataType.Date));
            Assert.AreEqual(dateTime, reader.GetObject(15, ClientDataType.DateTime));
            Assert.AreEqual(Instant.FromDateTimeUtc(utcNow), reader.GetObject(16, ClientDataType.Timestamp));
        }

        [Test]
        public void TestObjectWithType()
        {
            var guid = Guid.NewGuid();
            var utcNow = DateTime.UtcNow;
            var date = LocalDate.FromDateTime(utcNow);
            var dateTime = LocalDateTime.FromDateTime(utcNow);
            var bitArray = new BitArray(new[] { byte.MaxValue });
            var bytes = new byte[] { 1, 2 };

            var reader = BuildAndRead(
                (ref BinaryTupleBuilder b) =>
                {
                    b.AppendObjectWithType(null);
                    b.AppendObjectWithType(sbyte.MaxValue);
                    b.AppendObjectWithType(short.MaxValue);
                    b.AppendObjectWithType(int.MaxValue);
                    b.AppendObjectWithType(long.MaxValue);
                    b.AppendObjectWithType(float.MaxValue);
                    b.AppendObjectWithType(double.MaxValue);
                    b.AppendObjectWithType(decimal.One);
                    b.AppendObjectWithType(BigInteger.One);
                    b.AppendObjectWithType("foo");
                    b.AppendObjectWithType(bitArray);
                    b.AppendObjectWithType(guid);
                    b.AppendObjectWithType(bytes);
                    b.AppendObjectWithType(LocalTime.FromMinutesSinceMidnight(123));
                    b.AppendObjectWithType(date);
                    b.AppendObjectWithType(dateTime);
                    b.AppendObjectWithType(Instant.FromDateTimeUtc(utcNow));
                },
                17 * 3);

            Assert.IsNull(reader.GetObject(0));
            Assert.AreEqual(sbyte.MaxValue, reader.GetObject(3));
            Assert.AreEqual(short.MaxValue, reader.GetObject(6));
            Assert.AreEqual(int.MaxValue, reader.GetObject(9));
            Assert.AreEqual(long.MaxValue, reader.GetObject(12));
            Assert.AreEqual(float.MaxValue, reader.GetObject(15));
            Assert.AreEqual(double.MaxValue, reader.GetObject(18));
            Assert.AreEqual(decimal.One, reader.GetObject(21));
            Assert.AreEqual(BigInteger.One, reader.GetObject(24));
            Assert.AreEqual("foo", reader.GetObject(27));
            Assert.AreEqual(bitArray, reader.GetObject(30));
            Assert.AreEqual(guid, reader.GetObject(33));
            Assert.AreEqual(bytes, reader.GetObject(36));
            Assert.AreEqual(LocalTime.FromMinutesSinceMidnight(123), reader.GetObject(39));
            Assert.AreEqual(date, reader.GetObject(42));
            Assert.AreEqual(dateTime, reader.GetObject(45));
            Assert.AreEqual(Instant.FromDateTimeUtc(utcNow), reader.GetObject(48));
        }

        private static BinaryTupleReader BuildAndRead(BinaryTupleBuilderAction build, int numElements = 1)
        {
            var bytes = Build(build, numElements);

            return new BinaryTupleReader(bytes, numElements);
        }

        private static byte[] Build(BinaryTupleBuilderAction build, int numElements = 1)
        {
            var builder = new BinaryTupleBuilder(numElements);

            try
            {
                build.Invoke(ref builder);

                return builder.Build().ToArray();
            }
            finally
            {
                builder.Dispose();
            }
        }
    }
}
