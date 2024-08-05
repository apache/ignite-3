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
    using System.Linq;
    using Ignite.Sql;
    using Internal.Proto.BinaryTuple;
    using Internal.Table;
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
            // Header: 1 zero byte.
            // Offset table: 1 zero byte.
            byte[] bytes = { 0, 0 };
            var reader = new BinaryTupleReader(bytes, 1);

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

            Assert.IsTrue(reader.IsNull(0));
        }

        [Test]
        public void TestByte([Values(0, 1, sbyte.MaxValue, sbyte.MinValue)] sbyte value)
        {
            var res = Build((ref BinaryTupleBuilder b) => b.AppendByte(value));

            Assert.AreEqual(1, res[1]);
            Assert.AreEqual(3, res.Length);

            var reader = new BinaryTupleReader(res, 1);
            Assert.AreEqual(value, reader.GetByte(0));
            Assert.AreEqual(value, reader.GetShort(0));
            Assert.AreEqual(value, reader.GetInt(0));
            Assert.AreEqual(value, reader.GetLong(0));
        }

        [Test]
        public void TestBool([Values(true, false)] bool value)
        {
            var res = Build((ref BinaryTupleBuilder b) => b.AppendBool(value));

            Assert.AreEqual(1, res[1]);
            Assert.AreEqual(3, res.Length);

            var reader = new BinaryTupleReader(res, 1);
            Assert.AreEqual(value, reader.GetBool(0));
            Assert.AreEqual(value, reader.GetBoolNullable(0)!.Value);
            Assert.AreEqual(value ? 1 : 0, reader.GetInt(0));
        }

        [Test]
        public void TestShort()
        {
            short[] values = {sbyte.MinValue, -1, 0, 1, sbyte.MaxValue};

            foreach (var value in values)
            {
                var bytes = Build((ref BinaryTupleBuilder b) => b.AppendShort(value));

                Assert.AreEqual(1, bytes[1]);
                Assert.AreEqual(3, bytes.Length);

                var reader = new BinaryTupleReader(bytes, 1);
                Assert.AreEqual(value, reader.GetByte(0));
                Assert.AreEqual(value, reader.GetShort(0));
                Assert.AreEqual(value, reader.GetInt(0));
                Assert.AreEqual(value, reader.GetLong(0));
            }

            values = new short[] { short.MinValue, sbyte.MinValue - 1, sbyte.MaxValue + 1, short.MaxValue };

            foreach (var value in values)
            {
                var bytes = Build((ref BinaryTupleBuilder b) => b.AppendShort(value));

                Assert.AreEqual(2, bytes[1]);
                Assert.AreEqual(4, bytes.Length);

                var reader = new BinaryTupleReader(bytes, 1);
                Assert.AreEqual(value, reader.GetShort(0));
                Assert.AreEqual(value, reader.GetInt(0));
                Assert.AreEqual(value, reader.GetLong(0));
            }
        }

        [Test]
        public void TestInt()
        {
            int[] values = { sbyte.MinValue, -1, 0, 1, sbyte.MaxValue };
            foreach (var value in values)
            {
                var bytes = Build((ref BinaryTupleBuilder b) => b.AppendInt(value));

                Assert.AreEqual(1, bytes[1]);
                Assert.AreEqual(3, bytes.Length);

                var reader = new BinaryTupleReader(bytes, 1);
                Assert.AreEqual(value, reader.GetByte(0));
                Assert.AreEqual(value, reader.GetShort(0));
                Assert.AreEqual(value, reader.GetInt(0));
                Assert.AreEqual(value, reader.GetLong(0));
            }

            values = new[] { short.MinValue, sbyte.MinValue - 1, sbyte.MaxValue + 1, short.MaxValue };
            foreach (var value in values)
            {
                var bytes = Build((ref BinaryTupleBuilder b) => b.AppendInt(value));

                Assert.AreEqual(2, bytes[1]);
                Assert.AreEqual(4, bytes.Length);

                var reader = new BinaryTupleReader(bytes, 1);
                Assert.AreEqual(value, reader.GetShort(0));
                Assert.AreEqual(value, reader.GetInt(0));
                Assert.AreEqual(value, reader.GetLong(0));
            }

            values = new[] { int.MinValue, short.MinValue - 1, short.MaxValue + 1, int.MaxValue };
            foreach (var value in values)
            {
                var bytes = Build((ref BinaryTupleBuilder b) => b.AppendInt(value));

                Assert.AreEqual(4, bytes[1]);
                Assert.AreEqual(6, bytes.Length);

                BinaryTupleReader reader = new BinaryTupleReader(bytes, 1);
                Assert.AreEqual(value, reader.GetInt(0));
                Assert.AreEqual(value, reader.GetLong(0));
            }
        }

        [Test]
        public void TestLong()
        {
            long[] values = { sbyte.MinValue, -1, 0, 1, sbyte.MaxValue };
            foreach (var value in values)
            {
                var bytes = Build((ref BinaryTupleBuilder b) => b.AppendLong(value));

                Assert.AreEqual(1, bytes[1]);
                Assert.AreEqual(3, bytes.Length);

                BinaryTupleReader reader = new BinaryTupleReader(bytes, 1);
                Assert.AreEqual(value, reader.GetByte(0));
                Assert.AreEqual(value, reader.GetShort(0));
                Assert.AreEqual(value, reader.GetInt(0));
                Assert.AreEqual(value, reader.GetLong(0));
            }

            values = new long[] { short.MinValue, sbyte.MinValue - 1, sbyte.MaxValue + 1, short.MaxValue };
            foreach (var value in values)
            {
                var bytes = Build((ref BinaryTupleBuilder b) => b.AppendLong(value));

                Assert.AreEqual(2, bytes[1]);
                Assert.AreEqual(4, bytes.Length);

                BinaryTupleReader reader = new BinaryTupleReader(bytes, 1);
                Assert.AreEqual(value, reader.GetShort(0));
                Assert.AreEqual(value, reader.GetInt(0));
                Assert.AreEqual(value, reader.GetLong(0));
            }

            values = new long[] { int.MinValue, short.MinValue - 1, short.MaxValue + 1, int.MaxValue };
            foreach (var value in values)
            {
                var bytes = Build((ref BinaryTupleBuilder b) => b.AppendLong(value));

                Assert.AreEqual(4, bytes[1]);
                Assert.AreEqual(6, bytes.Length);

                BinaryTupleReader reader = new BinaryTupleReader(bytes, 1);
                Assert.AreEqual(value, reader.GetInt(0));
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

                Assert.AreEqual(4, bytes[1]);
                Assert.AreEqual(6, bytes.Length);

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

                Assert.AreEqual(4, bytes[1]);
                Assert.AreEqual(6, bytes.Length);

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
            var values = new[]
            {
                "ascii",
                "我愛Java",
                string.Empty,
                "a string with a bit more characters",
                ((char)BinaryTupleCommon.VarlenEmptyByte).ToString()
            };

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
        public void TestVarlenEmptyByte()
        {
            var bytes = new[] { BinaryTupleCommon.VarlenEmptyByte };
            var reader = BuildAndRead((ref BinaryTupleBuilder b) => b.AppendBytes(bytes));
            var res = reader.GetBytes(0);

            CollectionAssert.AreEqual(bytes, res);
        }

        [Test]
        public void TestBytesSpan([Values(0, 1, 123)] int count)
        {
            var bytes = Enumerable.Range(1, count).Select(x => (byte)x).ToArray();
            var reader = BuildAndRead((ref BinaryTupleBuilder b) => b.AppendBytes(bytes));
            var res = reader.GetBytesSpan(0).ToArray();

            CollectionAssert.AreEqual(bytes, res);
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
                () => BuildAndRead((ref BinaryTupleBuilder b) => b.AppendBytes(new byte[] { 64, 64, 64 })).GetDecimal(0, scale));

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
                    b.AppendDate(new LocalDate(1, 1, 1));
                },
                5);

            Assert.AreEqual(default(LocalDate), reader.GetDate(0));
            Assert.AreEqual(val, reader.GetDate(1));
            Assert.AreEqual(LocalDate.MaxIsoValue, reader.GetDate(2));
            Assert.AreEqual(LocalDate.MinIsoValue, reader.GetDate(3));
            Assert.AreEqual(new LocalDate(1, 1, 1), reader.GetDate(4));
        }

        [Test]
        public void TestTime()
        {
            var val = LocalDateTime.FromDateTime(DateTime.UtcNow).TimeOfDay;

            var reader = BuildAndRead(
                (ref BinaryTupleBuilder b) =>
                {
                    b.AppendTime(default, 0);
                    b.AppendTime(val, TemporalTypes.MaxTimePrecision);
                    b.AppendTime(LocalTime.MinValue, TemporalTypes.MaxTimePrecision);
                    b.AppendTime(LocalTime.MaxValue, TemporalTypes.MaxTimePrecision);
                    b.AppendTime(LocalTime.Midnight, 0);
                    b.AppendTime(LocalTime.Noon, 0);
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
                    b.AppendDateTime(default, 0);
                    b.AppendDateTime(val, TemporalTypes.MaxTimePrecision);
                    b.AppendDateTime(LocalDateTime.MaxIsoValue, TemporalTypes.MaxTimePrecision);
                    b.AppendDateTime(LocalDateTime.MinIsoValue, TemporalTypes.MaxTimePrecision);
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
                    b.AppendTimestamp(default, 0);
                    b.AppendTimestamp(val, TemporalTypes.MaxTimePrecision);
                    b.AppendTimestamp(Instant.MaxValue, TemporalTypes.MaxTimePrecision);
                    b.AppendTimestamp(Instant.MinValue, TemporalTypes.MaxTimePrecision);
                    b.AppendTimestamp(NodaConstants.BclEpoch, TemporalTypes.MaxTimePrecision);
                    b.AppendTimestamp(NodaConstants.JulianEpoch, TemporalTypes.MaxTimePrecision);
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
        public void TestGetNullable()
        {
            var reader = BuildAndRead((ref BinaryTupleBuilder b) => b.AppendNull());

            Assert.IsNull(reader.GetIntNullable(0));
            Assert.IsNull(reader.GetByteNullable(0));
            Assert.IsNull(reader.GetShortNullable(0));
            Assert.IsNull(reader.GetLongNullable(0));
            Assert.IsNull(reader.GetDoubleNullable(0));
            Assert.IsNull(reader.GetFloatNullable(0));
            Assert.IsNull(reader.GetDecimalNullable(0, 123));
            Assert.IsNull(reader.GetStringNullable(0));
            Assert.IsNull(reader.GetGuidNullable(0));
            Assert.IsNull(reader.GetBytesNullable(0));
            Assert.IsNull(reader.GetDurationNullable(0));
            Assert.IsNull(reader.GetPeriodNullable(0));
            Assert.IsNull(reader.GetTimeNullable(0));
            Assert.IsNull(reader.GetDateNullable(0));
            Assert.IsNull(reader.GetDateTimeNullable(0));
            Assert.IsNull(reader.GetBoolNullable(0));
        }

        [Test]
        public void TestAppendNullable()
        {
            var guid = Guid.NewGuid();
            var utcNow = DateTime.UtcNow;
            var date = LocalDate.FromDateTime(utcNow);
            var dateTime = LocalDateTime.FromDateTime(utcNow);
            var bytes = new byte[] { 1, 2 };

            var reader = BuildAndRead(
                (ref BinaryTupleBuilder b) =>
                {
                    b.AppendByteNullable(1);
                    b.AppendByteNullable(null);
                    b.AppendShortNullable(1);
                    b.AppendShortNullable(null);
                    b.AppendIntNullable(1);
                    b.AppendIntNullable(null);
                    b.AppendLongNullable(1);
                    b.AppendLongNullable(null);
                    b.AppendFloatNullable(1);
                    b.AppendFloatNullable(null);
                    b.AppendDoubleNullable(1);
                    b.AppendDoubleNullable(null);
                    b.AppendStringNullable("s");
                    b.AppendStringNullable(null);
                    b.AppendBytesNullable(bytes);
                    b.AppendBytesNullable(null);
                    b.AppendGuidNullable(guid);
                    b.AppendGuidNullable(null);
                    b.AppendDecimalNullable(1, 3);
                    b.AppendDecimalNullable(null, 3);
                    b.AppendDateNullable(date);
                    b.AppendDateNullable(null);
                    b.AppendTimeNullable(dateTime.TimeOfDay, TemporalTypes.MaxTimePrecision);
                    b.AppendTimeNullable(null, 0);
                    b.AppendDateTimeNullable(dateTime, TemporalTypes.MaxTimePrecision);
                    b.AppendDateTimeNullable(null, 0);
                    b.AppendTimestampNullable(Instant.FromDateTimeUtc(utcNow), TemporalTypes.MaxTimePrecision);
                    b.AppendTimestampNullable(null, 0);
                    b.AppendDurationNullable(Duration.FromMinutes(1));
                    b.AppendDurationNullable(null);
                    b.AppendPeriodNullable(Period.FromDays(1));
                    b.AppendPeriodNullable(null);
                },
                100);

            Assert.AreEqual(1, reader.GetByteNullable(0));
            Assert.IsNull(reader.GetByteNullable(1));
            Assert.AreEqual(1, reader.GetShortNullable(2));
            Assert.IsNull(reader.GetShortNullable(3));
            Assert.AreEqual(1, reader.GetIntNullable(4));
            Assert.IsNull(reader.GetIntNullable(5));
            Assert.AreEqual(1, reader.GetLongNullable(6));
            Assert.IsNull(reader.GetLongNullable(7));
            Assert.AreEqual(1, reader.GetFloatNullable(8));
            Assert.IsNull(reader.GetFloatNullable(9));
            Assert.AreEqual(1, reader.GetDoubleNullable(10));
            Assert.IsNull(reader.GetDoubleNullable(11));
            Assert.AreEqual("s", reader.GetStringNullable(12));
            Assert.IsNull(reader.GetStringNullable(13));
            Assert.AreEqual(bytes, reader.GetBytesNullable(14));
            Assert.IsNull(reader.GetBytesNullable(15));
            Assert.AreEqual(guid, reader.GetGuidNullable(16));
            Assert.IsNull(reader.GetGuidNullable(17));
            Assert.AreEqual(1, reader.GetDecimalNullable(18, 3));
            Assert.IsNull(reader.GetDecimalNullable(19, 3));
            Assert.AreEqual(date, reader.GetDateNullable(20));
            Assert.IsNull(reader.GetDateNullable(21));
            Assert.AreEqual(dateTime.TimeOfDay, reader.GetTimeNullable(22));
            Assert.IsNull(reader.GetTimeNullable(23));
            Assert.AreEqual(dateTime, reader.GetDateTimeNullable(24));
            Assert.IsNull(reader.GetDateTimeNullable(25));
            Assert.AreEqual(Instant.FromDateTimeUtc(utcNow), reader.GetTimestampNullable(26));
            Assert.IsNull(reader.GetTimestampNullable(27));
            Assert.AreEqual(Duration.FromMinutes(1), reader.GetDurationNullable(28));
            Assert.IsNull(reader.GetDurationNullable(29));
            Assert.AreEqual(Period.FromDays(1), reader.GetPeriodNullable(30));
            Assert.IsNull(reader.GetPeriodNullable(31));
        }

        [Test]
        public void TestObject()
        {
            var guid = Guid.NewGuid();
            var utcNow = DateTime.UtcNow;
            var date = LocalDate.FromDateTime(utcNow);
            var dateTime = LocalDateTime.FromDateTime(utcNow);
            var bytes = new byte[] { 1, 2 };

            var reader = BuildAndRead(
                (ref BinaryTupleBuilder b) =>
                {
                    b.AppendObject(null, ColumnType.String);
                    b.AppendObject(sbyte.MaxValue, ColumnType.Int8);
                    b.AppendObject(short.MaxValue, ColumnType.Int16);
                    b.AppendObject(int.MaxValue, ColumnType.Int32);
                    b.AppendObject(long.MaxValue, ColumnType.Int64);
                    b.AppendObject(float.MaxValue, ColumnType.Float);
                    b.AppendObject(double.MaxValue, ColumnType.Double);
                    b.AppendObject(decimal.One, ColumnType.Decimal);
                    b.AppendObject("foo", ColumnType.String);
                    b.AppendObject(guid, ColumnType.Uuid);
                    b.AppendObject(bytes, ColumnType.ByteArray);
                    b.AppendObject(LocalTime.FromMinutesSinceMidnight(123), ColumnType.Time, precision: TemporalTypes.MaxTimePrecision);
                    b.AppendObject(date, ColumnType.Date);
                    b.AppendObject(dateTime, ColumnType.Datetime, precision: TemporalTypes.MaxTimePrecision);
                    b.AppendObject(Instant.FromDateTimeUtc(utcNow), ColumnType.Timestamp, precision: TemporalTypes.MaxTimePrecision);
                },
                15);

            Assert.IsNull(reader.GetObject(0, ColumnType.String));
            Assert.AreEqual(sbyte.MaxValue, reader.GetObject(1, ColumnType.Int8));
            Assert.AreEqual(short.MaxValue, reader.GetObject(2, ColumnType.Int16));
            Assert.AreEqual(int.MaxValue, reader.GetObject(3, ColumnType.Int32));
            Assert.AreEqual(long.MaxValue, reader.GetObject(4, ColumnType.Int64));
            Assert.AreEqual(float.MaxValue, reader.GetObject(5, ColumnType.Float));
            Assert.AreEqual(double.MaxValue, reader.GetObject(6, ColumnType.Double));
            Assert.AreEqual(decimal.One, reader.GetObject(7, ColumnType.Decimal));
            Assert.AreEqual("foo", reader.GetObject(8, ColumnType.String));
            Assert.AreEqual(guid, reader.GetObject(9, ColumnType.Uuid));
            Assert.AreEqual(bytes, reader.GetObject(10, ColumnType.ByteArray));
            Assert.AreEqual(LocalTime.FromMinutesSinceMidnight(123), reader.GetObject(11, ColumnType.Time));
            Assert.AreEqual(date, reader.GetObject(12, ColumnType.Date));
            Assert.AreEqual(dateTime, reader.GetObject(13, ColumnType.Datetime));
            Assert.AreEqual(Instant.FromDateTimeUtc(utcNow), reader.GetObject(14, ColumnType.Timestamp));
        }

        [Test]
        public void TestObjectWithType()
        {
            var guid = Guid.NewGuid();
            var utcNow = DateTime.UtcNow;
            var date = LocalDate.FromDateTime(utcNow);
            var dateTime = LocalDateTime.FromDateTime(utcNow);
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
                    b.AppendObjectWithType("foo");
                    b.AppendObjectWithType(guid);
                    b.AppendObjectWithType(bytes);
                    b.AppendObjectWithType(LocalTime.FromMinutesSinceMidnight(123));
                    b.AppendObjectWithType(date);
                    b.AppendObjectWithType(dateTime);
                    b.AppendObjectWithType(Instant.FromDateTimeUtc(utcNow));
                    b.AppendObjectWithType(true);
                    b.AppendObjectWithType(Period.FromDays(2));
                    b.AppendObjectWithType(Duration.FromDays(3));
                },
                18 * 3);

            Assert.IsNull(reader.GetObject(0));
            Assert.AreEqual(sbyte.MaxValue, reader.GetObject(3));
            Assert.AreEqual(short.MaxValue, reader.GetObject(6));
            Assert.AreEqual(int.MaxValue, reader.GetObject(9));
            Assert.AreEqual(long.MaxValue, reader.GetObject(12));
            Assert.AreEqual(float.MaxValue, reader.GetObject(15));
            Assert.AreEqual(double.MaxValue, reader.GetObject(18));
            Assert.AreEqual(decimal.One, reader.GetObject(21));
            Assert.AreEqual("foo", reader.GetObject(24));
            Assert.AreEqual(guid, reader.GetObject(27));
            Assert.AreEqual(bytes, reader.GetObject(30));
            Assert.AreEqual(LocalTime.FromMinutesSinceMidnight(123), reader.GetObject(33));
            Assert.AreEqual(date, reader.GetObject(36));
            Assert.AreEqual(dateTime, reader.GetObject(39));
            Assert.AreEqual(Instant.FromDateTimeUtc(utcNow), reader.GetObject(42));
            Assert.IsTrue((bool)reader.GetObject(45)!);
            Assert.AreEqual(Period.FromDays(2), reader.GetObject(48));
            Assert.AreEqual(Duration.FromDays(3), reader.GetObject(51));
        }

        [Test]
        public void TestInvalidElementLengthThrowsException()
        {
            var bytes = Build((ref BinaryTupleBuilder b) => b.AppendBytes(new byte[1000]));

            Test(() => new BinaryTupleReader(bytes, 1).GetByte(0), 1);
            Test(() => new BinaryTupleReader(bytes, 1).GetByteNullable(0), 1);

            Test(() => new BinaryTupleReader(bytes, 1).GetShort(0), 2);
            Test(() => new BinaryTupleReader(bytes, 1).GetShortNullable(0), 2);

            Test(() => new BinaryTupleReader(bytes, 1).GetInt(0), 4);
            Test(() => new BinaryTupleReader(bytes, 1).GetIntNullable(0), 4);

            Test(() => new BinaryTupleReader(bytes, 1).GetLong(0), 8);
            Test(() => new BinaryTupleReader(bytes, 1).GetLongNullable(0), 8);

            Test(() => new BinaryTupleReader(bytes, 1).GetFloat(0), 4);
            Test(() => new BinaryTupleReader(bytes, 1).GetFloatNullable(0), 4);

            Test(() => new BinaryTupleReader(bytes, 1).GetDouble(0), 8);
            Test(() => new BinaryTupleReader(bytes, 1).GetDoubleNullable(0), 8);

            Test(() => new BinaryTupleReader(bytes, 1).GetGuid(0), 16);
            Test(() => new BinaryTupleReader(bytes, 1).GetGuidNullable(0), 16);

            Test(() => new BinaryTupleReader(bytes, 1).GetDate(0), 3);
            Test(() => new BinaryTupleReader(bytes, 1).GetDateNullable(0), 3);

            Test(() => new BinaryTupleReader(bytes, 1).GetTime(0), 6);
            Test(() => new BinaryTupleReader(bytes, 1).GetTimeNullable(0), 6);

            Test(() => new BinaryTupleReader(bytes, 1).GetDateTime(0), 9);
            Test(() => new BinaryTupleReader(bytes, 1).GetDateTimeNullable(0), 9);

            Test(() => new BinaryTupleReader(bytes, 1).GetPeriod(0), 12);
            Test(() => new BinaryTupleReader(bytes, 1).GetPeriodNullable(0), 12);

            Test(() => new BinaryTupleReader(bytes, 1).GetDuration(0), 12);
            Test(() => new BinaryTupleReader(bytes, 1).GetDurationNullable(0), 12);

            Test(() => new BinaryTupleReader(bytes, 1).GetTimestamp(0), 12);
            Test(() => new BinaryTupleReader(bytes, 1).GetTimestampNullable(0), 12);

            static void Test(TestDelegate testDelegate, int expectedLength)
            {
                var ex = Assert.Throws<InvalidOperationException>(testDelegate);

                Assert.AreEqual(
                    $"Binary tuple element with index 0 has invalid length (expected {expectedLength}, actual 1000).",
                    ex!.Message);
            }
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
