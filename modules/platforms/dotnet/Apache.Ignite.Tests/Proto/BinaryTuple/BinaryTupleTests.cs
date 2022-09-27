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
    using Internal.Proto.BinaryTuple;
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

            Test(12345.6789m, 4);
            Test(12345.678m, 4);
            Test(12345.67m, 4);

            Test(12345.6789m, 2, 12345.67m);
            Test(12345.6789m, 0, 12345m);

            static void Test(decimal val, int scale, decimal? expected = null)
            {
                var reader = BuildAndRead((ref BinaryTupleBuilder b) => b.AppendDecimal(val, scale));
                var res = reader.GetDecimal(0, scale);

                Assert.AreEqual(expected ?? val, res);
            }
        }

        [Test]
        public void TestDate()
        {
            Assert.Fail("TODO IGNITE-15431");
        }

        [Test]
        public void TestTime()
        {
            Assert.Fail("TODO IGNITE-15431");
        }

        [Test]
        public void TestDateTime()
        {
            Assert.Fail("TODO IGNITE-15431");
        }

        [Test]
        public void TestTimestamp()
        {
            Assert.Fail("TODO IGNITE-15431");
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
