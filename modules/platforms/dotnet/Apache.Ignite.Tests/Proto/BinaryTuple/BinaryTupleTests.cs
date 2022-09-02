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
    using Internal.Proto.BinaryTuple;
    using NUnit.Framework;

    /// <summary>
    /// Tests for binary tuple.
    /// </summary>
    public class BinaryTupleTests
    {
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

            var ex = Assert.Throws<InvalidOperationException>(() => reader.GetString(0));
            Assert.AreEqual("Binary tuple element with index 0 is null.", ex!.Message);
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
            using var builder = new BinaryTupleBuilder(numElements: 1, allowNulls: false, totalValueSize: 1);
            builder.AppendByte(value);
            var res = builder.Build().ToArray();
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
                using var builder = new BinaryTupleBuilder(1, false, 1);
                builder.AppendShort(value);
                var bytes = builder.Build();

                Assert.AreEqual(value != 0 ? 1 : 0, bytes.Span[1]);
                Assert.AreEqual(value != 0 ? 3 : 2, bytes.Length);

                var reader = new BinaryTupleReader(bytes, 1);
                Assert.AreEqual(value, reader.GetShort(0));
            }

            values = new short[] { short.MinValue, sbyte.MinValue - 1, sbyte.MaxValue + 1, short.MaxValue };

            foreach (var value in values)
            {
                using var builder = new BinaryTupleBuilder(1, false, 2);
                builder.AppendShort(value);
                var bytes = builder.Build();
                Assert.AreEqual(2, bytes.Span[1]);
                Assert.AreEqual(4, bytes.Length);

                var reader = new BinaryTupleReader(bytes, 1);
                Assert.AreEqual(value, reader.GetShort(0));
            }
        }

        [Test]
        public void TestLong()
        {
            int[] values = { sbyte.MinValue, -1, 0, 1, sbyte.MaxValue };
            foreach (int value in values)
            {
                var builder = new BinaryTupleBuilder(1, false, 1);
                builder.AppendInt(value);
                var bytes = builder.Build();

                Assert.AreEqual(value != 0 ? 1 : 0, bytes.Span[1]);
                Assert.AreEqual(value != 0 ? 3 : 2, bytes.Length);

                var reader = new BinaryTupleReader(bytes, 1);
                Assert.AreEqual(value, reader.GetInt(0));
            }

            values = new[] { short.MinValue, sbyte.MinValue - 1, sbyte.MaxValue + 1, short.MaxValue };
            foreach (int value in values)
            {
                var builder = new BinaryTupleBuilder(1, false, 2);
                builder.AppendInt(value);
                var bytes = builder.Build();

                Assert.AreEqual(2, bytes.Span[1]);
                Assert.AreEqual(4, bytes.Length);

                var reader = new BinaryTupleReader(bytes, 1);
                Assert.AreEqual(value, reader.GetInt(0));
            }

            values = new[] { int.MinValue, short.MinValue - 1, short.MaxValue + 1, int.MaxValue };
            foreach (int value in values)
            {
                var builder = new BinaryTupleBuilder(1, false, 3);
                builder.AppendInt(value);
                var bytes = builder.Build();

                Assert.AreEqual(4, bytes.Span[1]);
                Assert.AreEqual(6, bytes.Length);

                BinaryTupleReader reader = new BinaryTupleReader(bytes, 1);
                Assert.AreEqual(value, reader.GetInt(0));
            }
        }

        [Test]
        public void TestString()
        {
            var values = new[] {"ascii", "我愛Java", string.Empty, "a string with a bit more characters"};

            using var builder = new BinaryTupleBuilder(values.Length);
            foreach (var value in values)
            {
                builder.AppendString(value);
            }

            var res = builder.Build();
            var reader = new BinaryTupleReader(res, values.Length);

            for (var i = 0; i < values.Length; i++)
            {
               Assert.AreEqual(values[i], reader.GetString(i));
            }
        }

        [Test]
        public void TestGuid()
        {
            var guid = Guid.NewGuid();

            using var builder = new BinaryTupleBuilder(2);
            builder.AppendGuid(Guid.Empty);
            builder.AppendGuid(guid);

            var reader = new BinaryTupleReader(builder.Build(), 2);

            Assert.AreEqual(Guid.Empty, reader.GetGuid(0));
            Assert.AreEqual(guid, reader.GetGuid(1));
        }
    }
}
