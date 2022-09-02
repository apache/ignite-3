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
