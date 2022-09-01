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
    using Internal.Proto.BinaryTuple;
    using NUnit.Framework;

    /// <summary>
    /// Tests for binary tuple.
    /// </summary>
    public class BinaryTupleTests
    {
        [Test]
        public void ByteTest([Values(0, 1, sbyte.MaxValue, sbyte.MinValue)] sbyte value)
        {
            using var builder = new BinaryTupleBuilder(numElements: 1, allowNulls: false, totalValueSize: 1);
            builder.AppendByte(value);
            var res = builder.Build().ToArray();
            Assert.AreEqual(value != 0 ? 1 : 0, res[1]);
            Assert.AreEqual(value != 0 ? 3 : 2, res.Length);

            // TODO
            // BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            // assertEquals(value, reader.byteValue(0));
        }
    }
}
