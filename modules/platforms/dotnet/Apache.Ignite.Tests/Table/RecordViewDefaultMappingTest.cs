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

namespace Apache.Ignite.Tests.Table
{
    using System.Threading.Tasks;
    using Ignite.Table;
    using NUnit.Framework;
    using NUnit.Framework.Internal;

    /// <summary>
    /// Tests the default user type mapping behavior in <see cref="IRecordView{T}"/>.
    /// </summary>
    public class RecordViewDefaultMappingTest : IgniteTestsBase
    {
        [SetUp]
        public async Task SetUp()
        {
            await Table.RecordBinaryView.UpsertAsync(null, GetTuple(1, "2"));
        }

        [Test]
        public void TestPropertyMapping()
        {
            Poco res = Get(new Poco { Key = 1 });

            Assert.AreEqual("2", res.Val);
        }

        [Test]
        public void TestFieldMappingNoDefaultConstructor()
        {
            Fields res = Get(new Fields(1, null));

            Assert.AreEqual("2", res.GetVal());
        }

        [Test]
        public void TestRecordMapping()
        {
            Assert.Fail("TODO");
        }

        [Test]
        public void TestAnonymousTypeMapping()
        {
            Assert.Fail("TODO");
        }

        private T Get<T>(T key)
            where T : class
        {
            return Table.GetRecordView<T>().GetAsync(null, key).GetAwaiter().GetResult()!;
        }

        private class Fields
        {
            private long key;

            private string? val;

            public Fields(long key, string? val)
            {
                this.key = key;
                this.val = val;
            }

            public string? GetVal() => val;
        }
    }
}
