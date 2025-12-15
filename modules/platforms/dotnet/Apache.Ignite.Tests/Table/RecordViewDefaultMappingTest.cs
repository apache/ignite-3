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

// ReSharper disable InconsistentNaming, NotAccessedField.Local, NotAccessedPositionalProperty.Local
#pragma warning disable SA1201 // Type member order
namespace Apache.Ignite.Tests.Table
{
    using System;
    using System.Threading.Tasks;
    using Common.Table;
    using Ignite.Table;
    using NUnit.Framework;

    /// <summary>
    /// Tests the default user type mapping behavior in <see cref="IRecordView{T}"/>.
    /// </summary>
    public class RecordViewDefaultMappingTest : IgniteTestsBase
    {
        [SetUp]
        public async Task InsertData()
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
            FieldsTest res = Get(new FieldsTest(1, null));

            Assert.AreEqual("2", res.GetVal());
        }

        [Test]
        public void TestRecordMapping()
        {
            RecordTest res = Get(new RecordTest(1, null, Guid.Empty));

            Assert.AreEqual("2", res.Val);
        }

        [Test]
        public void TestAnonymousTypeMapping()
        {
            var key = new
            {
                Key = 1L,
                Val = "unused",
                Date = DateTime.Now
            };

            var res = Get(key);

            Assert.AreEqual("2", res.Val);
        }

        private T Get<T>(T key)
            where T : notnull => Table.GetRecordView<T>().GetAsync(null, key).GetAwaiter().GetResult().Value;

        private class FieldsTest
        {
            private readonly long key;

            private readonly string? val;

            public FieldsTest(long key, string? val)
            {
                this.key = key;
                this.val = val;
            }

            public string? GetVal() => val;
        }

        private record RecordTest(long Key, string? Val, Guid Id);
    }
}
