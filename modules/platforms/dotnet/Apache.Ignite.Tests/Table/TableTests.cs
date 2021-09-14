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
    using System.Globalization;
    using System.Linq;
    using System.Threading.Tasks;
    using Ignite.Table;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ITable"/>.
    /// </summary>
    public class TableTests : IgniteTestsBase
    {
        [Test]
        public async Task TestUpsertGet()
        {
            await Table.UpsertAsync(GetTuple(1, "foo"));

            var keyTuple = GetTuple(1);
            var resTuple = (await Table.GetAsync(keyTuple))!;

            Assert.IsNotNull(resTuple);
            Assert.AreEqual(2, resTuple.FieldCount);
            Assert.AreEqual(1L, resTuple["key"]);
            Assert.AreEqual("foo", resTuple["val"]);
        }

        [Test]
        public async Task TestUpsertOverridesPreviousValue()
        {
            var key = GetTuple(1);

            await Table.UpsertAsync(GetTuple(1, "foo"));
            Assert.AreEqual("foo", (await Table.GetAsync(key))![1]);

            await Table.UpsertAsync(GetTuple(1, "bar"));
            Assert.AreEqual("bar", (await Table.GetAsync(key))![1]);
        }

        [Test]
        public async Task TestUpsertCustomTuple()
        {
            await Table.UpsertAsync(new CustomTestIgniteTuple());

            var res = await Table.GetAsync(GetTuple(CustomTestIgniteTuple.Key));

            Assert.IsNotNull(res);
            Assert.AreEqual(CustomTestIgniteTuple.Value, res![1]);
        }

        [Test]
        public void TestUpsertEmptyTupleThrowsException()
        {
            var ex = Assert.ThrowsAsync<IgniteClientException>(async () => await Table.UpsertAsync(new IgniteTuple()));

            Assert.AreEqual(
                "Failed to set column (null was passed, but column is not nullable):" +
                " [col=Column [schemaIndex=0, name=key, type=NativeType [name=INT32, sizeInBytes=4, fixed=true]," +
                " nullable=false]]",
                ex!.Message);
        }

        [Test]
        public async Task TestDelete()
        {
            await Table.UpsertAsync(GetTuple(1, "1"));

            Assert.IsFalse(await Table.DeleteAsync(GetTuple(-1)));
            Assert.IsTrue(await Table.DeleteAsync(GetTuple(1)));
            Assert.IsNull(await Table.GetAsync(GetTuple(1)));
        }

        [Test]
        public async Task TestUpsertAll()
        {
            var ids = Enumerable.Range(1, 10).ToList();
            var records = ids.Select(x => GetTuple(x, x.ToString(CultureInfo.InvariantCulture)));

            await Table.UpsertAllAsync(records);

            foreach (var id in ids)
            {
                var res = await Table.GetAsync(GetTuple(id));
                Assert.AreEqual(id.ToString(CultureInfo.InvariantCulture), res![1]);
            }
        }

        [Test]
        public async Task TestUpsertAllLazyCollection()
        {
            // TODO
            await Task.Yield();
        }

        [Test]
        public async Task TestUpsertAllThrowsOnNullCollectionElement()
        {
            // TODO
            await Task.Yield();
        }

        private static IIgniteTuple GetTuple(int id, string? val = null) =>
            new IgniteTuple { [KeyCol] = id, [ValCol] = val };
    }
}
