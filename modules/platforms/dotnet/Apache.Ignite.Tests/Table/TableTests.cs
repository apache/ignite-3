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
    using System;
    using System.Collections.Generic;
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
        [SetUp]
        public async Task SetUp()
        {
            // Clean up test data.
            await Table.DeleteAllAsync(Enumerable.Range(0, 100).Select(x => GetTuple(x)));
        }

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
        public async Task TestInsertNonExistingKeyCreatesRecordReturnsTrue()
        {
            var res = await Table.InsertAsync(GetTuple(1, "1"));

            Assert.IsTrue(res);
            Assert.IsTrue(await Table.GetAsync(GetTuple(1)) != null);
        }

        [Test]
        public async Task TestInsertExistingKeyDoesNotOverwriteReturnsFalse()
        {
            await Table.UpsertAsync(GetTuple(1, "1"));
            var res = await Table.InsertAsync(GetTuple(1, "2"));

            Assert.IsFalse(res);
            Assert.AreEqual("1", (await Table.GetAsync(GetTuple(1)))![1]);
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
        public async Task TestDeleteExact()
        {
            await Table.UpsertAsync(GetTuple(1, "1"));

            Assert.IsFalse(await Table.DeleteExactAsync(GetTuple(-1)));
            Assert.IsFalse(await Table.DeleteExactAsync(GetTuple(1)));
            Assert.IsNotNull(await Table.GetAsync(GetTuple(1)));

            Assert.IsTrue(await Table.DeleteExactAsync(GetTuple(1, "1")));
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
        public async Task TestUpsertAllOverwritesExistingData()
        {
            await Table.InsertAsync(GetTuple(2, "x"));
            await Table.InsertAsync(GetTuple(4, "y"));

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
        public async Task TestInsertAll()
        {
            var ids = Enumerable.Range(1, 10).ToList();
            var records = ids.Select(x => GetTuple(x, x.ToString(CultureInfo.InvariantCulture)));

            IList<IIgniteTuple> skipped = await Table.InsertAllAsync(records);

            CollectionAssert.IsEmpty(skipped);

            foreach (var id in ids)
            {
                var res = await Table.GetAsync(GetTuple(id));
                Assert.AreEqual(id.ToString(CultureInfo.InvariantCulture), res![1]);
            }
        }

        [Test]
        public async Task TestInsertAllDoesNotOverwriteExistingDataReturnsSkippedTuples()
        {
            var existing = new[] { GetTuple(2, "x"), GetTuple(4, "y") }.ToDictionary(x => x[0]);
            await Table.InsertAllAsync(existing.Values);

            var ids = Enumerable.Range(1, 10).ToList();
            var records = ids.Select(x => GetTuple(x, x.ToString(CultureInfo.InvariantCulture)));

            IList<IIgniteTuple> skipped = await Table.InsertAllAsync(records);
            var skippedArr = skipped.OrderBy(x => x[0]).ToArray();

            Assert.AreEqual(2, skippedArr.Length);
            Assert.AreEqual(2, skippedArr[0][0]);
            Assert.AreEqual("2", skippedArr[0][1]);

            Assert.AreEqual(4, skippedArr[1][0]);
            Assert.AreEqual("4", skippedArr[1][1]);

            foreach (var id in ids)
            {
                var res = await Table.GetAsync(GetTuple(id));

                if (existing.TryGetValue(res![0], out var old))
                {
                    Assert.AreEqual(old[1], res[1]);
                }
                else
                {
                    Assert.AreEqual(id.ToString(CultureInfo.InvariantCulture), res[1]);
                }
            }
        }

        [Test]
        public async Task TestGetAll()
        {
            var records = Enumerable
                .Range(1, 10)
                .Select(x => GetTuple(x, x.ToString(CultureInfo.InvariantCulture)));

            await Table.UpsertAllAsync(records);

            var res = await Table.GetAllAsync(Enumerable.Range(9, 4).Select(x => GetTuple(x)));
            var resArr = res.OrderBy(x => x[0]).ToArray();

            Assert.AreEqual(2, res.Count);

            Assert.AreEqual(9, resArr[0][0]);
            Assert.AreEqual("9", resArr[0][1]);

            Assert.AreEqual(10, resArr[1][0]);
            Assert.AreEqual("10", resArr[1][1]);
        }

        [Test]
        public async Task TestGetAllNonExistentKeysReturnsEmptyList()
        {
            var res = await Table.GetAllAsync(new[] { GetTuple(-100) });

            Assert.AreEqual(0, res.Count);
        }

        [Test]
        public async Task TestGetAllEmptyKeysReturnsEmptyList()
        {
            var res = await Table.GetAllAsync(Array.Empty<IIgniteTuple>());

            Assert.AreEqual(0, res.Count);
        }

        [Test]
        public void TestUpsertAllThrowsArgumentExceptionOnNullCollectionElement()
        {
            var ex = Assert.ThrowsAsync<ArgumentException>(
                async () => await Table.UpsertAllAsync(new[] { GetTuple(1, "1"), null! }));

            Assert.AreEqual("Tuple collection can't contain null elements.", ex!.Message);
        }

        private static IIgniteTuple GetTuple(int id, string? val = null) =>
            new IgniteTuple { [KeyCol] = id, [ValCol] = val };
    }
}
