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
        [Test]
        public async Task TestUpsertGet()
        {
            await Table.UpsertAsync(null, GetTuple(1, "foo"));

            var keyTuple = GetTuple(1);
            var resTuple = (await Table.GetAsync(null, keyTuple))!;

            Assert.IsNotNull(resTuple);
            Assert.AreEqual(2, resTuple.FieldCount);
            Assert.AreEqual(1L, resTuple["key"]);
            Assert.AreEqual("foo", resTuple["val"]);
        }

        [Test]
        public async Task TestUpsertOverridesPreviousValue()
        {
            var key = GetTuple(1);

            await Table.UpsertAsync(null, GetTuple(1, "foo"));
            Assert.AreEqual("foo", (await Table.GetAsync(null, key))![1]);

            await Table.UpsertAsync(null, GetTuple(1, "bar"));
            Assert.AreEqual("bar", (await Table.GetAsync(null, key))![1]);
        }

        [Test]
        public async Task TestUpsertAllowsCustomTupleImplementation()
        {
            await Table.UpsertAsync(null, new CustomTestIgniteTuple());

            var res = await Table.GetAsync(null, GetTuple(CustomTestIgniteTuple.Key));

            Assert.IsNotNull(res);
            Assert.AreEqual(CustomTestIgniteTuple.Value, res![1]);
        }

        [Test]
        public void TestUpsertEmptyTupleThrowsException()
        {
            var ex = Assert.ThrowsAsync<IgniteClientException>(async () => await Table.UpsertAsync(null, new IgniteTuple()));

            Assert.AreEqual(
                "Missed key column: key",
                ex!.Message);
        }

        [Test]
        public async Task TestGetAndUpsertNonExistentRecordReturnsNull()
        {
            IIgniteTuple? res = await Table.GetAndUpsertAsync(null, GetTuple(2, "2"));

            Assert.IsNull(res);
            Assert.AreEqual("2", (await Table.GetAsync(null, GetTuple(2)))![1]);
        }

        [Test]
        public async Task TestGetAndUpsertExistingRecordOverwritesAndReturns()
        {
            await Table.UpsertAsync(null, GetTuple(2, "2"));
            IIgniteTuple? res = await Table.GetAndUpsertAsync(null, GetTuple(2, "22"));

            Assert.IsNotNull(res);
            Assert.AreEqual(2, res![0]);
            Assert.AreEqual("2", res[1]);
            Assert.AreEqual("22", (await Table.GetAsync(null, GetTuple(2)))![1]);
        }

        [Test]
        public async Task TestGetAndDeleteNonExistentRecordReturnsNull()
        {
            IIgniteTuple? res = await Table.GetAndDeleteAsync(null, GetTuple(2, "2"));

            Assert.IsNull(res);
            Assert.IsNull(await Table.GetAsync(null, GetTuple(2)));
        }

        [Test]
        public async Task TestGetAndDeleteExistingRecordRemovesAndReturns()
        {
            await Table.UpsertAsync(null, GetTuple(2, "2"));
            IIgniteTuple? res = await Table.GetAndDeleteAsync(null, GetTuple(2));

            Assert.IsNotNull(res);
            Assert.AreEqual(2, res![0]);
            Assert.AreEqual("2", res[1]);
            Assert.IsNull(await Table.GetAsync(null, GetTuple(2)));
        }

        [Test]
        public async Task TestInsertNonExistentKeyCreatesRecordReturnsTrue()
        {
            var res = await Table.InsertAsync(null, GetTuple(1, "1"));

            Assert.IsTrue(res);
            Assert.IsTrue(await Table.GetAsync(null, GetTuple(1)) != null);
        }

        [Test]
        public async Task TestInsertExistingKeyDoesNotOverwriteReturnsFalse()
        {
            await Table.UpsertAsync(null, GetTuple(1, "1"));
            var res = await Table.InsertAsync(null, GetTuple(1, "2"));

            Assert.IsFalse(res);
            Assert.AreEqual("1", (await Table.GetAsync(null, GetTuple(1)))![1]);
        }

        [Test]
        public async Task TestDeleteNonExistentRecordReturnFalse()
        {
            Assert.IsFalse(await Table.DeleteAsync(null, GetTuple(-1)));
        }

        [Test]
        public async Task TestDeleteExistingRecordReturnsTrue()
        {
            await Table.UpsertAsync(null, GetTuple(1, "1"));

            Assert.IsTrue(await Table.DeleteAsync(null, GetTuple(1)));
            Assert.IsNull(await Table.GetAsync(null, GetTuple(1)));
        }

        [Test]
        public async Task TestDeleteExactNonExistentRecordReturnsFalse()
        {
            Assert.IsFalse(await Table.DeleteExactAsync(null, GetTuple(-1)));
        }

        [Test]
        public async Task TestDeleteExactExistingKeyDifferentValueReturnsFalseDoesNotDelete()
        {
            await Table.UpsertAsync(null, GetTuple(1, "1"));

            Assert.IsFalse(await Table.DeleteExactAsync(null, GetTuple(1)));
            Assert.IsFalse(await Table.DeleteExactAsync(null, GetTuple(1, "2")));
            Assert.IsNotNull(await Table.GetAsync(null, GetTuple(1)));
        }

        [Test]
        public async Task TestDeleteExactSameKeyAndValueReturnsTrueDeletesRecord()
        {
            await Table.UpsertAsync(null, GetTuple(1, "1"));

            Assert.IsTrue(await Table.DeleteExactAsync(null, GetTuple(1, "1")));
            Assert.IsNull(await Table.GetAsync(null, GetTuple(1)));
        }

        [Test]
        public async Task TestReplaceNonExistentRecordReturnsFalseDoesNotCreateRecord()
        {
            bool res = await Table.ReplaceAsync(null, GetTuple(1, "1"));

            Assert.IsFalse(res);
            Assert.IsNull(await Table.GetAsync(null, GetTuple(1)));
        }

        [Test]
        public async Task TestReplaceExistingRecordReturnsTrueOverwrites()
        {
            await Table.UpsertAsync(null, GetTuple(1, "1"));
            bool res = await Table.ReplaceAsync(null, GetTuple(1, "2"));

            Assert.IsTrue(res);
            Assert.AreEqual("2", (await Table.GetAsync(null, GetTuple(1)))![1]);
        }

        [Test]
        public async Task TestGetAndReplaceNonExistentRecordReturnsNullDoesNotCreateRecord()
        {
            IIgniteTuple? res = await Table.GetAndReplaceAsync(null, GetTuple(1, "1"));

            Assert.IsNull(res);
            Assert.IsNull(await Table.GetAsync(null, GetTuple(1)));
        }

        [Test]
        public async Task TestGetAndReplaceExistingRecordReturnsOldOverwrites()
        {
            await Table.UpsertAsync(null, GetTuple(1, "1"));
            IIgniteTuple? res = await Table.GetAndReplaceAsync(null, GetTuple(1, "2"));

            Assert.IsNotNull(res);
            Assert.AreEqual("1", res![1]);
            Assert.AreEqual("2", (await Table.GetAsync(null, GetTuple(1)))![1]);
        }

        [Test]
        public async Task TestReplaceExactNonExistentRecordReturnsFalseDoesNotCreateRecord()
        {
            bool res = await Table.ReplaceAsync(null, GetTuple(1, "1"), GetTuple(1, "2"));

            Assert.IsFalse(res);
            Assert.IsNull(await Table.GetAsync(null, GetTuple(1)));
        }

        [Test]
        public async Task TestReplaceExactExistingRecordWithDifferentValueReturnsFalseDoesNotReplace()
        {
            await Table.UpsertAsync(null, GetTuple(1, "1"));
            bool res = await Table.ReplaceAsync(null, GetTuple(1, "11"), GetTuple(1, "22"));

            Assert.IsFalse(res);
            Assert.AreEqual("1", (await Table.GetAsync(null, GetTuple(1)))![1]);
        }

        [Test]
        public async Task TestReplaceExactExistingRecordWithSameValueReturnsTrueReplacesOld()
        {
            await Table.UpsertAsync(null, GetTuple(1, "1"));
            bool res = await Table.ReplaceAsync(null, GetTuple(1, "1"), GetTuple(1, "22"));

            Assert.IsTrue(res);
            Assert.AreEqual("22", (await Table.GetAsync(null, GetTuple(1)))![1]);
        }

        [Test]
        public async Task TestUpsertAllCreatesRecords()
        {
            var ids = Enumerable.Range(1, 10).ToList();
            var records = ids.Select(x => GetTuple(x, x.ToString(CultureInfo.InvariantCulture)));

            await Table.UpsertAllAsync(null, records);

            foreach (var id in ids)
            {
                var res = await Table.GetAsync(null, GetTuple(id));
                Assert.AreEqual(id.ToString(CultureInfo.InvariantCulture), res![1]);
            }
        }

        [Test]
        public async Task TestUpsertAllOverwritesExistingData()
        {
            await Table.InsertAsync(null, GetTuple(2, "x"));
            await Table.InsertAsync(null, GetTuple(4, "y"));

            var ids = Enumerable.Range(1, 10).ToList();
            var records = ids.Select(x => GetTuple(x, x.ToString(CultureInfo.InvariantCulture)));

            await Table.UpsertAllAsync(null, records);

            foreach (var id in ids)
            {
                var res = await Table.GetAsync(null, GetTuple(id));
                Assert.AreEqual(id.ToString(CultureInfo.InvariantCulture), res![1]);
            }
        }

        [Test]
        public async Task TestInsertAllCreatesRecords()
        {
            var ids = Enumerable.Range(1, 10).ToList();
            var records = ids.Select(x => GetTuple(x, x.ToString(CultureInfo.InvariantCulture)));

            IList<IIgniteTuple> skipped = await Table.InsertAllAsync(null, records);

            CollectionAssert.IsEmpty(skipped);

            foreach (var id in ids)
            {
                var res = await Table.GetAsync(null, GetTuple(id));
                Assert.AreEqual(id.ToString(CultureInfo.InvariantCulture), res![1]);
            }
        }

        [Test]
        public async Task TestInsertAllDoesNotOverwriteExistingDataReturnsSkippedTuples()
        {
            var existing = new[] { GetTuple(2, "x"), GetTuple(4, "y") }.ToDictionary(x => x[0]);
            await Table.InsertAllAsync(null, existing.Values);

            var ids = Enumerable.Range(1, 10).ToList();
            var records = ids.Select(x => GetTuple(x, x.ToString(CultureInfo.InvariantCulture)));

            IList<IIgniteTuple> skipped = await Table.InsertAllAsync(null, records);
            var skippedArr = skipped.OrderBy(x => x[0]).ToArray();

            Assert.AreEqual(2, skippedArr.Length);
            Assert.AreEqual(2, skippedArr[0][0]);
            Assert.AreEqual("2", skippedArr[0][1]);

            Assert.AreEqual(4, skippedArr[1][0]);
            Assert.AreEqual("4", skippedArr[1][1]);

            foreach (var id in ids)
            {
                var res = await Table.GetAsync(null, GetTuple(id));

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
        public async Task TestInsertAllEmptyCollectionDoesNothingReturnsEmptyList()
        {
            var res = await Table.InsertAllAsync(null, Array.Empty<IIgniteTuple>());
            CollectionAssert.IsEmpty(res);
        }

        [Test]
        public async Task TestUpsertAllEmptyCollectionDoesNothing()
        {
            await Table.UpsertAllAsync(null, Array.Empty<IIgniteTuple>());
        }

        [Test]
        public async Task TestGetAllReturnsRecordsForExistingKeys()
        {
            var records = Enumerable
                .Range(1, 10)
                .Select(x => GetTuple(x, x.ToString(CultureInfo.InvariantCulture)));

            await Table.UpsertAllAsync(null, records);

            // TODO: Key order should be preserved by the server (IGNITE-16004).
            var res = await Table.GetAllAsync(null, Enumerable.Range(9, 4).Select(x => GetTuple(x)));
            var resArr = res.OrderBy(x => x?[0]).ToArray();

            Assert.AreEqual(2, res.Count);

            Assert.AreEqual(9, resArr[0]![0]);
            Assert.AreEqual("9", resArr[0]![1]);

            Assert.AreEqual(10, resArr[1]![0]);
            Assert.AreEqual("10", resArr[1]![1]);
        }

        [Test]
        public async Task TestGetAllNonExistentKeysReturnsEmptyList()
        {
            var res = await Table.GetAllAsync(null, new[] { GetTuple(-100) });

            Assert.AreEqual(0, res.Count);
        }

        [Test]
        public async Task TestGetAllEmptyKeysReturnsEmptyList()
        {
            var res = await Table.GetAllAsync(null, Array.Empty<IIgniteTuple>());

            Assert.AreEqual(0, res.Count);
        }

        [Test]
        public async Task TestDeleteAllEmptyKeysReturnsEmptyList()
        {
            var skipped = await Table.DeleteAllAsync(null, Array.Empty<IIgniteTuple>());

            Assert.AreEqual(0, skipped.Count);
        }

        [Test]
        public async Task TestDeleteAllNonExistentKeysReturnsAllKeys()
        {
            var skipped = await Table.DeleteAllAsync(null, new[] { GetTuple(1), GetTuple(2) });

            Assert.AreEqual(2, skipped.Count);
        }

        [Test]
        public async Task TestDeleteAllExistingKeysReturnsEmptyListRemovesRecords()
        {
            await Table.UpsertAllAsync(null, new[] { GetTuple(1, "1"), GetTuple(2, "2") });
            var skipped = await Table.DeleteAllAsync(null, new[] { GetTuple(1), GetTuple(2) });

            Assert.AreEqual(0, skipped.Count);
            Assert.IsNull(await Table.GetAsync(null, GetTuple(1)));
            Assert.IsNull(await Table.GetAsync(null, GetTuple(2)));
        }

        [Test]
        public async Task TestDeleteAllRemovesExistingRecordsReturnsNonExistentKeys()
        {
            await Table.UpsertAllAsync(null, new[] { GetTuple(1, "1"), GetTuple(2, "2"), GetTuple(3, "3") });
            var skipped = await Table.DeleteAllAsync(null, new[] { GetTuple(1), GetTuple(2), GetTuple(4) });

            Assert.AreEqual(1, skipped.Count);
            Assert.AreEqual(4, skipped[0][0]);
            Assert.IsNull(await Table.GetAsync(null, GetTuple(1)));
            Assert.IsNull(await Table.GetAsync(null, GetTuple(2)));
            Assert.IsNotNull(await Table.GetAsync(null, GetTuple(3)));
        }

        [Test]
        public async Task TestDeleteAllExactNonExistentKeysReturnsAllKeys()
        {
            var skipped = await Table.DeleteAllExactAsync(null, new[] { GetTuple(1, "1"), GetTuple(2, "2") });

            Assert.AreEqual(2, skipped.Count);
            CollectionAssert.AreEquivalent(new long[] { 1, 2 }, skipped.Select(x => (long)x[0]!).ToArray());
        }

        [Test]
        public async Task TestDeleteAllExactExistingKeysDifferentValuesReturnsAllKeysDoesNotRemove()
        {
            await Table.UpsertAllAsync(null, new[] { GetTuple(1, "1"), GetTuple(2, "2") });
            var skipped = await Table.DeleteAllExactAsync(null, new[] { GetTuple(1, "11"), GetTuple(2, "x") });

            Assert.AreEqual(2, skipped.Count);
        }

        [Test]
        public async Task TestDeleteAllExactExistingKeysReturnsEmptyListRemovesRecords()
        {
            await Table.UpsertAllAsync(null, new[] { GetTuple(1, "1"), GetTuple(2, "2") });
            var skipped = await Table.DeleteAllExactAsync(null, new[] { GetTuple(1, "1"), GetTuple(2, "2") });

            Assert.AreEqual(0, skipped.Count);
            Assert.IsNull(await Table.GetAsync(null, GetTuple(1)));
            Assert.IsNull(await Table.GetAsync(null, GetTuple(2)));
        }

        [Test]
        public async Task TestDeleteAllExactRemovesExistingRecordsReturnsNonExistentKeys()
        {
            await Table.UpsertAllAsync(null, new[] { GetTuple(1, "1"), GetTuple(2, "2"), GetTuple(3, "3") });
            var skipped = await Table.DeleteAllExactAsync(null, new[] { GetTuple(1, "1"), GetTuple(2, "22") });

            Assert.AreEqual(1, skipped.Count);
            Assert.IsNull(await Table.GetAsync(null, GetTuple(1)));
            Assert.IsNotNull(await Table.GetAsync(null, GetTuple(2)));
            Assert.IsNotNull(await Table.GetAsync(null, GetTuple(3)));
        }

        [Test]
        public void TestUpsertAllThrowsArgumentExceptionOnNullCollectionElement()
        {
            var ex = Assert.ThrowsAsync<ArgumentException>(
                async () => await Table.UpsertAllAsync(null, new[] { GetTuple(1, "1"), null! }));

            Assert.AreEqual("Tuple collection can't contain null elements.", ex!.Message);
        }

        [Test]
        public void TestGetAllThrowsArgumentExceptionOnNullCollectionElement()
        {
            var ex = Assert.ThrowsAsync<ArgumentException>(
                async () => await Table.GetAllAsync(null, new[] { GetTuple(1, "1"), null! }));

            Assert.AreEqual("Tuple collection can't contain null elements.", ex!.Message);
        }

        [Test]
        public void TestDeleteAllThrowsArgumentExceptionOnNullCollectionElement()
        {
            var ex = Assert.ThrowsAsync<ArgumentException>(
                async () => await Table.DeleteAllAsync(null, new[] { GetTuple(1, "1"), null! }));

            Assert.AreEqual("Tuple collection can't contain null elements.", ex!.Message);
        }

        [Test]
        public async Task TestLongValueRoundTrip([Values(
            long.MinValue,
            long.MinValue + 1,
            (long)int.MinValue - 1,
            int.MinValue,
            int.MinValue + 1,
            (long)short.MinValue - 1,
            short.MinValue,
            short.MinValue + 1,
            (long)byte.MinValue - 1,
            byte.MinValue,
            byte.MinValue + 1,
            -1,
            0,
            1,
            byte.MaxValue - 1,
            byte.MaxValue,
            (long)byte.MaxValue + 1,
            short.MaxValue - 1,
            short.MaxValue,
            (long)short.MaxValue + 1,
            int.MaxValue - 1,
            int.MaxValue,
            (long)int.MaxValue + 1,
            long.MaxValue - 1,
            long.MaxValue)] long key)
        {
            var val = key.ToString(CultureInfo.InvariantCulture);
            var tuple = new IgniteTuple { [KeyCol] = key, [ValCol] = val };
            await Table.UpsertAsync(null, tuple);

            var keyTuple = new IgniteTuple { [KeyCol] = key };
            var resTuple = (await Table.GetAsync(null, keyTuple))!;

            Assert.IsNotNull(resTuple);
            Assert.AreEqual(2, resTuple.FieldCount);
            Assert.AreEqual(key, resTuple["key"]);
            Assert.AreEqual(val, resTuple["val"]);
        }
    }
}
