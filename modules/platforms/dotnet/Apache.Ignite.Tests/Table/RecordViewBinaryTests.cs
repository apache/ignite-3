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
    using System.Collections;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Threading.Tasks;
    using Ignite.Table;
    using NodaTime;
    using NUnit.Framework;

    /// <summary>
    /// Tests for tuple view.
    /// </summary>
    public class RecordViewBinaryTests : IgniteTestsBase
    {
        [TearDown]
        public async Task CleanTable()
        {
            await TupleView.DeleteAllAsync(null, Enumerable.Range(-1, 50).Select(x => GetTuple(x)));
        }

        [Test]
        public async Task TestUpsertGet()
        {
            await TupleView.UpsertAsync(null, GetTuple(1, "foo"));

            var keyTuple = GetTuple(1);
            var (value, hasValue) = await TupleView.GetAsync(null, keyTuple);

            Assert.IsTrue(hasValue);
            Assert.AreEqual(2, value.FieldCount);
            Assert.AreEqual(1L, value["key"]);
            Assert.AreEqual("foo", value["val"]);
        }

        [Test]
        public async Task TestUpsertOverridesPreviousValue()
        {
            var key = GetTuple(1);

            await TupleView.UpsertAsync(null, GetTuple(1, "foo"));
            Assert.AreEqual("foo", (await TupleView.GetAsync(null, key)).Value[1]);

            await TupleView.UpsertAsync(null, GetTuple(1, "bar"));
            Assert.AreEqual("bar", (await TupleView.GetAsync(null, key)).Value[1]);
        }

        [Test]
        public async Task TestUpsertAllowsCustomTupleImplementation()
        {
            await TupleView.UpsertAsync(null, new CustomTestIgniteTuple());

            var res = await TupleView.GetAsync(null, GetTuple(CustomTestIgniteTuple.Key));

            Assert.IsTrue(res.HasValue);
            Assert.AreEqual(CustomTestIgniteTuple.Value, res.Value[1]);
        }

        [Test]
        public void TestUpsertEmptyTupleThrowsException()
        {
            var ex = Assert.ThrowsAsync<ArgumentException>(async () => await TupleView.UpsertAsync(null, new IgniteTuple()));
            StringAssert.Contains("Matching fields not found.", ex!.Message);
        }

        [Test]
        public async Task TestGetAndUpsertNonExistentRecordReturnsNull()
        {
            Option<IIgniteTuple> res = await TupleView.GetAndUpsertAsync(null, GetTuple(2, "2"));

            Assert.IsFalse(res.HasValue);
            Assert.AreEqual("2", (await TupleView.GetAsync(null, GetTuple(2))).Value[1]);
        }

        [Test]
        public async Task TestGetAndUpsertExistingRecordOverwritesAndReturns()
        {
            await TupleView.UpsertAsync(null, GetTuple(2, "2"));
            Option<IIgniteTuple> res = await TupleView.GetAndUpsertAsync(null, GetTuple(2, "22"));

            Assert.IsTrue(res.HasValue);
            Assert.AreEqual(2, res.Value[0]);
            Assert.AreEqual("2", res.Value[1]);
            Assert.AreEqual("22", (await TupleView.GetAsync(null, GetTuple(2))).Value[1]);
        }

        [Test]
        public async Task TestGetAndDeleteNonExistentRecordReturnsNull()
        {
            Option<IIgniteTuple> res = await TupleView.GetAndDeleteAsync(null, GetTuple(2));

            Assert.IsFalse(res.HasValue);
            Assert.IsFalse((await TupleView.GetAsync(null, GetTuple(2))).HasValue);
        }

        [Test]
        public async Task TestGetAndDeleteExistingRecordRemovesAndReturns()
        {
            await TupleView.UpsertAsync(null, GetTuple(2, "2"));
            var (res, hasRes) = await TupleView.GetAndDeleteAsync(null, GetTuple(2));

            Assert.IsTrue(hasRes);
            Assert.AreEqual(2, res[0]);
            Assert.AreEqual("2", res[1]);
            Assert.IsFalse((await TupleView.GetAsync(null, GetTuple(2))).HasValue);
        }

        [Test]
        public async Task TestInsertNonExistentKeyCreatesRecordReturnsTrue()
        {
            var res = await TupleView.InsertAsync(null, GetTuple(1, "1"));

            Assert.IsTrue(res);
            Assert.IsTrue((await TupleView.GetAsync(null, GetTuple(1))).HasValue);
        }

        [Test]
        public async Task TestInsertExistingKeyDoesNotOverwriteReturnsFalse()
        {
            await TupleView.UpsertAsync(null, GetTuple(1, "1"));
            var res = await TupleView.InsertAsync(null, GetTuple(1, "2"));

            Assert.IsFalse(res);
            Assert.AreEqual("1", (await TupleView.GetAsync(null, GetTuple(1))).Value[1]);
        }

        [Test]
        public async Task TestDeleteNonExistentRecordReturnFalse()
        {
            Assert.IsFalse(await TupleView.DeleteAsync(null, GetTuple(-1)));
        }

        [Test]
        public async Task TestDeleteExistingRecordReturnsTrue()
        {
            await TupleView.UpsertAsync(null, GetTuple(1, "1"));

            Assert.IsTrue(await TupleView.DeleteAsync(null, GetTuple(1)));
            Assert.IsTrue(await TupleView.GetAsync(null, GetTuple(1)) is { HasValue: false });
        }

        [Test]
        public async Task TestDeleteExactNonExistentRecordReturnsFalse()
        {
            Assert.IsFalse(await TupleView.DeleteExactAsync(null, GetTuple(-1)));
        }

        [Test]
        public async Task TestDeleteExactExistingKeyDifferentValueReturnsFalseDoesNotDelete()
        {
            await TupleView.UpsertAsync(null, GetTuple(1, "1"));

            Assert.IsFalse(await TupleView.DeleteExactAsync(null, GetTuple(1)));
            Assert.IsFalse(await TupleView.DeleteExactAsync(null, GetTuple(1, "2")));
            Assert.IsTrue((await TupleView.GetAsync(null, GetTuple(1))).HasValue);
        }

        [Test]
        public async Task TestDeleteExactSameKeyAndValueReturnsTrueDeletesRecord()
        {
            await TupleView.UpsertAsync(null, GetTuple(1, "1"));

            Assert.IsTrue(await TupleView.DeleteExactAsync(null, GetTuple(1, "1")));
            Assert.IsFalse((await TupleView.GetAsync(null, GetTuple(1))).HasValue);
        }

        [Test]
        public async Task TestReplaceNonExistentRecordReturnsFalseDoesNotCreateRecord()
        {
            bool res = await TupleView.ReplaceAsync(null, GetTuple(1, "1"));

            Assert.IsFalse(res);
            Assert.IsFalse((await TupleView.GetAsync(null, GetTuple(1))).HasValue);
        }

        [Test]
        public async Task TestReplaceExistingRecordReturnsTrueOverwrites()
        {
            await TupleView.UpsertAsync(null, GetTuple(1, "1"));
            bool res = await TupleView.ReplaceAsync(null, GetTuple(1, "2"));

            Assert.IsTrue(res);
            Assert.AreEqual("2", (await TupleView.GetAsync(null, GetTuple(1))).Value[1]);
        }

        [Test]
        public async Task TestGetAndReplaceNonExistentRecordReturnsNullDoesNotCreateRecord()
        {
            Option<IIgniteTuple> res = await TupleView.GetAndReplaceAsync(null, GetTuple(1, "1"));

            Assert.IsFalse(res.HasValue);
            Assert.IsFalse((await TupleView.GetAsync(null, GetTuple(1))).HasValue);
        }

        [Test]
        public async Task TestGetAndReplaceExistingRecordReturnsOldOverwrites()
        {
            await TupleView.UpsertAsync(null, GetTuple(1, "1"));
            Option<IIgniteTuple> res = await TupleView.GetAndReplaceAsync(null, GetTuple(1, "2"));

            Assert.IsTrue(res.HasValue);
            Assert.AreEqual("1", res.Value[1]);
            Assert.AreEqual("2", (await TupleView.GetAsync(null, GetTuple(1))).Value[1]);
        }

        [Test]
        public async Task TestReplaceExactNonExistentRecordReturnsFalseDoesNotCreateRecord()
        {
            bool res = await TupleView.ReplaceAsync(null, GetTuple(1, "1"), GetTuple(1, "2"));

            Assert.IsFalse(res);
            Assert.IsFalse((await TupleView.GetAsync(null, GetTuple(1))).HasValue);
        }

        [Test]
        public async Task TestReplaceExactExistingRecordWithDifferentValueReturnsFalseDoesNotReplace()
        {
            await TupleView.UpsertAsync(null, GetTuple(1, "1"));
            bool res = await TupleView.ReplaceAsync(null, GetTuple(1, "11"), GetTuple(1, "22"));

            Assert.IsFalse(res);
            Assert.AreEqual("1", (await TupleView.GetAsync(null, GetTuple(1))).Value[1]);
        }

        [Test]
        public async Task TestReplaceExactExistingRecordWithSameValueReturnsTrueReplacesOld()
        {
            await TupleView.UpsertAsync(null, GetTuple(1, "1"));
            bool res = await TupleView.ReplaceAsync(null, GetTuple(1, "1"), GetTuple(1, "22"));

            Assert.IsTrue(res);
            Assert.AreEqual("22", (await TupleView.GetAsync(null, GetTuple(1))).Value[1]);
        }

        [Test]
        public async Task TestUpsertAllCreatesRecords()
        {
            var ids = Enumerable.Range(1, 10).ToList();
            var records = ids.Select(x => GetTuple(x, x.ToString(CultureInfo.InvariantCulture)));

            await TupleView.UpsertAllAsync(null, records);

            foreach (var id in ids)
            {
                var res = await TupleView.GetAsync(null, GetTuple(id));
                Assert.AreEqual(id.ToString(CultureInfo.InvariantCulture), res.Value[1]);
            }
        }

        [Test]
        public async Task TestUpsertAllOverwritesExistingData()
        {
            await TupleView.InsertAsync(null, GetTuple(2, "x"));
            await TupleView.InsertAsync(null, GetTuple(4, "y"));

            var ids = Enumerable.Range(1, 10).ToList();
            var records = ids.Select(x => GetTuple(x, x.ToString(CultureInfo.InvariantCulture)));

            await TupleView.UpsertAllAsync(null, records);

            foreach (var id in ids)
            {
                var res = await TupleView.GetAsync(null, GetTuple(id));
                Assert.AreEqual(id.ToString(CultureInfo.InvariantCulture), res.Value[1]);
            }
        }

        [Test]
        public async Task TestInsertAllCreatesRecords()
        {
            var ids = Enumerable.Range(1, 10).ToList();
            var records = ids.Select(x => GetTuple(x, x.ToString(CultureInfo.InvariantCulture)));

            IList<IIgniteTuple> skipped = await TupleView.InsertAllAsync(null, records);

            CollectionAssert.IsEmpty(skipped);

            foreach (var id in ids)
            {
                var res = await TupleView.GetAsync(null, GetTuple(id));
                Assert.AreEqual(id.ToString(CultureInfo.InvariantCulture), res.Value[1]);
            }
        }

        [Test]
        public async Task TestInsertAllDoesNotOverwriteExistingDataReturnsSkippedTuples()
        {
            var existing = new[] { GetTuple(2, "x"), GetTuple(4, "y") }.ToDictionary(x => x[0]!);
            await TupleView.InsertAllAsync(null, existing.Values);

            var ids = Enumerable.Range(1, 10).ToList();
            var records = ids.Select(x => GetTuple(x, x.ToString(CultureInfo.InvariantCulture)));

            IList<IIgniteTuple> skipped = await TupleView.InsertAllAsync(null, records);
            var skippedArr = skipped.OrderBy(x => x[0]).ToArray();

            Assert.AreEqual(2, skippedArr.Length);
            Assert.AreEqual(2, skippedArr[0][0]);
            Assert.AreEqual("2", skippedArr[0][1]);

            Assert.AreEqual(4, skippedArr[1][0]);
            Assert.AreEqual("4", skippedArr[1][1]);

            foreach (var id in ids)
            {
                var res = await TupleView.GetAsync(null, GetTuple(id));

                if (existing.TryGetValue(res.Value[0]!, out var old))
                {
                    Assert.AreEqual(old[1], res.Value[1]);
                }
                else
                {
                    Assert.AreEqual(id.ToString(CultureInfo.InvariantCulture), res.Value[1]);
                }
            }
        }

        [Test]
        public async Task TestInsertAllEmptyCollectionDoesNothingReturnsEmptyList()
        {
            var res = await TupleView.InsertAllAsync(null, Array.Empty<IIgniteTuple>());
            CollectionAssert.IsEmpty(res);
        }

        [Test]
        public async Task TestUpsertAllEmptyCollectionDoesNothing()
        {
            await TupleView.UpsertAllAsync(null, Array.Empty<IIgniteTuple>());
        }

        [Test]
        public async Task TestGetAllReturnsRecordsForExistingKeys()
        {
            var records = Enumerable
                .Range(1, 10)
                .Select(x => GetTuple(x, x.ToString(CultureInfo.InvariantCulture)));

            await TupleView.UpsertAllAsync(null, records);

            var res = await TupleView.GetAllAsync(null, Enumerable.Range(9, 4).Select(x => GetTuple(x)));
            var resArr = res.ToArray();

            Assert.AreEqual(4, res.Count);

            Assert.AreEqual(9, resArr[0].Value[0]);
            Assert.AreEqual("9", resArr[0].Value[1]);

            Assert.AreEqual(10, resArr[1].Value[0]);
            Assert.AreEqual("10", resArr[1].Value[1]);

            Assert.IsFalse(resArr[2].HasValue);
            Assert.IsFalse(resArr[3].HasValue);
        }

        [Test]
        public async Task TestGetAllNonExistentKeysReturnsListWithNoValue()
        {
            var res = await TupleView.GetAllAsync(null, new[] { GetTuple(-100) });

            Assert.AreEqual(1, res.Count);
            Assert.IsFalse(res[0].HasValue);
        }

        [Test]
        public async Task TestGetAllEmptyKeysReturnsEmptyList()
        {
            var res = await TupleView.GetAllAsync(null, Array.Empty<IIgniteTuple>());

            Assert.AreEqual(0, res.Count);
        }

        [Test]
        public async Task TestDeleteAllEmptyKeysReturnsEmptyList()
        {
            var skipped = await TupleView.DeleteAllAsync(null, Array.Empty<IIgniteTuple>());

            Assert.AreEqual(0, skipped.Count);
        }

        [Test]
        public async Task TestDeleteAllNonExistentKeysReturnsAllKeys()
        {
            var skipped = await TupleView.DeleteAllAsync(null, new[] { GetTuple(1), GetTuple(2) });

            Assert.AreEqual(2, skipped.Count);
        }

        [Test]
        public async Task TestDeleteAllExistingKeysReturnsEmptyListRemovesRecords()
        {
            await TupleView.UpsertAllAsync(null, new[] { GetTuple(1, "1"), GetTuple(2, "2") });
            var skipped = await TupleView.DeleteAllAsync(null, new[] { GetTuple(1), GetTuple(2) });

            Assert.AreEqual(0, skipped.Count);
            Assert.IsFalse((await TupleView.GetAsync(null, GetTuple(1))).HasValue);
            Assert.IsFalse((await TupleView.GetAsync(null, GetTuple(2))).HasValue);
        }

        [Test]
        public async Task TestDeleteAllRemovesExistingRecordsReturnsNonExistentKeys()
        {
            await TupleView.UpsertAllAsync(null, new[] { GetTuple(1, "1"), GetTuple(2, "2"), GetTuple(3, "3") });
            var skipped = await TupleView.DeleteAllAsync(null, new[] { GetTuple(1), GetTuple(2), GetTuple(4) });

            Assert.AreEqual(1, skipped.Count);
            Assert.AreEqual(4, skipped[0][0]);
            Assert.IsFalse((await TupleView.GetAsync(null, GetTuple(1))).HasValue);
            Assert.IsFalse((await TupleView.GetAsync(null, GetTuple(2))).HasValue);
            Assert.IsTrue((await TupleView.GetAsync(null, GetTuple(3))).HasValue);
        }

        [Test]
        public async Task TestDeleteAllExactNonExistentKeysReturnsAllKeys()
        {
            var skipped = await TupleView.DeleteAllExactAsync(null, new[] { GetTuple(1, "1"), GetTuple(2, "2") });

            Assert.AreEqual(2, skipped.Count);
            CollectionAssert.AreEquivalent(new long[] { 1, 2 }, skipped.Select(x => (long)x[0]!).ToArray());
        }

        [Test]
        public async Task TestDeleteAllExactExistingKeysDifferentValuesReturnsAllKeysDoesNotRemove()
        {
            await TupleView.UpsertAllAsync(null, new[] { GetTuple(1, "1"), GetTuple(2, "2") });
            var skipped = await TupleView.DeleteAllExactAsync(null, new[] { GetTuple(1, "11"), GetTuple(2, "x") });

            Assert.AreEqual(2, skipped.Count);
        }

        [Test]
        public async Task TestDeleteAllExactExistingKeysReturnsEmptyListRemovesRecords()
        {
            await TupleView.UpsertAllAsync(null, new[] { GetTuple(1, "1"), GetTuple(2, "2") });
            var skipped = await TupleView.DeleteAllExactAsync(null, new[] { GetTuple(1, "1"), GetTuple(2, "2") });

            Assert.AreEqual(0, skipped.Count);
            Assert.IsFalse((await TupleView.GetAsync(null, GetTuple(1))).HasValue);
            Assert.IsFalse((await TupleView.GetAsync(null, GetTuple(2))).HasValue);
        }

        [Test]
        public async Task TestDeleteAllExactRemovesExistingRecordsReturnsNonExistentKeys()
        {
            await TupleView.UpsertAllAsync(null, new[] { GetTuple(1, "1"), GetTuple(2, "2"), GetTuple(3, "3") });
            var skipped = await TupleView.DeleteAllExactAsync(null, new[] { GetTuple(1, "1"), GetTuple(2, "22") });

            Assert.AreEqual(1, skipped.Count);
            Assert.IsFalse((await TupleView.GetAsync(null, GetTuple(1))).HasValue);
            Assert.IsTrue((await TupleView.GetAsync(null, GetTuple(2))).HasValue);
            Assert.IsTrue((await TupleView.GetAsync(null, GetTuple(3))).HasValue);
        }

        [Test]
        public void TestUpsertAllThrowsArgumentExceptionOnNullCollectionElement()
        {
            var ex = Assert.ThrowsAsync<ArgumentException>(
                async () => await TupleView.UpsertAllAsync(null, new[] { GetTuple(1, "1"), null! }));

            Assert.AreEqual("Record collection can't contain null elements.", ex!.Message);
        }

        [Test]
        public void TestGetAllThrowsArgumentExceptionOnNullCollectionElement()
        {
            var ex = Assert.ThrowsAsync<ArgumentException>(
                async () => await TupleView.GetAllAsync(null, new[] { GetTuple(1), null! }));

            Assert.AreEqual("Record collection can't contain null elements.", ex!.Message);
        }

        [Test]
        public void TestDeleteAllThrowsArgumentExceptionOnNullCollectionElement()
        {
            var ex = Assert.ThrowsAsync<ArgumentException>(
                async () => await TupleView.DeleteAllAsync(null, new[] { GetTuple(1), null! }));

            Assert.AreEqual("Record collection can't contain null elements.", ex!.Message);
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
            await TupleView.UpsertAsync(null, tuple);

            var keyTuple = new IgniteTuple { [KeyCol] = key };
            var (resTuple, resTupleHasValue) = await TupleView.GetAsync(null, keyTuple);

            Assert.IsTrue(resTupleHasValue);
            Assert.AreEqual(2, resTuple.FieldCount);
            Assert.AreEqual(key, resTuple["key"]);
            Assert.AreEqual(val, resTuple["val"]);
        }

        [Test]
        public async Task TestAllColumns()
        {
            var table = await Client.Tables.GetTableAsync(TableAllColumnsName);
            var tupleView = table!.RecordBinaryView;

            var keyTuple = new IgniteTuple { ["Key"] = 123L };
            var dt = LocalDateTime.FromDateTime(DateTime.UtcNow);
            var tuple = new IgniteTuple
            {
                ["Key"] = 123L,
                ["Str"] = "str",
                ["Int8"] = (sbyte)8,
                ["Int16"] = (short)16,
                ["Int32"] = 32,
                ["Int64"] = 64L,
                ["Float"] = 32.32f,
                ["Double"] = 64.64,
                ["Uuid"] = Guid.NewGuid(),
                ["Date"] = dt.Date,
                ["BitMask"] = new BitArray(new byte[] { 1 }),
                ["Time"] = dt.TimeOfDay,
                ["DateTime"] = dt,
                ["Timestamp"] = Instant.FromDateTimeUtc(DateTime.UtcNow),
                ["Blob"] = new byte[] { 1, 2, 3 },
                ["Decimal"] = 123.456m,
                ["Boolean"] = true
            };

            await tupleView.UpsertAsync(null, tuple);

            var res = (await tupleView.GetAsync(null, keyTuple)).Value;

            Assert.AreEqual(tuple["Blob"], res["Blob"]);
            Assert.AreEqual(tuple["Date"], res["Date"]);
            Assert.AreEqual(tuple["Decimal"], res["Decimal"]);
            Assert.AreEqual(tuple["Double"], res["Double"]);
            Assert.AreEqual(tuple["Float"], res["Float"]);
            Assert.AreEqual(tuple["Int8"], res["Int8"]);
            Assert.AreEqual(tuple["Int16"], res["Int16"]);
            Assert.AreEqual(tuple["Int32"], res["Int32"]);
            Assert.AreEqual(tuple["Int64"], res["Int64"]);
            Assert.AreEqual(tuple["Str"], res["Str"]);
            Assert.AreEqual(tuple["Uuid"], res["Uuid"]);
            Assert.AreEqual(tuple["BitMask"], res["BitMask"]);
            Assert.AreEqual(tuple["Timestamp"], res["Timestamp"]);
            Assert.AreEqual(tuple["Time"], res["Time"]);
            Assert.AreEqual(tuple["DateTime"], res["DateTime"]);
            Assert.AreEqual(tuple["Boolean"], res["Boolean"]);
        }

        [Test]
        public async Task TestContainsKey()
        {
            var keyTuple = GetTuple(1);
            var tuple = GetTuple(1, "foo");

            await TupleView.UpsertAsync(null, tuple);

            Assert.IsTrue(await TupleView.ContainsKeyAsync(null, keyTuple));
            Assert.IsFalse(await TupleView.ContainsKeyAsync(null, GetTuple(-128)));
        }

        [Test]
        public void TestToString()
        {
            StringAssert.StartsWith("RecordView`1[IIgniteTuple] { Table = Table { Name = TBL1, Id =", TupleView.ToString());
        }
    }
}
