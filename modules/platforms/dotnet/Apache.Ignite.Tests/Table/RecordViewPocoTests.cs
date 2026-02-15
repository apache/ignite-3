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
    using Common;
    using Common.Table;
    using NodaTime;
    using NUnit.Framework;
    using static Common.Table.TestTables;

    /// <summary>
    /// Tests for POCO view.
    /// </summary>
    [TestFixture("reflective")]
    [TestFixture("mapper")]
    public class RecordViewPocoTests(string mode) : IgniteTestsBase(useMapper: mode == "mapper")
    {
        [TearDown]
        public async Task CleanTable()
        {
            await TupleView.DeleteAllAsync(null, Enumerable.Range(-1, 12).Select(x => GetTuple(x)));
        }

        [Test]
        public async Task TestUpsertGet()
        {
            await PocoView.UpsertAsync(null, GetPoco(1, "foo"));

            var keyTuple = GetPoco(1);
            var (resTuple, hasValue) = await PocoView.GetAsync(null, keyTuple);

            Assert.IsTrue(hasValue);
            Assert.AreEqual(1L, resTuple.Key);
            Assert.AreEqual("foo", resTuple.Val);

            Assert.IsNull(resTuple.UnmappedStr);
            Assert.AreEqual(default(Guid), resTuple.UnmappedId);
        }

        [Test]
        public async Task TestUpsertGetValueType()
        {
            var pocoView = Table.GetRecordView<PocoStruct>();

            await pocoView.UpsertAsync(null, new PocoStruct(1, "foo"));

            var keyTuple = new PocoStruct(1, null);
            var (resTuple, hasValue) = await pocoView.GetAsync(null, keyTuple);

            Assert.IsTrue(hasValue);
            Assert.AreEqual(1L, resTuple.Key);
            Assert.AreEqual("foo", resTuple.Val);
            Assert.IsNull(resTuple.UnmappedStr);
        }

        [Test]
        public async Task TestGetMissingRowValueType()
        {
            var pocoView = Table.GetRecordView<PocoStruct>();

            var keyTuple = new PocoStruct(1, null);
            var (resTuple, hasValue) = await pocoView.GetAsync(null, keyTuple);

            Assert.IsFalse(hasValue);
            Assert.AreEqual(0L, resTuple.Key);
            Assert.IsNull(resTuple.Val);
            Assert.IsNull(resTuple.UnmappedStr);
        }

        [Test]
        public async Task TestUpsertOverridesPreviousValue()
        {
            var key = GetPoco(1);

            await PocoView.UpsertAsync(null, GetPoco(1, "foo"));
            Assert.AreEqual("foo", (await PocoView.GetAsync(null, key)).Value.Val);

            await PocoView.UpsertAsync(null, GetPoco(1, "bar"));
            Assert.AreEqual("bar", (await PocoView.GetAsync(null, key)).Value.Val);
        }

        [Test]
        public void TestUpsertEmptyPocoThrowsException()
        {
            var pocoView = Table.GetRecordView<object>();

            var ex = Assert.ThrowsAsync<ArgumentException>(async () => await pocoView.UpsertAsync(null, new object()));

            Assert.AreEqual("Can't map 'System.Object' to columns 'Int64 KEY, String VAL'. Matching fields not found.", ex!.Message);
        }

        [Test]
        public async Task TestGetAndUpsertNonExistentRecordReturnsNull()
        {
            Option<Poco> res = await PocoView.GetAndUpsertAsync(null, GetPoco(2, "2"));

            Assert.IsFalse(res.HasValue);
            Assert.AreEqual("2", (await PocoView.GetAsync(null, GetPoco(2))).Value.Val);
        }

        [Test]
        public async Task TestGetAndUpsertExistingRecordOverwritesAndReturns()
        {
            await PocoView.UpsertAsync(null, GetPoco(2, "2"));
            var (res, hasRes) = await PocoView.GetAndUpsertAsync(null, GetPoco(2, "22"));

            Assert.IsTrue(hasRes);
            Assert.AreEqual(2, res.Key);
            Assert.AreEqual("2", res.Val);
            Assert.AreEqual("22", (await PocoView.GetAsync(null, GetPoco(2))).Value.Val);
        }

        [Test]
        public async Task TestGetAndDeleteNonExistentRecordReturnsNull()
        {
            Option<Poco> res = await PocoView.GetAndDeleteAsync(null, GetPoco(2, "2"));

            Assert.IsFalse(res.HasValue);
            Assert.IsFalse((await PocoView.GetAsync(null, GetPoco(2))).HasValue);
        }

        [Test]
        public async Task TestGetAndDeleteExistingRecordRemovesAndReturns()
        {
            await PocoView.UpsertAsync(null, GetPoco(2, "2"));
            var (res, hasRes) = await PocoView.GetAndDeleteAsync(null, GetPoco(2));

            Assert.IsTrue(hasRes);
            Assert.AreEqual(2, res.Key);
            Assert.AreEqual("2", res.Val);
            Assert.IsFalse((await PocoView.GetAsync(null, GetPoco(2))).HasValue);
        }

        [Test]
        public async Task TestInsertNonExistentKeyCreatesRecordReturnsTrue()
        {
            var res = await PocoView.InsertAsync(null, GetPoco(1, "1"));

            Assert.IsTrue(res);
            Assert.IsTrue((await PocoView.GetAsync(null, GetPoco(1))).HasValue);
        }

        [Test]
        public async Task TestInsertExistingKeyDoesNotOverwriteReturnsFalse()
        {
            await PocoView.UpsertAsync(null, GetPoco(1, "1"));
            var res = await PocoView.InsertAsync(null, GetPoco(1, "2"));

            Assert.IsFalse(res);
            Assert.AreEqual("1", (await PocoView.GetAsync(null, GetPoco(1))).Value.Val);
        }

        [Test]
        public async Task TestDeleteNonExistentRecordReturnFalse()
        {
            Assert.IsFalse(await PocoView.DeleteAsync(null, GetPoco(-1)));
        }

        [Test]
        public async Task TestDeleteExistingRecordReturnsTrue()
        {
            await PocoView.UpsertAsync(null, GetPoco(1, "1"));

            Assert.IsTrue(await PocoView.DeleteAsync(null, GetPoco(1)));
            Assert.IsFalse((await PocoView.GetAsync(null, GetPoco(1))).HasValue);
        }

        [Test]
        public async Task TestDeleteExactNonExistentRecordReturnsFalse()
        {
            Assert.IsFalse(await PocoView.DeleteExactAsync(null, GetPoco(-1)));
        }

        [Test]
        public async Task TestDeleteExactExistingKeyDifferentValueReturnsFalseDoesNotDelete()
        {
            await PocoView.UpsertAsync(null, GetPoco(1, "1"));

            Assert.IsFalse(await PocoView.DeleteExactAsync(null, GetPoco(1)));
            Assert.IsFalse(await PocoView.DeleteExactAsync(null, GetPoco(1, "2")));
            Assert.IsTrue(await PocoView.GetAsync(null, GetPoco(1)) is { HasValue: true });
        }

        [Test]
        public async Task TestDeleteExactSameKeyAndValueReturnsTrueDeletesRecord()
        {
            await PocoView.UpsertAsync(null, GetPoco(1, "1"));

            Assert.IsTrue(await PocoView.DeleteExactAsync(null, GetPoco(1, "1")));
            Assert.IsFalse((await PocoView.GetAsync(null, GetPoco(1))).HasValue);
        }

        [Test]
        public async Task TestReplaceNonExistentRecordReturnsFalseDoesNotCreateRecord()
        {
            bool res = await PocoView.ReplaceAsync(null, GetPoco(1, "1"));
            Option<Poco> res2 = await PocoView.GetAsync(null, GetPoco(1));

            Assert.IsFalse(res);
            Assert.IsFalse(res2.HasValue);
        }

        [Test]
        public async Task TestReplaceExistingRecordReturnsTrueOverwrites()
        {
            await PocoView.UpsertAsync(null, GetPoco(1, "1"));
            bool res = await PocoView.ReplaceAsync(null, GetPoco(1, "2"));

            Assert.IsTrue(res);
            Assert.AreEqual("2", (await PocoView.GetAsync(null, GetPoco(1))).Value.Val);
        }

        [Test]
        public async Task TestGetAndReplaceNonExistentRecordReturnsNullDoesNotCreateRecord()
        {
            Option<Poco> res = await PocoView.GetAndReplaceAsync(null, GetPoco(1, "1"));
            Option<Poco> res2 = await PocoView.GetAsync(null, GetPoco(1));

            Assert.IsFalse(res.HasValue);
            Assert.IsFalse(res2.HasValue);
        }

        [Test]
        public async Task TestGetAndReplaceExistingRecordReturnsOldOverwrites()
        {
            await PocoView.UpsertAsync(null, GetPoco(1, "1"));
            var (res, hasRes) = await PocoView.GetAndReplaceAsync(null, GetPoco(1, "2"));

            Assert.IsTrue(hasRes);
            Assert.AreEqual("1", res.Val);
            Assert.AreEqual("2", (await PocoView.GetAsync(null, GetPoco(1))).Value.Val);
        }

        [Test]
        public async Task TestReplaceExactNonExistentRecordReturnsFalseDoesNotCreateRecord()
        {
            bool res = await PocoView.ReplaceAsync(null, GetPoco(1, "1"), GetPoco(1, "2"));
            Option<Poco> res2 = await PocoView.GetAsync(null, GetPoco(1));

            Assert.IsFalse(res);
            Assert.IsFalse(res2.HasValue);
        }

        [Test]
        public async Task TestReplaceExactExistingRecordWithDifferentValueReturnsFalseDoesNotReplace()
        {
            await PocoView.UpsertAsync(null, GetPoco(1, "1"));
            bool res = await PocoView.ReplaceAsync(null, GetPoco(1, "11"), GetPoco(1, "22"));

            Assert.IsFalse(res);
            Assert.AreEqual("1", (await PocoView.GetAsync(null, GetPoco(1))).Value.Val);
        }

        [Test]
        public async Task TestReplaceExactExistingRecordWithSameValueReturnsTrueReplacesOld()
        {
            await PocoView.UpsertAsync(null, GetPoco(1, "1"));
            bool res = await PocoView.ReplaceAsync(null, GetPoco(1, "1"), GetPoco(1, "22"));

            Assert.IsTrue(res);
            Assert.AreEqual("22", (await PocoView.GetAsync(null, GetPoco(1))).Value.Val);
        }

        [Test]
        public async Task TestUpsertAllCreatesRecords()
        {
            var ids = Enumerable.Range(1, 10).ToList();
            var records = ids.Select(x => GetPoco(x, x.ToString(CultureInfo.InvariantCulture)));

            await PocoView.UpsertAllAsync(null, records);

            foreach (var id in ids)
            {
                var res = await PocoView.GetAsync(null, GetPoco(id));
                Assert.AreEqual(id.ToString(CultureInfo.InvariantCulture), res.Value.Val);
            }
        }

        [Test]
        public async Task TestUpsertAllOverwritesExistingData()
        {
            await PocoView.InsertAsync(null, GetPoco(2, "x"));
            await PocoView.InsertAsync(null, GetPoco(4, "y"));

            var ids = Enumerable.Range(1, 10).ToList();
            var records = ids.Select(x => GetPoco(x, x.ToString(CultureInfo.InvariantCulture)));

            await PocoView.UpsertAllAsync(null, records);

            foreach (var id in ids)
            {
                var res = await PocoView.GetAsync(null, GetPoco(id));
                Assert.AreEqual(id.ToString(CultureInfo.InvariantCulture), res.Value.Val);
            }
        }

        [Test]
        public async Task TestInsertAllCreatesRecords()
        {
            var ids = Enumerable.Range(1, 10).ToList();
            var records = ids.Select(x => GetPoco(x, x.ToString(CultureInfo.InvariantCulture)));

            IList<Poco> skipped = await PocoView.InsertAllAsync(null, records);

            CollectionAssert.IsEmpty(skipped);

            foreach (var id in ids)
            {
                var res = await PocoView.GetAsync(null, GetPoco(id));
                Assert.AreEqual(id.ToString(CultureInfo.InvariantCulture), res.Value.Val);
            }
        }

        [Test]
        public async Task TestInsertAllDoesNotOverwriteExistingDataReturnsSkippedTuples()
        {
            var existing = new[] { GetPoco(2, "x"), GetPoco(4, "y") }.ToDictionary(x => x.Key);
            await PocoView.InsertAllAsync(null, existing.Values);

            var ids = Enumerable.Range(1, 10).ToList();
            var records = ids.Select(x => GetPoco(x, x.ToString(CultureInfo.InvariantCulture)));

            IList<Poco> skipped = await PocoView.InsertAllAsync(null, records);
            var skippedArr = skipped.OrderBy(x => x.Key).ToArray();

            Assert.AreEqual(2, skippedArr.Length);
            Assert.AreEqual(2, skippedArr[0].Key);
            Assert.AreEqual("2", skippedArr[0].Val);

            Assert.AreEqual(4, skippedArr[1].Key);
            Assert.AreEqual("4", skippedArr[1].Val);

            foreach (var id in ids)
            {
                var res = await PocoView.GetAsync(null, GetPoco(id));

                if (existing.TryGetValue(res.Value.Key, out var old))
                {
                    Assert.AreEqual(old.Val, res.Value.Val);
                }
                else
                {
                    Assert.AreEqual(id.ToString(CultureInfo.InvariantCulture), res.Value.Val);
                }
            }
        }

        [Test]
        public async Task TestInsertAllEmptyCollectionDoesNothingReturnsEmptyList()
        {
            var res = await PocoView.InsertAllAsync(null, Array.Empty<Poco>());
            CollectionAssert.IsEmpty(res);
        }

        [Test]
        public async Task TestUpsertAllEmptyCollectionDoesNothing()
        {
            await PocoView.UpsertAllAsync(null, Array.Empty<Poco>());
        }

        [Test]
        public async Task TestGetAllReturnsRecordsForExistingKeys()
        {
            var records = Enumerable
                .Range(1, 10)
                .Select(x => GetPoco(x, x.ToString(CultureInfo.InvariantCulture)));

            await PocoView.UpsertAllAsync(null, records);

            var res = await PocoView.GetAllAsync(null, Enumerable.Range(9, 4).Select(x => GetPoco(x)));
            var resArr = res.ToArray();

            Assert.AreEqual(4, res.Count);

            Assert.AreEqual(9, resArr[0].Value.Key);
            Assert.AreEqual("9", resArr[0].Value.Val);

            Assert.AreEqual(10, resArr[1].Value.Key);
            Assert.AreEqual("10", resArr[1].Value.Val);

            Assert.IsFalse(resArr[2].HasValue);
            Assert.IsFalse(resArr[3].HasValue);
        }

        [Test]
        public async Task TestGetAllNonExistentKeysReturnsListWithNoValue()
        {
            var res = await PocoView.GetAllAsync(null, new[] { GetPoco(-100) });

            Assert.AreEqual(1, res.Count);
            Assert.IsFalse(res[0].HasValue);
        }

        [Test]
        public async Task TestGetAllEmptyKeysReturnsEmptyList()
        {
            var res = await PocoView.GetAllAsync(null, Array.Empty<Poco>());

            Assert.AreEqual(0, res.Count);
        }

        [Test]
        public async Task TestDeleteAllEmptyKeysReturnsEmptyList()
        {
            var skipped = await PocoView.DeleteAllAsync(null, Array.Empty<Poco>());

            Assert.AreEqual(0, skipped.Count);
        }

        [Test]
        public async Task TestDeleteAllNonExistentKeysReturnsAllKeys()
        {
            var skipped = await PocoView.DeleteAllAsync(null, new[] { GetPoco(1), GetPoco(2) });

            Assert.AreEqual(2, skipped.Count);
        }

        [Test]
        public async Task TestDeleteAllExistingKeysReturnsEmptyListRemovesRecords()
        {
            await PocoView.UpsertAllAsync(null, new[] { GetPoco(1, "1"), GetPoco(2, "2") });
            var skipped = await PocoView.DeleteAllAsync(null, new[] { GetPoco(1), GetPoco(2) });

            Assert.AreEqual(0, skipped.Count);
            Assert.IsFalse((await PocoView.GetAsync(null, GetPoco(1))).HasValue);
            Assert.IsFalse((await PocoView.GetAsync(null, GetPoco(2))).HasValue);
        }

        [Test]
        public async Task TestDeleteAllRemovesExistingRecordsReturnsNonExistentKeys()
        {
            await PocoView.UpsertAllAsync(null, new[] { GetPoco(1, "1"), GetPoco(2, "2"), GetPoco(3, "3") });
            var skipped = await PocoView.DeleteAllAsync(null, new[] { GetPoco(1), GetPoco(2), GetPoco(4) });

            Assert.AreEqual(1, skipped.Count);
            Assert.AreEqual(4, skipped[0].Key);
            Assert.IsFalse((await PocoView.GetAsync(null, GetPoco(1))).HasValue);
            Assert.IsFalse((await PocoView.GetAsync(null, GetPoco(2))).HasValue);
            Assert.IsTrue((await PocoView.GetAsync(null, GetPoco(3))).HasValue);
        }

        [Test]
        public async Task TestDeleteAllExactNonExistentKeysReturnsAllKeys()
        {
            var skipped = await PocoView.DeleteAllExactAsync(null, new[] { GetPoco(1, "1"), GetPoco(2, "2") });

            Assert.AreEqual(2, skipped.Count);
            CollectionAssert.AreEquivalent(new long[] { 1, 2 }, skipped.Select(x => x.Key).ToArray());
        }

        [Test]
        public async Task TestDeleteAllExactExistingKeysDifferentValuesReturnsAllKeysDoesNotRemove()
        {
            await PocoView.UpsertAllAsync(null, new[] { GetPoco(1, "1"), GetPoco(2, "2") });
            var skipped = await PocoView.DeleteAllExactAsync(null, new[] { GetPoco(1, "11"), GetPoco(2, "x") });

            Assert.AreEqual(2, skipped.Count);
        }

        [Test]
        public async Task TestDeleteAllExactExistingKeysReturnsEmptyListRemovesRecords()
        {
            await PocoView.UpsertAllAsync(null, new[] { GetPoco(1, "1"), GetPoco(2, "2") });
            var skipped = await PocoView.DeleteAllExactAsync(null, new[] { GetPoco(1, "1"), GetPoco(2, "2") });

            Assert.AreEqual(0, skipped.Count);
            Assert.IsFalse((await PocoView.GetAsync(null, GetPoco(1))).HasValue);
            Assert.IsFalse((await PocoView.GetAsync(null, GetPoco(2))).HasValue);
        }

        [Test]
        public async Task TestDeleteAllExactRemovesExistingRecordsReturnsNonExistentKeys()
        {
            await PocoView.UpsertAllAsync(null, new[] { GetPoco(1, "1"), GetPoco(2, "2"), GetPoco(3, "3") });
            var skipped = await PocoView.DeleteAllExactAsync(null, new[] { GetPoco(1, "1"), GetPoco(2, "22") });

            Assert.AreEqual(1, skipped.Count);
            Assert.IsFalse((await PocoView.GetAsync(null, GetPoco(1))).HasValue);
            Assert.IsTrue((await PocoView.GetAsync(null, GetPoco(2))).HasValue);
            Assert.IsTrue((await PocoView.GetAsync(null, GetPoco(3))).HasValue);
        }

        [Test]
        public void TestUpsertAllThrowsArgumentExceptionOnNullCollectionElement()
        {
            var ex = Assert.ThrowsAsync<ArgumentException>(
                async () => await PocoView.UpsertAllAsync(null, new[] { GetPoco(1, "1"), null! }));

            Assert.AreEqual("Record collection can't contain null elements.", ex!.Message);
        }

        [Test]
        public void TestGetAllThrowsArgumentExceptionOnNullCollectionElement()
        {
            var ex = Assert.ThrowsAsync<ArgumentException>(
                async () => await PocoView.GetAllAsync(null, new[] { GetPoco(1, "1"), null! }));

            Assert.AreEqual("Record collection can't contain null elements.", ex!.Message);
        }

        [Test]
        public void TestDeleteAllThrowsArgumentExceptionOnNullCollectionElement()
        {
            var ex = Assert.ThrowsAsync<ArgumentException>(
                async () => await PocoView.DeleteAllAsync(null, new[] { GetPoco(1, "1"), null! }));

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
            var poco = new Poco { Key = key, Val = val};
            await PocoView.UpsertAsync(null, poco);

            var keyTuple = new Poco { Key = key };
            var (resTuple, resTupleHasValue) = await PocoView.GetAsync(null, keyTuple);

            Assert.IsTrue(resTupleHasValue);
            Assert.AreEqual(key, resTuple.Key);
            Assert.AreEqual(val, resTuple.Val);
        }

        [Test]
        public async Task TestBigPoco()
        {
            var sql = "CREATE TABLE IF NOT EXISTS TestBigPoco(ID INT PRIMARY KEY, PROP1 TINYINT, PROP2 SMALLINT, PROP3 INT, " +
                      "PROP4 BIGINT, PROP5 FLOAT, PROP6 DOUBLE, PROP7 BIGINT, PROP8 VARCHAR, PROP9 INT, PROP10 INT)";

            await Client.Sql.ExecuteAsync(null, sql);

            using var deferDropTable = new DisposeAction(
                () => Client.Sql.ExecuteAsync(null, "DROP TABLE TestBigPoco").GetAwaiter().GetResult());

            var table = await Client.Tables.GetTableAsync("TestBigPoco".ToUpperInvariant());
            var pocoView = table!.GetRecordView<Poco2>();

            var poco = new Poco2
            {
                Id = -1,
                Prop1 = 1,
                Prop2 = 2,
                Prop3 = 3,
                Prop4 = 4,
                Prop5 = 5,
                Prop6 = 6,
                Prop7 = 7,
                Prop8 = "8",
                Prop9 = 9,
                Prop10 = 10
            };

            await pocoView.UpsertAsync(null, poco);

            var res = (await pocoView.GetAsync(null, new Poco2 { Id = -1 })).Value;

            Assert.AreEqual(poco.Prop1, res.Prop1);
            Assert.AreEqual(poco.Prop2, res.Prop2);
            Assert.AreEqual(poco.Prop3, res.Prop3);
            Assert.AreEqual(poco.Prop4, res.Prop4);
            Assert.AreEqual(poco.Prop5, res.Prop5);
            Assert.AreEqual(poco.Prop6, res.Prop6);
            Assert.AreEqual(poco.Prop7, res.Prop7);
            Assert.AreEqual(poco.Prop8, res.Prop8);
            Assert.AreEqual(poco.Prop9, res.Prop9);
            Assert.AreEqual(poco.Prop10, res.Prop10);
        }

        [Test]
        public async Task TestAllColumnsPoco()
        {
            var pocoView = PocoAllColumnsView;

            var poco = new PocoAllColumns(
                Key: 123,
                Str: "str",
                Int8: 8,
                Int16: 16,
                Int32: 32,
                Int64: 64,
                Float: 32.32f,
                Double: 64.64,
                Uuid: Guid.NewGuid(),
                Decimal: 123.456m);

            await pocoView.UpsertAsync(null, poco);

            var res = (await pocoView.GetAsync(null, poco)).Value;

            Assert.AreEqual(poco.Decimal, res.Decimal);
            Assert.AreEqual(poco.Double, res.Double);
            Assert.AreEqual(poco.Float, res.Float);
            Assert.AreEqual(poco.Int8, res.Int8);
            Assert.AreEqual(poco.Int16, res.Int16);
            Assert.AreEqual(poco.Int32, res.Int32);
            Assert.AreEqual(poco.Int64, res.Int64);
            Assert.AreEqual(poco.Str, res.Str);
            Assert.AreEqual(poco.Uuid, res.Uuid);
        }

        [Test]
        public async Task TestAllColumnsPocoBigDecimal()
        {
            var pocoView = PocoAllColumnsBigDecimalView;

            var poco = new PocoAllColumnsBigDecimal(
                Key: 123,
                Str: "str",
                Int8: 8,
                Int16: 16,
                Int32: 32,
                Int64: 64,
                Float: 32.32f,
                Double: 64.64,
                Uuid: Guid.NewGuid(),
                Decimal: new BigDecimal(123456789, 2));

            await pocoView.UpsertAsync(null, poco);

            var res = (await pocoView.GetAsync(null, poco)).Value;

            Assert.AreEqual(poco.Decimal, res.Decimal);
            Assert.AreEqual(poco.Double, res.Double);
            Assert.AreEqual(poco.Float, res.Float);
            Assert.AreEqual(poco.Int8, res.Int8);
            Assert.AreEqual(poco.Int16, res.Int16);
            Assert.AreEqual(poco.Int32, res.Int32);
            Assert.AreEqual(poco.Int64, res.Int64);
            Assert.AreEqual(poco.Str, res.Str);
            Assert.AreEqual(poco.Uuid, res.Uuid);
        }

        [Test]
        public async Task TestAllColumnsPocoNullableNotNull()
        {
            var pocoView = PocoAllColumnsNullableView;

            var dt = LocalDateTime.FromDateTime(DateTime.UtcNow);
            var poco = new PocoAllColumnsNullable(
                Key: 123,
                Str: "str",
                Int8: 8,
                Int16: 16,
                Int32: 32,
                Int64: 64,
                Float: 32.32f,
                Double: 64.64,
                Uuid: Guid.NewGuid(),
                Date: dt.Date,
                Time: dt.TimeOfDay,
                DateTime: dt,
                Timestamp: Instant.FromDateTimeUtc(DateTime.UtcNow),
                Blob: new byte[] { 1, 2, 3 },
                Decimal: 123.456m,
                Boolean: true);

            await pocoView.UpsertAsync(null, poco);

            var res = (await pocoView.GetAsync(null, poco)).Value;

            Assert.AreEqual(poco.Blob, res.Blob);
            Assert.AreEqual(poco.Date, res.Date);
            Assert.AreEqual(poco.Decimal, res.Decimal);
            Assert.AreEqual(poco.Double, res.Double);
            Assert.AreEqual(poco.Float, res.Float);
            Assert.AreEqual(poco.Int8, res.Int8);
            Assert.AreEqual(poco.Int16, res.Int16);
            Assert.AreEqual(poco.Int32, res.Int32);
            Assert.AreEqual(poco.Int64, res.Int64);
            Assert.AreEqual(poco.Str, res.Str);
            Assert.AreEqual(poco.Uuid, res.Uuid);
            Assert.AreEqual(poco.Timestamp, res.Timestamp);
            Assert.AreEqual(poco.Time, res.Time);
            Assert.AreEqual(poco.DateTime, res.DateTime);
            Assert.AreEqual(poco.Boolean, res.Boolean);
        }

        [Test]
        public async Task TestAllColumnsPocoNullable()
        {
            var pocoView = PocoAllColumnsNullableView;

            var poco = new PocoAllColumnsNullable(123);
            await pocoView.UpsertAsync(null, poco);

            var res = (await pocoView.GetAsync(null, poco)).Value;

            Assert.AreEqual(poco.Blob, res.Blob);
            Assert.AreEqual(poco.Date, res.Date);
            Assert.AreEqual(poco.Decimal, res.Decimal);
            Assert.AreEqual(poco.Double, res.Double);
            Assert.AreEqual(poco.Float, res.Float);
            Assert.AreEqual(poco.Int8, res.Int8);
            Assert.AreEqual(poco.Int16, res.Int16);
            Assert.AreEqual(poco.Int32, res.Int32);
            Assert.AreEqual(poco.Int64, res.Int64);
            Assert.AreEqual(poco.Str, res.Str);
            Assert.AreEqual(poco.Uuid, res.Uuid);
            Assert.AreEqual(poco.Timestamp, res.Timestamp);
            Assert.AreEqual(poco.Time, res.Time);
            Assert.AreEqual(poco.DateTime, res.DateTime);
            Assert.AreEqual(poco.Boolean, res.Boolean);
        }

        [Test]
        public async Task TestEnumColumns()
        {
            var table = await Client.Tables.GetTableAsync(TableAllColumnsNotNullName);

            // Normal values.
            await Test(new PocoEnums.PocoIntEnum(1, PocoEnums.IntEnum.Foo));
            await Test(new PocoEnums.PocoByteEnum(1, PocoEnums.ByteEnum.Foo));
            await Test(new PocoEnums.PocoShortEnum(1, PocoEnums.ShortEnum.Foo));
            await Test(new PocoEnums.PocoLongEnum(1, PocoEnums.LongEnum.Foo));

            // Values that are not represented in the enum (it is just a number underneath).
            await Test(new PocoEnums.PocoIntEnum(1, (PocoEnums.IntEnum)100));
            await Test(new PocoEnums.PocoByteEnum(1, (PocoEnums.ByteEnum)101));
            await Test(new PocoEnums.PocoShortEnum(1, (PocoEnums.ShortEnum)102));
            await Test(new PocoEnums.PocoLongEnum(1, (PocoEnums.LongEnum)103));

            // Default values.
            await Test(new PocoEnums.PocoIntEnum(1, default));
            await Test(new PocoEnums.PocoByteEnum(1, default));
            await Test(new PocoEnums.PocoShortEnum(1, default));
            await Test(new PocoEnums.PocoLongEnum(1, default));

            async Task Test<T>(T val)
                where T : notnull
            {
                var view = table!.GetRecordView<T>();

                await view.UpsertAsync(null, val);

                var res = await view.GetAsync(null, val);
                Assert.AreEqual(val, res.Value);
            }
        }

        [Test]
        public async Task TestEnumColumnsNullable()
        {
            var table = await Client.Tables.GetTableAsync(TableAllColumnsName);

            // Normal values.
            await Test(new PocoEnums.PocoIntEnumNullable(1, PocoEnums.IntEnum.Foo));
            await Test(new PocoEnums.PocoByteEnumNullable(1, PocoEnums.ByteEnum.Foo));
            await Test(new PocoEnums.PocoShortEnumNullable(1, PocoEnums.ShortEnum.Foo));
            await Test(new PocoEnums.PocoLongEnumNullable(1, PocoEnums.LongEnum.Foo));

            // Values that are not represented in the enum (it is just a number underneath).
            await Test(new PocoEnums.PocoIntEnumNullable(1, (PocoEnums.IntEnum)100));
            await Test(new PocoEnums.PocoByteEnumNullable(1, (PocoEnums.ByteEnum)101));
            await Test(new PocoEnums.PocoShortEnumNullable(1, (PocoEnums.ShortEnum)102));
            await Test(new PocoEnums.PocoLongEnumNullable(1, (PocoEnums.LongEnum)103));

            // Default values.
            await Test(new PocoEnums.PocoIntEnumNullable(1, default));
            await Test(new PocoEnums.PocoByteEnumNullable(1, default));
            await Test(new PocoEnums.PocoShortEnumNullable(1, default));
            await Test(new PocoEnums.PocoLongEnumNullable(1, default));

            async Task Test<T>(T val)
                where T : notnull
            {
                var view = table!.GetRecordView<T>();

                await view.UpsertAsync(null, val);

                var res = await view.GetAsync(null, val);
                Assert.AreEqual(val, res.Value);
            }
        }

        [Test]
        public async Task TestUnsupportedColumnTypeThrowsException()
        {
            var table = await Client.Tables.GetTableAsync(TableAllColumnsName);
            var pocoView = table!.GetRecordView<UnsupportedByteType>();

            var ex = Assert.ThrowsAsync<IgniteClientException>(async () => await pocoView.UpsertAsync(null, new UnsupportedByteType(1)));
            Assert.AreEqual(
                "Can't map field 'UnsupportedByteType.<Int8>k__BackingField' of type 'System.Byte' " +
                "to column 'INT8' of type 'System.SByte' - types do not match.",
                ex!.Message);
        }

        [Test]
        public async Task TestColumnNullabilityMismatchThrowsException()
        {
            var table = await Client.Tables.GetTableAsync(TableAllColumnsName);
            var pocoView = table!.GetRecordView<NonNullableLongType>();

            var ex = Assert.ThrowsAsync<IgniteClientException>(async () => await pocoView.UpsertAsync(null, new NonNullableLongType(1, 1)));
            Assert.AreEqual(
                "Can't map field 'NonNullableLongType.<Int64>k__BackingField' of type 'System.Int64' " +
                "to column 'INT64' - column is nullable, but field is not.",
                ex!.Message);
        }

        [Test]
        public async Task TestUnsupportedEnumColumnTypeThrowsException()
        {
            var table = await Client.Tables.GetTableAsync(TableAllColumnsName);
            var pocoView = table!.GetRecordView<PocoEnums.PocoUnsignedByteEnum>();
            var poco = new PocoEnums.PocoUnsignedByteEnum(1, default);

            var ex = Assert.ThrowsAsync<IgniteClientException>(async () => await pocoView.UpsertAsync(null, poco));
            Assert.AreEqual(
                "Can't map field 'PocoUnsignedByteEnum.<Int8>k__BackingField' of type " +
                "'Apache.Ignite.Tests.Common.Table.PocoEnums+UnsignedByteEnum' to column 'INT8' of type 'System.SByte' - types do not match.",
                ex!.Message);
        }

        [Test]
        public async Task TestContainsKey()
        {
            var keyPoco = GetPoco(1);
            var poco = GetPoco(1, "foo");

            await PocoView.UpsertAsync(null, poco);

            Assert.IsTrue(await PocoView.ContainsKeyAsync(null, keyPoco));
            Assert.IsTrue(await PocoView.ContainsKeyAsync(null, poco));
            Assert.IsFalse(await PocoView.ContainsKeyAsync(null, GetPoco(-128)));
        }

        [Test]
        public async Task TestContainsAllKeysWhenAllKeysExistReturnsTrue()
        {
            var records = Enumerable
                .Range(1, 10)
                .Select(x => GetPoco(x, x.ToString(CultureInfo.InvariantCulture)));
            await PocoView.UpsertAllAsync(null, records);

            var result = await PocoView.ContainsAllKeysAsync(null, Enumerable.Range(1, 10).Select(x => GetPoco(x)));

            Assert.IsTrue(result);
        }

        [Test]
        public async Task TestContainsAllKeysWithAllNonExistingKeysReturnsFalse()
        {
            var result = await PocoView.ContainsAllKeysAsync(null, [GetPoco(1), GetPoco(2)]);

            Assert.IsFalse(result);
        }

        [Test]
        public async Task TestContainsAllKeysWithNonExistingKeysReturnsFalse()
        {
            var records = Enumerable
                .Range(1, 10)
                .Select(x => GetPoco(x, x.ToString(CultureInfo.InvariantCulture)));
            await PocoView.UpsertAllAsync(null, records);

            var result = await PocoView.ContainsAllKeysAsync(null, Enumerable.Range(5, 10).Select(x => GetPoco(x)));

            Assert.IsFalse(result);
        }

        [Test]
        public void TestContainsAllKeysThrowsArgumentExceptionOnNullCollectionElement()
        {
            var ex = Assert.ThrowsAsync<ArgumentException>(
                async () => await PocoView.ContainsAllKeysAsync(null, [GetPoco(1), null!]));

            Assert.AreEqual("Record collection can't contain null elements.", ex!.Message);
        }

        [Test]
        public void TestToString()
        {
            StringAssert.StartsWith("RecordView`1[Poco] { Table = Table { Name = PUBLIC.TBL1, Id =", PocoView.ToString());
        }

        // ReSharper disable NotAccessedPositionalProperty.Local
        private record UnsupportedByteType(byte Int8);

        private record NonNullableLongType(long Key, long Int64);
    }
}
