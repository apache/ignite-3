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

namespace Apache.Ignite.Tests.Table;

using System;
using System.Threading.Tasks;
using Ignite.Table;
using NUnit.Framework;

/// <summary>
/// Tests schema validation.
/// <para>Client checks unmapped columns.</para>
/// <para>Server checks missing columns.</para>
/// </summary>
public class SchemaValidationTest : IgniteTestsBase
{
    private const string TableNameRequiredVal = nameof(SchemaValidationTest);

    private ITable TableRequiredVal { get; set; } = null!;

    [OneTimeSetUp]
    public async Task CreateTable()
    {
        await Client.Sql.ExecuteAsync(null, $"CREATE TABLE {TableNameRequiredVal} (KEY BIGINT PRIMARY KEY, VAL VARCHAR NOT NULL)");
        TableRequiredVal = (await Client.Tables.GetTableAsync(TableNameRequiredVal))!;
    }

    [OneTimeTearDown]
    public async Task DropTable() => await Client.Sql.ExecuteAsync(null, $"DROP TABLE {TableNameRequiredVal}");

    [Test]
    public void TestUnmappedTupleFields()
    {
        var igniteTuple = new IgniteTuple
        {
            [KeyCol] = 1L,
            [ValCol] = "v",
            ["foo"] = "abc",
            ["bar"] = null
        };

        var ex = Assert.ThrowsAsync<ArgumentException>(async () => await TupleView.UpsertAsync(null, igniteTuple));
        StringAssert.StartsWith("Tuple doesn't match schema: schemaVersion=1, extraColumns=FOO, BAR ", ex!.Message);
    }

    [Test]
    public void TestKvUnmappedKeyTupleFields()
    {
        var keyTuple = new IgniteTuple
        {
            [KeyCol] = 1L,
            ["bar"] = null
        };

        var valTuple = new IgniteTuple
        {
            [ValCol] = "v"
        };

        var kvView = Table.KeyValueBinaryView;
        var ex = Assert.ThrowsAsync<ArgumentException>(async () => await kvView.PutAsync(null, keyTuple, valTuple));
        StringAssert.StartsWith("Tuple pair doesn't match schema: schemaVersion=1, extraColumns=BAR ", ex!.Message);
    }

    [Test]
    public void TestKvUnmappedValTupleFields()
    {
        var keyTuple = new IgniteTuple
        {
            [KeyCol] = 1L
        };

        var valTuple = new IgniteTuple
        {
            [ValCol] = "v",
            ["baz"] = 0
        };

        var kvView = Table.KeyValueBinaryView;
        var ex = Assert.ThrowsAsync<ArgumentException>(async () => await kvView.PutAsync(null, keyTuple, valTuple));
        StringAssert.StartsWith("Tuple pair doesn't match schema: schemaVersion=1, extraColumns=BAZ ", ex!.Message);
    }

    [Test]
    public void TestMissingKeyTupleFields()
    {
        var igniteTuple = new IgniteTuple
        {
            [ValCol] = "v"
        };

        var ex = Assert.ThrowsAsync<IgniteException>(async () => await TupleView.UpsertAsync(null, igniteTuple));
        Assert.AreEqual("Missed key column: KEY", ex!.Message);
    }

    [Test]
    public void TestMissingValTupleFields()
    {
        var igniteTuple = new IgniteTuple
        {
            [KeyCol] = 1L
        };

        var ex = Assert.ThrowsAsync<IgniteException>(async () => await TableRequiredVal.RecordBinaryView.UpsertAsync(null, igniteTuple));
        StringAssert.StartsWith("Failed to set column (null was passed, but column is not null", ex!.Message);
        StringAssert.Contains("name=VAL", ex.Message);
    }

    [Test]
    public void TestKvMissingKeyTupleFields()
    {
        var keyTuple = new IgniteTuple();

        var valTuple = new IgniteTuple
        {
            [ValCol] = "v"
        };

        var ex = Assert.ThrowsAsync<IgniteException>(async () => await Table.KeyValueBinaryView.PutAsync(null, keyTuple, valTuple));
        Assert.AreEqual("Missed key column: KEY", ex!.Message);
    }

    [Test]
    public void TestKvMissingValTupleFields()
    {
        var keyTuple = new IgniteTuple
        {
            [KeyCol] = 1L
        };

        var valTuple = new IgniteTuple();

        var ex = Assert.ThrowsAsync<IgniteException>(
            async () => await TableRequiredVal.KeyValueBinaryView.PutAsync(null, keyTuple, valTuple));
        StringAssert.StartsWith("Failed to set column (null was passed, but column is not null", ex!.Message);
        StringAssert.Contains("name=VAL", ex.Message);
    }

    [Test]
    public void TestMissingAllFields()
    {
        var igniteTuple = new IgniteTuple
        {
            ["abc"] = "v"
        };

        var ex = Assert.ThrowsAsync<ArgumentException>(async () => await TupleView.UpsertAsync(null, igniteTuple));
        Assert.AreEqual("Can't map 'IgniteTuple { ABC = v }' to columns 'Int64 KEY, String VAL'. Matching fields not found.", ex!.Message);
    }

    [Test]
    public void TestKvMissingAllFields()
    {
        var keyTuple = new IgniteTuple
        {
            ["abc"] = "v"
        };

        var valTuple = new IgniteTuple
        {
            ["x"] = "y"
        };

        var ex = Assert.ThrowsAsync<ArgumentException>(async () => await Table.KeyValueBinaryView.PutAsync(null, keyTuple, valTuple));
        Assert.AreEqual(
            "Can't map 'KvPair { Key = IgniteTuple { ABC = v }, Val = IgniteTuple { X = y } }' to columns 'Int64 KEY, String VAL'. " +
            "Matching fields not found.",
            ex!.Message);
    }

    [Test]
    public void TestUnmappedPocoFields()
    {
        var poco = new PocoUnmapped(1, "x", "y");

        var ex = Assert.ThrowsAsync<ArgumentException>(
            async () => await Table.GetRecordView<PocoUnmapped>().UpsertAsync(null, poco));

        StringAssert.StartsWith(
            "Record of type Apache.Ignite.Tests.Table.SchemaValidationTest+PocoUnmapped doesn't match schema: " +
            "schemaVersion=1, extraColumns=Foo, Bar",
            ex!.Message);
    }

    [Test]
    public void TestKvUnmappedKeyPocoFields()
    {
        var poco = new PocoUnmapped(1, "x", "y");

        var ex = Assert.ThrowsAsync<ArgumentException>(
            async () => await Table.GetKeyValueView<PocoUnmapped, string>().PutAsync(null, poco, "x"));

        StringAssert.StartsWith(
            "KeyValue pair of type (Apache.Ignite.Tests.Table.SchemaValidationTest+PocoUnmapped, ) doesn't match schema: " +
            "schemaVersion=1, extraColumns=Foo, Bar",
            ex!.Message);
    }

    [Test]
    public void TestKvUnmappedValPocoFields()
    {
        Assert.Fail("TODO");
    }

    [Test]
    public void TestMissingKeyPocoFields()
    {
        Assert.Fail("TODO");
    }

    [Test]
    public void TestMissingValPocoFields()
    {
        Assert.Fail("TODO");
    }

    [Test]
    public void TestKvMissingKeyPocoFields()
    {
        Assert.Fail("TODO");
    }

    [Test]
    public void TestKvMissingValPocoFields()
    {
        Assert.Fail("TODO");
    }

    [Test]
    public void TestMissingAllPocoFields()
    {
        Assert.Fail("TODO");
    }

    [Test]
    public void TestKvMissingAllPocoFields()
    {
        Assert.Fail("TODO");
    }

    private record PocoUnmapped(long Key, string? Foo, string? Bar);
}
