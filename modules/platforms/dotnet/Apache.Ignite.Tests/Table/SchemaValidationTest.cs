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
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Common.Table;
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
        Assert.AreEqual("Tuple doesn't match schema: schemaVersion=1, extraColumns=FOO, BAR", ex!.Message);
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
        Assert.AreEqual("Tuple pair doesn't match schema: schemaVersion=1, extraColumns=BAR", ex!.Message);
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
        Assert.AreEqual("Tuple pair doesn't match schema: schemaVersion=1, extraColumns=BAZ", ex!.Message);
    }

    [Test]
    public void TestMissingKeyTupleFields()
    {
        var igniteTuple = new IgniteTuple
        {
            [ValCol] = "v"
        };

        var ex = Assert.ThrowsAsync<ArgumentException>(async () => await TupleView.UpsertAsync(null, igniteTuple));
        Assert.AreEqual("Key column 'KEY' not found in the provided tuple 'IgniteTuple { VAL = v }'", ex!.Message);
    }

    [Test]
    public void TestMissingValTupleFields()
    {
        var igniteTuple = new IgniteTuple
        {
            [KeyCol] = 1L
        };

        var ex = Assert.ThrowsAsync<MarshallerException>(async () => await TableRequiredVal.RecordBinaryView.UpsertAsync(null, igniteTuple));
        StringAssert.Contains("Column 'VAL' does not allow NULLs", ex!.Message);
    }

    [Test]
    public void TestKvMissingKeyTupleFields()
    {
        var keyTuple = new IgniteTuple();

        var valTuple = new IgniteTuple
        {
            [ValCol] = "v"
        };

        var ex = Assert.ThrowsAsync<MarshallerException>(async () => await Table.KeyValueBinaryView.PutAsync(null, keyTuple, valTuple));
        StringAssert.Contains("Missed key column: KEY", ex!.Message);
    }

    [Test]
    public void TestKvMissingValTupleFields()
    {
        var keyTuple = new IgniteTuple
        {
            [KeyCol] = 1L
        };

        var valTuple = new IgniteTuple();

        var ex = Assert.ThrowsAsync<MarshallerException>(
            async () => await TableRequiredVal.KeyValueBinaryView.PutAsync(null, keyTuple, valTuple));
        StringAssert.Contains("Column 'VAL' does not allow NULLs", ex!.Message);
    }

    [Test]
    public void TestMissingAllFields()
    {
        var igniteTuple = new IgniteTuple
        {
            ["abc"] = "v"
        };

        var ex = Assert.ThrowsAsync<ArgumentException>(async () => await TupleView.UpsertAsync(null, igniteTuple));
        Assert.AreEqual("Key column 'KEY' not found in the provided tuple 'IgniteTuple { ABC = v }'", ex!.Message);
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
    public void TestDuplicateTupleFields()
    {
        var tuple = new DuplicateFieldTuple();

        var ex = Assert.ThrowsAsync<ArgumentException>(async () => await Table.RecordBinaryView.UpsertAsync(null, tuple));
        StringAssert.StartsWith("Duplicate column in Tuple: KEY", ex!.Message);
    }

    [Test]
    public void TestKvDuplicateTupleFields()
    {
        var tuple = new DuplicateFieldTuple();

        var ex = Assert.ThrowsAsync<ArgumentException>(async () => await Table.KeyValueBinaryView.PutAsync(null, tuple, tuple));
        StringAssert.StartsWith("Duplicate column in Key portion of KeyValue pair: KEY", ex!.Message);
    }

    [Test]
    public void TestUnmappedPocoFields()
    {
        var poco = new PocoUnmapped(1, "x", "y");

        var ex = Assert.ThrowsAsync<ArgumentException>(
            async () => await Table.GetRecordView<PocoUnmapped>().UpsertAsync(null, poco));

        Assert.AreEqual(
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

        Assert.AreEqual(
            "KeyValue pair of type (Apache.Ignite.Tests.Table.SchemaValidationTest+PocoUnmapped, ) doesn't match schema: " +
            "schemaVersion=1, extraColumns=Foo, Bar",
            ex!.Message);
    }

    [Test]
    public void TestKvUnmappedValPocoFields()
    {
        var poco = new PocoUnmapped(1, "x", "y");

        var ex = Assert.ThrowsAsync<ArgumentException>(
            async () => await Table.GetKeyValueView<long, PocoUnmapped>().PutAsync(null, 1, poco));

        Assert.AreEqual(
            "KeyValue pair of type (, Apache.Ignite.Tests.Table.SchemaValidationTest+PocoUnmapped) doesn't match schema: " +
            "schemaVersion=1, extraColumns=Foo, Bar",
            ex!.Message);
    }

    [Test]
    public void TestMissingKeyPocoFields()
    {
        var ex = Assert.ThrowsAsync<MarshallerException>(async () => await Table.GetRecordView<ValPoco>().UpsertAsync(null, new ValPoco()));

        StringAssert.Contains("Missed key column: KEY", ex!.Message);
    }

    [Test]
    public void TestMissingValPocoFields()
    {
        var ex = Assert.ThrowsAsync<MarshallerException>(
            async () => await TableRequiredVal.GetRecordView<KeyPoco>().UpsertAsync(null, new KeyPoco()));

        StringAssert.Contains("Column 'VAL' does not allow NULLs", ex!.Message);
    }

    [Test]
    public void TestKvMissingKeyPocoFields()
    {
        var ex = Assert.ThrowsAsync<MarshallerException>(
            async () => await Table.GetKeyValueView<ValPoco, string>().PutAsync(null, new ValPoco(), "x"));

        StringAssert.Contains("Missed key column: KEY", ex!.Message);
    }

    [Test]
    public void TestKvMissingValPocoFields()
    {
        var ex = Assert.ThrowsAsync<MarshallerException>(
            async () => await TableRequiredVal.GetKeyValueView<long, KeyPoco>().PutAsync(null, 1L, new KeyPoco()));

        StringAssert.Contains("Column 'VAL' does not allow NULLs", ex!.Message);
    }

    [Test]
    public void TestMissingAllPocoFields()
    {
        var ex = Assert.ThrowsAsync<ArgumentException>(
            async () => await Table.GetRecordView<PocoNoMatchingFields>().UpsertAsync(null, new PocoNoMatchingFields("x", 1)));

        Assert.AreEqual(
            "Can't map 'Apache.Ignite.Tests.Table.SchemaValidationTest+PocoNoMatchingFields' to columns 'Int64 KEY, String VAL'. " +
            "Matching fields not found.",
            ex!.Message);
    }

    [Test]
    public void TestKvMissingAllPocoFields()
    {
        var poco = new PocoNoMatchingFields("x", 1);
        var ex = Assert.ThrowsAsync<ArgumentException>(
            async () => await Table.GetKeyValueView<PocoNoMatchingFields, PocoNoMatchingFields>().PutAsync(null, poco, poco));

        Assert.AreEqual(
            "Can't map 'Apache.Ignite.Tests.Table.SchemaValidationTest+PocoNoMatchingFields' " +
            "and 'Apache.Ignite.Tests.Table.SchemaValidationTest+PocoNoMatchingFields' to columns 'Int64 KEY, String VAL'. " +
            "Matching fields not found.",
            ex!.Message);
    }

    [Test]
    public void TestKvDuplicatePocoFields()
    {
        var ex = Assert.ThrowsAsync<ArgumentException>(
            async () => await Table.GetRecordView<DuplicateFieldPoco>().UpsertAsync(null, new DuplicateFieldPoco()));

        StringAssert.StartsWith("Column 'VAL' maps to more than one field", ex!.Message);
    }

    // ReSharper disable NotAccessedPositionalProperty.Local
    private record PocoUnmapped(long Key, string? Foo, string? Bar);

    private record PocoNoMatchingFields(string Name, int Age);

    private class DuplicateFieldTuple : IIgniteTuple
    {
        public int FieldCount => 3;

        public object? this[int ordinal]
        {
            get => ordinal == 0 ? 1L : "1";
            set {}
        }

        public object? this[string name]
        {
            get => name == "KEY" ? 1L : "1";
            set {}
        }

        public string GetName(int ordinal) => "KEY";

        public int GetOrdinal(string name) => name == "KEY" ? 0 : 1;
    }

    [SuppressMessage("ReSharper", "UnusedMember.Local", Justification = "Reviewed")]
    private class DuplicateFieldPoco
    {
        public long Key { get; set; }

        [Column("VAL")]
        public string? Val { get; set; }

        [Column("VAL")]
        public string? Val2 { get; set; }
    }
}
