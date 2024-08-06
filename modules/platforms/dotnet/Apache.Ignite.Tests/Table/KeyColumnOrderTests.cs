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

using System.Threading.Tasks;
using Ignite.Table;
using NUnit.Framework;

/// <summary>
/// Tests mixed key column order (before, after, intermixed with value columns).
/// </summary>
public class KeyColumnOrderTests : IgniteTestsBase
{
    private static readonly string[] Tables = { "test1", "test2", "test3", "test4" };

    private int _key;

    [SetUp]
    public void IncrementKey() => _key++;

    [OneTimeSetUp]
    public async Task CreateTables()
    {
        await Client.Sql.ExecuteAsync(null, "CREATE TABLE test1 (key INT, key2 INT, val1 VARCHAR, val2 VARCHAR, primary key(key, key2))");
        await Client.Sql.ExecuteAsync(null, "CREATE TABLE test2 (val1 VARCHAR, val2 VARCHAR, key INT, key2 INT, primary key(key2, key))");
        await Client.Sql.ExecuteAsync(null, "CREATE TABLE test3 (key INT, val1 VARCHAR, val2 VARCHAR, key2 INT, primary key(key, key2))");
        await Client.Sql.ExecuteAsync(null, "CREATE TABLE test4 (key2 INT, val1 VARCHAR, val2 VARCHAR, key INT, primary key(key2, key))");
    }

    [OneTimeTearDown]
    public async Task DropTables()
    {
        foreach (var table in Tables)
        {
            await Client.Sql.ExecuteAsync(null, $"DROP TABLE {table}");
        }
    }

    [Test]
    [TestCaseSource(nameof(Tables))]
    public async Task TestRecordBinaryView(string tableName)
    {
        var table = await Client.Tables.GetTableAsync(tableName);
        var view = table!.RecordBinaryView;

        var key = new IgniteTuple
        {
            ["key"] = _key,
            ["key2"] = -_key
        };

        var row = new IgniteTuple
        {
            ["key"] = _key,
            ["key2"] = -_key,
            ["val1"] = "val1",
            ["val2"] = "val2"
        };

        await view.UpsertAsync(null, row);
        var res = await view.GetAsync(null, key);

        Assert.AreEqual(row, res.Value);
    }

    [Test]
    [TestCaseSource(nameof(Tables))]
    public async Task TestKeyValueBinaryView(string tableName)
    {
        var table = await Client.Tables.GetTableAsync(tableName);
        var view = table!.KeyValueBinaryView;

        var key = new IgniteTuple
        {
            ["key"] = _key,
            ["key2"] = -_key
        };

        var val = new IgniteTuple
        {
            ["val1"] = "val1",
            ["val2"] = "val2"
        };

        await view.PutAsync(null, key, val);
        var res = await view.GetAsync(null, key);

        Assert.AreEqual(val, res.Value);
    }

    [Test]
    [TestCaseSource(nameof(Tables))]
    public async Task TestRecordView(string tableName)
    {
        var table = await Client.Tables.GetTableAsync(tableName);
        var view = table!.GetRecordView<Rec>();

        var row = new Rec(_key, -_key, "val1", "val2");

        await view.UpsertAsync(null, row);
        var res = await view.GetAsync(null, row);

        Assert.AreEqual(row, res.Value);
    }

    private record Rec(int Key, int Key2, string Val1, string Val2);

    private record KeyRec(int Key, int Key2);

    private record ValRec(string Val1, string Val2);
}
