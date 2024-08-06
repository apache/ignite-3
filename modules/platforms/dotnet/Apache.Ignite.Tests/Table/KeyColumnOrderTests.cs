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
    private static readonly string[] Tables = { "test1", "test2", "test3", "test4", "test5", "test6" };

    [OneTimeSetUp]
    public async Task CreateTables()
    {
        await Client.Sql.ExecuteAsync(null, "CREATE TABLE test1 (key INT PRIMARY KEY, val1 VARCHAR, val2 VARCHAR)");
        await Client.Sql.ExecuteAsync(null, "CREATE TABLE test2 (val1 VARCHAR, key INT PRIMARY KEY, val2 VARCHAR)");
        await Client.Sql.ExecuteAsync(null, "CREATE TABLE test3 (val1 VARCHAR, val2 VARCHAR, key INT PRIMARY KEY)");
        await Client.Sql.ExecuteAsync(null, "CREATE TABLE test4 (key INT PRIMARY KEY, val1 VARCHAR, val2 VARCHAR)");
        await Client.Sql.ExecuteAsync(null, "CREATE TABLE test5 (val1 VARCHAR, key INT PRIMARY KEY, val2 VARCHAR)");
        await Client.Sql.ExecuteAsync(null, "CREATE TABLE test6 (val1 VARCHAR, val2 VARCHAR, key INT PRIMARY KEY)");
    }

    [OneTimeTearDown]
    public async Task DropTables()
    {
        await Client.Sql.ExecuteAsync(null, "DROP TABLE test1");
        await Client.Sql.ExecuteAsync(null, "DROP TABLE test2");
        await Client.Sql.ExecuteAsync(null, "DROP TABLE test3");
        await Client.Sql.ExecuteAsync(null, "DROP TABLE test4");
        await Client.Sql.ExecuteAsync(null, "DROP TABLE test5");
        await Client.Sql.ExecuteAsync(null, "DROP TABLE test6");
    }

    [Test]
    [TestCaseSource(nameof(Tables))]
    public async Task TestRecordBinaryView(string tableName)
    {
        var table = await Client.Tables.GetTableAsync(tableName);
        var view = table!.RecordBinaryView;

        var key = new IgniteTuple
        {
            ["key"] = 1
        };

        var row = new IgniteTuple
        {
            ["key"] = 1,
            ["val1"] = "val1",
            ["val2"] = "val2"
        };

        await view.UpsertAsync(null, row);
        var res = await view.GetAsync(null, key);

        Assert.AreEqual(row, res.Value);
    }
}
