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

namespace Apache.Ignite.Tests;

using System;
using System.Threading.Tasks;
using Common;
using Common.Compute;
using Ignite.Compute;
using Ignite.Table;
using Internal.Proto;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using static Common.Table.TestTables;

/// <summary>
/// Tests partition awareness in a real cluster.
/// </summary>
public class PartitionAwarenessRealClusterTests : IgniteTestsBase
{
    private const int Iterations = 50;

    private string _tableName = string.Empty;

    [TearDown]
    public async Task DropTables()
    {
        if (!string.IsNullOrWhiteSpace(_tableName))
        {
            await Client.Sql.ExecuteScriptAsync($"DROP TABLE IF EXISTS {_tableName}");
        }
    }

    /// <summary>
    /// Uses <see cref="JavaJobs.NodeNameJob"/> to get the name of the node that should be the primary for the given key,
    /// and compares to the actual node that received the request (using IgniteProxy).
    /// </summary>
    /// <param name="withTx">Whether to use transactions.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task TestPutRoutesRequestToPrimaryNode([Values(true, false)] bool withTx)
    {
        await TestRequestRouting(
            TableName,
            id => new IgniteTuple { ["KEY"] = id },
            async (client, view, tuple) =>
            {
                await using var tx = withTx ? await client.Transactions.BeginAsync() : null;
                await view.UpsertAsync(tx, tuple);
            },
            ClientOp.TupleUpsert);
    }

    [Test]
    [TestCase("SELECT * FROM TBL1 WHERE KEY = ?")]
    [TestCase("SELECT * FROM TBL1 WHERE 1 = 1 AND KEY = ?")]
    [TestCase("SELECT * FROM TBL1 WHERE VAL IS NOT NULL AND KEY = ? AND 2 = 2")]
    public async Task TestSqlSimpleKey(string query)
    {
        await TestRequestRouting(
            TableName,
            id => new IgniteTuple { ["KEY"] = id },
            async (client, _, tuple) =>
            {
                await using var resultSet = await client.Sql.ExecuteAsync(
                    transaction: null,
                    statement: query,
                    tuple[KeyCol]);
            },
            ClientOp.SqlExec);
    }

    [Test]
    [TestCase("SELECT * FROM TBL1 WHERE KEY >= ? AND KEY = ? AND KEY <= ?")]
    [TestCase("SELECT * FROM TBL1 WHERE 1 = ? AND KEY = ? AND 2 = ?")]
    public async Task TestSqlSimpleKeyWithExtraArgs(string query)
    {
        await TestRequestRouting(
            TableName,
            id => new IgniteTuple { ["KEY"] = id },
            async (client, _, tuple) =>
            {
                await using var resultSet = await client.Sql.ExecuteAsync(
                    transaction: null,
                    statement: query,
                    tuple[KeyCol],
                    tuple[KeyCol],
                    tuple[KeyCol]);
            },
            ClientOp.SqlExec);
    }

    [Test]
    public async Task TestSqlCompositeKey()
    {
        await CreateTable("(KEY BIGINT, VAL1 VARCHAR, VAL2 VARCHAR, PRIMARY KEY (KEY, VAL2))");

        await TestRequestRouting(
            _tableName,
            id => new IgniteTuple { ["KEY"] = id, ["VAL1"] = $"v1_{id}", ["VAL2"] = $"v2_{id}" },
            async (client, _, tuple) =>
            {
                await using var resultSet = await client.Sql.ExecuteAsync(
                    transaction: null,
                    statement: $"SELECT * FROM {_tableName} WHERE KEY = ? AND VAL2 = ?",
                    tuple["KEY"],
                    tuple["VAL2"]);
            },
            ClientOp.SqlExec);
    }

    [Test]
    public async Task TestSqlCompositeKeyConstants()
    {
        await CreateTable("(KEY BIGINT, VAL1 VARCHAR, VAL2 VARCHAR, PRIMARY KEY (KEY, VAL2))");

        await TestRequestRouting(
            _tableName,
            id => new IgniteTuple { ["KEY"] = 42L, ["VAL1"] = $"v1_{id}", ["VAL2"] = $"v2_{id}" },
            async (client, _, tuple) =>
            {
                await using var resultSet = await client.Sql.ExecuteAsync(
                    transaction: null,
                    statement: $"SELECT * FROM {_tableName} WHERE 1 = ? AND 2 = ? AND KEY = 42 AND VAL2 = ? AND 3 = ?",
                    1,
                    2,
                    tuple["VAL2"],
                    3);
            },
            ClientOp.SqlExec);
    }

    [Test]
    public async Task TestSqlColocateBy()
    {
        await CreateTable(
            "(KEY1 INT, KEY2 VARCHAR, COL_STRING VARCHAR, COL_GUID UUID, COL_BYTES VARBINARY, " +
            "EXTRA1 VARCHAR, EXTRA2 VARCHAR, " +
            "PRIMARY KEY (KEY2, KEY1, COL_STRING, COL_GUID, COL_BYTES)) " +
            "COLOCATE BY (KEY1, COL_GUID, COL_STRING, COL_BYTES)");

        await TestRequestRouting(
            _tableName,
            id => new IgniteTuple
            {
                ["KEY1"] = (int)id,
                ["KEY2"] = $"key2_{id}",
                ["COL_STRING"] = $"str_{id}",
                ["COL_GUID"] = Guid.NewGuid(),
                ["COL_BYTES"] = new[] { (byte)id, (byte)(id >> 8) },
                ["EXTRA1"] = $"extra1_{id}",
                ["EXTRA2"] = $"extra2_{id}"
            },
            async (client, _, tuple) =>
            {
                await using var resultSet = await client.Sql.ExecuteAsync(
                    transaction: null,
                    statement: $"SELECT * FROM {_tableName} WHERE KEY1 = ? AND COL_STRING = ? AND COL_GUID = ? AND COL_BYTES = ?",
                    tuple["KEY1"],
                    tuple["COL_STRING"],
                    tuple["COL_GUID"],
                    tuple["COL_BYTES"]);
            },
            ClientOp.SqlExec);
    }

    [Test]
    public async Task TestSqlColocateByAllConstants()
    {
        await CreateTable(
            "(KEY1 INT, KEY2 VARCHAR, VAL INT, " +
            "PRIMARY KEY (KEY1, KEY2)) " +
            "COLOCATE BY (KEY1, KEY2)");

        await TestRequestRouting(
            _tableName,
            id => new IgniteTuple
            {
                ["KEY1"] = 42,
                ["KEY2"] = "constant_key",
                ["VAL"] = (int)id
            },
            async (client, _, _) =>
            {
                await using var resultSet = await client.Sql.ExecuteAsync(
                    transaction: null,
                    statement: $"SELECT * FROM {_tableName} WHERE KEY1 = 42 AND KEY2 = 'constant_key'");
            },
            ClientOp.SqlExec);
    }

    [Test]
    public async Task TestSqlColocateByMixedConstantsMiddle()
    {
        await CreateTable(
            "(KEY1 INT, KEY2 VARCHAR, KEY3 INT, VAL VARCHAR, " +
            "PRIMARY KEY (KEY1, KEY2, KEY3)) " +
            "COLOCATE BY (KEY1, KEY2, KEY3)");

        await TestRequestRouting(
            _tableName,
            id => new IgniteTuple
            {
                ["KEY1"] = (int)id,
                ["KEY2"] = "fixed",
                ["KEY3"] = (int)(id * 2),
                ["VAL"] = $"val_{id}"
            },
            async (client, _, tuple) =>
            {
                await using var resultSet = await client.Sql.ExecuteAsync(
                    transaction: null,
                    statement: $"SELECT * FROM {_tableName} WHERE KEY1 = ? AND KEY2 = 'fixed' AND KEY3 = ?",
                    tuple["KEY1"],
                    tuple["KEY3"]);
            },
            ClientOp.SqlExec);
    }

    [Test]
    public async Task TestSqlSimpleKeyWithTransaction()
    {
        await TestRequestRouting(
            TableName,
            id => new IgniteTuple { ["KEY"] = id },
            async (client, _, tuple) =>
            {
                await using var tx = await client.Transactions.BeginAsync();
                await using var resultSet = await client.Sql.ExecuteAsync(
                    transaction: tx,
                    statement: "SELECT * FROM TBL1 WHERE KEY = ?",
                    tuple[KeyCol]);
                await tx.CommitAsync();
            },
            ClientOp.SqlExec);
    }

    [Test]
    public async Task TestSqlColocateByDifferentOrder()
    {
        await CreateTable(
            "(KEY1 INT, KEY2 INT, KEY3 INT, VAL VARCHAR, " +
            "PRIMARY KEY (KEY1, KEY2, KEY3)) " +
            "COLOCATE BY (KEY1, KEY2)");

        await TestRequestRouting(
            _tableName,
            id => new IgniteTuple
            {
                ["KEY1"] = (int)id,
                ["KEY2"] = (int)(id * 2),
                ["KEY3"] = (int)(id * 3),
                ["VAL"] = $"val_{id}"
            },
            async (client, _, tuple) =>
            {
                await using var resultSet = await client.Sql.ExecuteAsync(
                    transaction: null,
                    statement: $"SELECT * FROM {_tableName} WHERE KEY2 = ? AND KEY1 = ? AND KEY3 = ?",
                    tuple["KEY2"],
                    tuple["KEY1"],
                    tuple["KEY3"]);
            },
            ClientOp.SqlExec);
    }

    private static async Task<string> GetPrimaryNodeNameWithJavaJob(IIgniteClient client, string tableName, IIgniteTuple tuple)
    {
        var primaryNodeNameExec = await client.Compute.SubmitAsync(
            JobTarget.Colocated(tableName, tuple),
            JavaJobs.NodeNameJob,
            null);

        return await primaryNodeNameExec.GetResultAsync();
    }

    private async Task TestRequestRouting(
        string tableName,
        Func<long, IIgniteTuple> tupleFactory,
        Func<IIgniteClient, IRecordView<IIgniteTuple>, IIgniteTuple, Task> operation,
        ClientOp expectedOp)
    {
        using var loggerFactory = new ConsoleLogger(LogLevel.Trace);
        var proxies = GetProxies();
        using var client = await IgniteClient.StartAsync(GetConfig(proxies, loggerFactory));
        var recordView = (await client.Tables.GetTableAsync(tableName))!.RecordBinaryView;

        client.WaitForConnections(proxies.Count);

        // Warm up.
        await operation(client, recordView, tupleFactory(-1));
        GetRequestTargetNodeName(proxies, expectedOp);

        // Check.
        for (long key = 0; key < Iterations; key++)
        {
            var tuple = tupleFactory(key);
            var primaryNodeName = await GetPrimaryNodeNameWithJavaJob(client, tableName, tuple);

            if (primaryNodeName.EndsWith("_3", StringComparison.Ordinal) || primaryNodeName.EndsWith("_4", StringComparison.Ordinal))
            {
                // Skip nodes without direct client connection.
                continue;
            }

            await operation(client, recordView, tuple);

            var requestTargetNodeName = GetRequestTargetNodeName(proxies, expectedOp);

            Assert.AreEqual(primaryNodeName, requestTargetNodeName);
        }
    }

    private async Task CreateTable(string columns)
    {
        _tableName = $"{nameof(PartitionAwarenessRealClusterTests)}_{TestContext.CurrentContext.Test.Name}";

        await Client.Sql.ExecuteScriptAsync($"CREATE TABLE {_tableName} {columns}");
    }
}
