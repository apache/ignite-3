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

namespace Apache.Ignite.Tests.Compatibility;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common;
using Ignite.Compute;
using Ignite.Sql;
using Ignite.Table;
using Internal;
using Internal.Proto;
using Internal.Table;
using Microsoft.Extensions.Logging;
using Network;
using NodaTime;
using NUnit.Framework;
using TestHelpers;

[TestFixture("3.0.0")]
[Category(TestUtils.CategoryIntensive)]
public class CurrentClientWithOldServerCompatibilityTest
{
    private const string TableNameTest = "TEST";

    private const string TableNameAllColumns = "ALL_COLUMNS";

    private readonly string _serverVersion;

    private TempDir _workDir;

    private JavaServer _javaServer;

    private IIgniteClient _client;

    private int _idGen = 1000;

    public CurrentClientWithOldServerCompatibilityTest(string serverVersion) =>
        _serverVersion = serverVersion;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _workDir = new TempDir();
        _javaServer = await JavaServer.StartOldAsync(_serverVersion, _workDir.Path);

        var cfg = new IgniteClientConfiguration($"localhost:{_javaServer.Port}")
        {
            LoggerFactory = TestUtils.GetConsoleLoggerFactory(LogLevel.Trace)
        };

        var startupTimeout = TimeSpan.FromSeconds(10);

        _client = await IgniteClient.StartAsync(cfg).WaitAsync(startupTimeout);

        await _client.Sql.ExecuteScriptAsync("DELETE FROM TEST WHERE ID >= 1000").WaitAsync(startupTimeout);
        await _client.Sql.ExecuteScriptAsync("DELETE FROM ALL_COLUMNS WHERE ID >= 1000").WaitAsync(startupTimeout);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown()
    {
        // ReSharper disable ConditionalAccessQualifierIsNonNullableAccordingToAPIContract
        _client?.Dispose();
        _javaServer?.Dispose();

        // ReSharper restore ConditionalAccessQualifierIsNonNullableAccordingToAPIContract
        _workDir.Dispose();
    }

    [Test]
    public void TestProtocolFeatures()
    {
        ClientSocket socket = ((IgniteClientInternal)_client).Socket.GetSockets().First();
        ProtocolBitmaskFeature features = socket.ConnectionContext.Features;

        switch (_serverVersion)
        {
            case "3.0.0":
                Assert.AreEqual(default(ProtocolBitmaskFeature), features);
                break;

            default:
                Assert.Fail($"Unexpected server version: {_serverVersion}");
                break;
        }
    }

    [Test]
    public async Task TestTables()
    {
        var tables = await _client.Tables.GetTablesAsync();
        Assert.AreEqual(2, tables.Count);

        var tableNames = tables.Select(t => t.QualifiedName.ObjectName).Order().ToList();

        Assert.AreEqual(TableNameAllColumns, tableNames[0]);
        Assert.AreEqual(TableNameTest, tableNames[1]);
    }

    [Test]
    public async Task TestTableByName()
    {
        var table = await _client.Tables.GetTableAsync(TableNameTest);
        Assert.IsNotNull(table);

        Assert.AreEqual(TableNameTest, table.QualifiedName.ObjectName);
    }

    [Test]
    public async Task TestTableByQualifiedName()
    {
        var table = await _client.Tables.GetTableAsync(QualifiedName.Parse(TableNameTest));
        Assert.IsNotNull(table);

        Assert.AreEqual(TableNameTest, table.QualifiedName.ObjectName);
    }

    [Test]
    public async Task TestPartitionManager()
    {
        ITable? table = await _client.Tables.GetTableAsync(TableNameTest);
        Assert.IsNotNull(table);

        IReadOnlyDictionary<IPartition, IClusterNode> primaryReplicas = await table.PartitionDistribution.GetPrimaryReplicasAsync();
        Assert.AreEqual(25, primaryReplicas.Count);

        var clusterNode = _client.GetConnections().Select(x => x.Node).First();

        foreach (var (partition, node) in primaryReplicas)
        {
            Assert.IsInstanceOf<HashPartition>(partition);
            Assert.AreEqual(clusterNode.Name, node.Name);
            Assert.AreEqual(clusterNode.Id, node.Id);
        }
    }

    [Test]
    public async Task TestGetClusterNodes()
    {
        IList<IClusterNode> nodes = await _client.GetClusterNodesAsync();
        Assert.AreEqual(1, nodes.Count);

        var connectedNode = _client.GetConnections().Select(x => x.Node).First();

        Assert.AreEqual(connectedNode.Id, nodes[0].Id);
        Assert.AreEqual(connectedNode.Name, nodes[0].Name);
    }

    [Test]
    public async Task TestSqlColumnMeta()
    {
        await using var cursor = await _client.Sql.ExecuteAsync(null, $"select * from {TableNameAllColumns}");
        var meta = cursor.Metadata;
        Assert.IsNotNull(meta);

        var cols = meta.Columns;
        Assert.AreEqual(16, cols.Count);

        StringAssert.Contains("Name = ID, Type = Int32, Precision = 10", cols[0].ToString());
        StringAssert.Contains("Name = BYTE, Type = Int8, Precision = 3", cols[1].ToString());
        StringAssert.Contains("Name = SHORT, Type = Int16, Precision = 5", cols[2].ToString());
        StringAssert.Contains("Name = INT, Type = Int32, Precision = 10", cols[3].ToString());
        StringAssert.Contains("Name = LONG, Type = Int64, Precision = 19", cols[4].ToString());
        StringAssert.Contains("Name = FLOAT, Type = Float, Precision = 7", cols[5].ToString());
        StringAssert.Contains("Name = DOUBLE, Type = Double, Precision = 15", cols[6].ToString());
        StringAssert.Contains("Name = DEC, Type = Decimal, Precision = 10, Scale = 1", cols[7].ToString());
        StringAssert.Contains("Name = STRING, Type = String, Precision = 65536", cols[8].ToString());
        StringAssert.Contains("Name = GUID, Type = Uuid, Precision = -1", cols[9].ToString());
        StringAssert.Contains("Name = DT, Type = Date, Precision = 0", cols[10].ToString());
        StringAssert.Contains("Name = TM, Type = Time, Precision = 9", cols[11].ToString());
        StringAssert.Contains("Name = TS, Type = Datetime, Precision = 9", cols[12].ToString());
        StringAssert.Contains("Name = TSTZ, Type = Timestamp, Precision = 6", cols[13].ToString());
        StringAssert.Contains("Name = BOOL, Type = Boolean, Precision = 1", cols[14].ToString());
        StringAssert.Contains("Name = BYTES, Type = ByteArray, Precision = 65536", cols[15].ToString());
    }

    [Test]
    public async Task TestSqlSelectAllColumnTypes()
    {
        var rows = await _client.Sql.ExecuteAsync(
            null, $"select * from {TableNameAllColumns} where id = 1");

        Assert.IsNotNull(rows);

        var rowList = await rows.ToListAsync();
        Assert.AreEqual(1, rowList.Count);

        var row = rowList[0];
        Assert.AreEqual(1, row["ID"]);
        Assert.AreEqual((byte)1, row["BYTE"]);
        Assert.AreEqual((short)2, row["SHORT"]);
        Assert.AreEqual(3, row["INT"]);
        Assert.AreEqual(4L, row["LONG"]);
        Assert.AreEqual(5.0f, row["FLOAT"]);
        Assert.AreEqual(6.0d, row["DOUBLE"]);
        Assert.AreEqual(new BigDecimal(7m), row["DEC"]);
        Assert.AreEqual("test", row["STRING"]);
        Assert.AreEqual(Guid.Parse("10000000-2000-3000-4000-500000000000"), row["GUID"]);
        Assert.AreEqual(new LocalDate(2023, 1, 1), row["DT"]);
        Assert.AreEqual(new LocalTime(12, 0, 0, 0), row["TM"]);
        Assert.AreEqual(new LocalDateTime(2023, 1, 1, 12, 0, 0), row["TS"]);
        Assert.AreEqual(Instant.FromUnixTimeSeconds(1714946523), row["TSTZ"]);
        Assert.IsTrue((bool)row["BOOL"]!);
        CollectionAssert.AreEqual(new byte[] { 1, 2, 3, 4 }, (byte[])row["BYTES"]!);
    }

    [Test]
    public async Task TestSqlMultiplePages()
    {
        int count = 1234;
        int minId = ++_idGen;

        var tuples = Enumerable.Range(0, count)
            .Select(_ => ++_idGen)
            .Select(id => new IgniteTuple { ["ID"] = id, ["NAME"] = $"test{id}" })
            .ToList();

        var table = await _client.Tables.GetTableAsync(TableNameTest);
        await table!.RecordBinaryView.UpsertAllAsync(null, tuples);

        var statement = new SqlStatement($"SELECT * FROM {TableNameTest} WHERE ID > ?", pageSize: 10);

        await using var cursor = await _client.Sql.ExecuteAsync(null, statement, minId);
        var rowCnt = await cursor.CountAsync();
        Assert.AreEqual(count, rowCnt);
    }

    [Test]
    public async Task TestSqlScript()
    {
        await _client.Sql.ExecuteScriptAsync("CREATE TABLE testSqlScript (id INT PRIMARY KEY, name VARCHAR)");
        var rows = await _client.Sql.ExecuteAsync(
            null, "SELECT * FROM SYSTEM.TABLES WHERE NAME = 'TESTSQLSCRIPT'");

        var rowList = await rows.ToListAsync();
        Assert.AreEqual(1, rowList.Count);

        await _client.Sql.ExecuteScriptAsync("DROP TABLE testSqlScript");
        rows = await _client.Sql.ExecuteAsync(
            null, "SELECT * FROM SYSTEM.TABLES WHERE NAME = 'TESTSQLSCRIPT'");

        rowList = await rows.ToListAsync();
        Assert.AreEqual(0, rowList.Count);
    }

    [Test]
    public async Task TestRecordViewOperations()
    {
        int id = ++_idGen;
        int id2 = ++_idGen;
        var key = new IgniteTuple { ["ID"] = id };
        var key2 = new IgniteTuple { ["ID"] = id2 };

        var table = await _client.Tables.GetTableAsync(TableNameTest);
        var view = table!.RecordBinaryView;

        // Insert.
        Assert.IsTrue(await view.InsertAsync(null, new IgniteTuple { ["ID"] = id, ["NAME"] = "v1" }));
        Assert.AreEqual("v1", (await view.GetAsync(null, key)).Value["NAME"]);
        Assert.IsFalse(await view.InsertAsync(null, new IgniteTuple { ["ID"] = id, ["NAME"] = "v2" }));

        // Insert All.
        var insertAllRes = await view.InsertAllAsync(null, [
            new IgniteTuple { ["ID"] = id, ["NAME"] = "v3" },
            new IgniteTuple { ["ID"] = id2, ["NAME"] = "v4" }
        ]);
        Assert.AreEqual(1, insertAllRes.Count);
        Assert.AreEqual(id, insertAllRes[0]["ID"]);

        // Upsert.
        await view.UpsertAsync(null, new IgniteTuple { ["ID"] = id, ["NAME"] = "v2" });
        Assert.AreEqual("v2", (await view.GetAsync(null, key)).Value["NAME"]);

        // Get and upsert.
        var oldValue = await view.GetAndUpsertAsync(null, new IgniteTuple { ["ID"] = id, ["NAME"] = "v5" });
        Assert.AreEqual("v2", oldValue.Value["NAME"]);

        // Upsert All.
        await view.UpsertAllAsync(null, [
            new IgniteTuple { ["ID"] = id, ["NAME"] = "v5" },
            new IgniteTuple { ["ID"] = id2, ["NAME"] = "v6" }
        ]);
        Assert.AreEqual("v5", (await view.GetAsync(null, key)).Value["NAME"]);
        Assert.AreEqual("v6", (await view.GetAsync(null, key2)).Value["NAME"]);

        // Contains.
        Assert.IsTrue(await view.ContainsKeyAsync(null, key));
        Assert.IsFalse(await view.ContainsKeyAsync(null, new IgniteTuple { ["ID"] = -id }));

        // Contains all.
        Assert.IsTrue(await view.ContainsAllKeysAsync(null, new[] { key, key2 }));
        Assert.IsFalse(await view.ContainsAllKeysAsync(null, new[] { key, new IgniteTuple { ["ID"] = -id } }));

        // Get.
        Assert.IsTrue((await view.GetAsync(null, key)).HasValue);
        Assert.IsFalse((await view.GetAsync(null, new IgniteTuple { ["ID"] = -id })).HasValue);

        // Get all.
        var keys = new[] { key, new IgniteTuple { ["ID"] = -id } };
        var results = await view.GetAllAsync(null, keys);
        Assert.AreEqual(2, results.Count);
        Assert.AreEqual("v5", results[0].Value["NAME"]);
        Assert.IsFalse(results[1].HasValue);

        // Replace.
        Assert.IsTrue(await view.ReplaceAsync(null, new IgniteTuple { ["ID"] = id, ["NAME"] = "v7" }));
        Assert.IsFalse(await view.ReplaceAsync(null, new IgniteTuple { ["ID"] = -id, ["NAME"] = "v8" }));
        Assert.AreEqual("v7", (await view.GetAsync(null, key)).Value["NAME"]);

        // Replace exact.
        Assert.IsFalse(await view.ReplaceAsync(
            null,
            new IgniteTuple { ["ID"] = id, ["NAME"] = "-v7" },
            new IgniteTuple { ["ID"] = id, ["NAME"] = "v8" }));

        Assert.IsTrue(await view.ReplaceAsync(
            null,
            new IgniteTuple { ["ID"] = id, ["NAME"] = "v7" },
            new IgniteTuple { ["ID"] = id, ["NAME"] = "v8" }));

        Assert.AreEqual("v8", (await view.GetAsync(null, key)).Value["NAME"]);

        // Get and replace.
        var old = await view.GetAndReplaceAsync(null, new IgniteTuple { ["ID"] = id, ["NAME"] = "v9" });
        Assert.AreEqual("v8", old.Value["NAME"]);
        Assert.AreEqual("v9", (await view.GetAsync(null, key)).Value["NAME"]);

        // Delete.
        Assert.IsTrue(await view.DeleteAsync(null, key));
        Assert.IsFalse(await view.DeleteAsync(null, key));
        Assert.IsFalse((await view.GetAsync(null, key)).HasValue);

        // Delete exact.
        Assert.IsFalse(await view.DeleteExactAsync(null, new IgniteTuple { ["ID"] = id2, ["NAME"] = "v9" }));
        Assert.IsTrue(await view.DeleteExactAsync(null, new IgniteTuple { ["ID"] = id2, ["NAME"] = "v6" }));

        // Get and delete.
        await view.UpsertAsync(null, new IgniteTuple { ["ID"] = id, ["NAME"] = "v10" });
        Assert.IsFalse((await view.GetAndDeleteAsync(null, new IgniteTuple { ["ID"] = -id })).HasValue);

        var getAndDelete = await view.GetAndDeleteAsync(null, new IgniteTuple { ["ID"] = id });
        Assert.AreEqual("v10", getAndDelete.Value["NAME"]);

        // Delete all.
        await view.UpsertAsync(null, new IgniteTuple { ["ID"] = id, ["NAME"] = "v11" });
        var deleteAllRes = await view.DeleteAllAsync(null, [
            new IgniteTuple { ["ID"] = id },
            new IgniteTuple { ["ID"] = id2 }
        ]);

        Assert.AreEqual(1, deleteAllRes.Count);
        Assert.AreEqual(id2, deleteAllRes[0]["ID"]);

        // Delete all exact.
        await view.UpsertAsync(null, new IgniteTuple { ["ID"] = id, ["NAME"] = "v12" });
        await view.UpsertAsync(null, new IgniteTuple { ["ID"] = id2, ["NAME"] = "v13" });

        var deleteAllExactRes = await view.DeleteAllExactAsync(null, [
            new IgniteTuple { ["ID"] = id },
            new IgniteTuple { ["ID"] = id2, ["NAME"] = "v13" }
        ]);

        Assert.AreEqual(1, deleteAllExactRes.Count);
        Assert.AreEqual(id, deleteAllExactRes[0]["ID"]);
    }

    [Test]
    public async Task TestRecordViewAllColumnTypes()
    {
        var table = await _client.Tables.GetTableAsync(TableNameAllColumns);
        var view = table!.RecordBinaryView;

        int id = ++_idGen;

        var tuple = new IgniteTuple
        {
            ["ID"] = id,
            ["BYTE"] = (sbyte)1,
            ["SHORT"] = (short)2,
            ["INT"] = 3,
            ["LONG"] = 4L,
            ["FLOAT"] = 5.5f,
            ["DOUBLE"] = 6.6d,
            ["DEC"] = new BigDecimal(7.7m),
            ["STRING"] = "test",
            ["GUID"] = Guid.NewGuid(),
            ["DT"] = LocalDate.FromDateTime(DateTime.Now.Date),
            ["TM"] = LocalTime.FromTicksSinceMidnight(DateTime.Now.TimeOfDay.Ticks),
            ["TS"] = LocalDateTime.FromDateTime(DateTime.Now),
            ["TSTZ"] = Instant.FromUnixTimeSeconds(123456),
            ["BOOL"] = true,
            ["BYTES"] = new byte[] { 1, 2, 3, 4 }
        };

        Assert.IsTrue(await view.InsertAsync(null, tuple));

        IIgniteTuple res = (await view.GetAsync(null, new IgniteTuple { ["ID"] = id })).Value;
        Assert.AreEqual(tuple, res);
    }

    [Test]
    public async Task TestTxCommit()
    {
        int id = ++_idGen;
        var key = new IgniteTuple { ["ID"] = id };

        var table = await _client.Tables.GetTableAsync(TableNameTest);
        var view = table!.RecordBinaryView;

        Assert.IsFalse((await view.GetAsync(null, key)).HasValue);

        await _client.Transactions.RunInTransactionAsync(async tx =>
        {
            var tuple = new IgniteTuple { ["ID"] = id, ["NAME"] = "testTxCommit" };
            await view.InsertAsync(tx, tuple);
        });

        var res = (await view.GetAsync(null, key)).Value;
        Assert.IsNotNull(res);
        Assert.AreEqual("testTxCommit", res["NAME"]);
    }

    [Test]
    public async Task TestTxRollback()
    {
        int id = ++_idGen;
        var key = new IgniteTuple { ["ID"] = id };

        var table = await _client.Tables.GetTableAsync(TableNameTest);
        var view = table!.RecordBinaryView;

        Assert.IsFalse((await view.GetAsync(null, key)).HasValue);

        await using var tx = await _client.Transactions.BeginAsync();
        await view.InsertAsync(tx, new IgniteTuple { ["ID"] = id, ["NAME"] = "testTxRollback" });
        await tx.RollbackAsync();

        Assert.IsFalse((await view.GetAsync(null, key)).HasValue);
    }

    [Test]
    public async Task TestTxReadOnly()
    {
        int id = ++_idGen;
        var key = new IgniteTuple { ["ID"] = id };

        var table = await _client.Tables.GetTableAsync(TableNameTest);
        var view = table!.RecordBinaryView;

        // Start and activate a read-only transaction.
        await using var tx = await _client.Transactions.BeginAsync(new(ReadOnly: true));
        Assert.IsFalse((await view.GetAsync(tx, key)).HasValue); // Activate lazy tx.

        // Insert a record with an implicit tx.
        await view.InsertAsync(null, new IgniteTuple { ["ID"] = id, ["NAME"] = "testTxReadOnly" });

        // RO transaction should not see the changes made outside of it.
        Assert.IsFalse((await view.GetAsync(tx, key)).HasValue, "Read-only transaction shows snapshot of data in the past.");

        await tx.RollbackAsync();
    }

    [Test]
    public async Task TestComputeMissingJob()
    {
        var target = JobTarget.AnyNode(await _client.GetClusterNodesAsync());
        var desc = new JobDescriptor<string?, string?>("test");

        var ex = Assert.ThrowsAsync<ComputeException>(async () =>
        {
            var jobExecution = await _client.Compute.SubmitAsync(target, desc, null);
            await jobExecution.GetResultAsync();
        });

        StringAssert.Contains("Cannot load job class by name 'test'", ex!.Message);
    }

    [Test]
    public async Task TestStreamer()
    {
        var table = await _client.Tables.GetTableAsync(TableNameTest);
        var view = table!.RecordBinaryView;
        var keys = new List<IgniteTuple>();

        await view.StreamDataAsync(GetData(), DataStreamerOptions.Default with { PageSize = 123 });

        var results = await view.GetAllAsync(null, keys);
        Assert.AreEqual(keys.Count, results.Count);

        async IAsyncEnumerable<IIgniteTuple> GetData()
        {
            await Task.Yield(); // Simulate async enumeration.

            for (int i = 0; i < 5000; i++)
            {
                var id = ++_idGen;
                keys.Add(new IgniteTuple { ["ID"] = id });
                yield return new IgniteTuple { ["ID"] = id, ["NAME"] = $"TestStreamer{i}" };
            }
        }
    }
}
