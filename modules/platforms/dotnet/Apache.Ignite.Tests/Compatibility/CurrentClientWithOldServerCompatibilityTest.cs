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
using Ignite.Sql;
using Ignite.Table;
using Internal;
using Internal.Proto;
using Internal.Table;
using Network;
using NodaTime;
using NUnit.Framework;
using TestHelpers;

[TestFixture("3.0.0")]
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

        var cfg = new IgniteClientConfiguration($"localhost:{_javaServer.Port}");
        _client = await IgniteClient.StartAsync(cfg);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown()
    {
        _client.Dispose();
        _javaServer.Dispose();
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

        IReadOnlyDictionary<IPartition, IClusterNode> primaryReplicas = await table.PartitionManager.GetPrimaryReplicasAsync();
        Assert.AreEqual(25, primaryReplicas.Count);

        var clusterNode = _client.GetConnections().Select(x => x.Node).Single();

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

        var connectedNode = _client.GetConnections().Select(x => x.Node).Single();

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
        int count = 12345;
        int minId = ++_idGen;

        var tuples = Enumerable.Range(0, count)
            .Select(_ => ++_idGen)
            .Select(id => new IgniteTuple { ["ID"] = id, ["NAME"] = $"test{id}" })
            .ToList();

        var table = await _client.Tables.GetTableAsync(TableNameTest);
        await table!.RecordBinaryView.UpsertAllAsync(null, tuples);

        var statement = new SqlStatement($"SELECT * FROM {TableNameTest} WHERE ID > ?", pageSize: 10);

        await using var cursor = await _client.Sql.ExecuteAsync(null, statement, minId);

        int rowCnt = 0;
        await foreach (var row in cursor)
        {
            rowCnt++;
        }

        Assert.AreEqual(count, rowCnt);
    }

    [Test]
    public async Task TestSqlScript()
    {
        await _client.Sql.ExecuteScriptAsync("CREATE TABLE testSqlScript (id INT PRIMARY KEY, name VARCHAR)");
        var rows = await _client.Sql.ExecuteAsync(null, "SELECT * FROM SYSTEM.TABLES WHERE NAME = 'TESTSQLSCRIPT'");
        var rowList = await rows.ToListAsync();
        Assert.AreEqual(1, rowList.Count);

        await _client.Sql.ExecuteScriptAsync("DROP TABLE testSqlScript");
        rows = await _client.Sql.ExecuteAsync(null, "SELECT * FROM SYSTEM.TABLES WHERE NAME = 'TESTSQLSCRIPT'");
        rowList = await rows.ToListAsync();
        Assert.AreEqual(0, rowList.Count);
    }
}
