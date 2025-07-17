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

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Ignite.Table;
using Internal;
using Internal.Proto;
using Internal.Table;
using Network;
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
}
