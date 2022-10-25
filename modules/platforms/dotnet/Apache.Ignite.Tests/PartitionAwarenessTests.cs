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
using System.Collections.Generic;
using System.Threading.Tasks;
using Internal.Proto;
using NUnit.Framework;

/// <summary>
/// Tests partition awareness.
/// TODO:
/// * testClientReceivesPartitionAssignmentUpdates
/// * testCustomColocationKey
/// * testCompositeKey
/// * testAllOperations (KV, Record)
/// * HashCalculatorTests - check consistency with Java via Compute.
/// </summary>
public class PartitionAwarenessTests
{
    private FakeServer _server1 = null!;
    private FakeServer _server2 = null!;

    [SetUp]
    public void SetUp()
    {
        _server1 = new FakeServer(nodeName: "srv1");
        _server2 = new FakeServer(nodeName: "srv2");

        var assignment = new[] { _server1.Node.Id, _server2.Node.Id };
        _server1.PartitionAssignment = assignment;
        _server2.PartitionAssignment = assignment;
    }

    [TearDown]
    public void TearDown()
    {
        _server1.Dispose();
        _server2.Dispose();
    }

    [Test]
    public async Task TestPutRoutesRequestToPrimaryNode()
    {
        using var client = await GetClient();
        var recordView = (await client.Tables.GetTableAsync(FakeServer.ExistingTableName))!.GetRecordView<int>();
        var (defaultServer, secondaryServer) = GetServerPair();

        // Default server.
        await recordView.UpsertAsync(null, 1);

        Assert.AreEqual(
            new[] { ClientOp.TableGet, ClientOp.SchemasGet, ClientOp.PartitionAssignmentGet, ClientOp.TupleUpsert },
            defaultServer.ClientOps);

        CollectionAssert.IsEmpty(secondaryServer.ClientOps);

        // Second server.
        ClearOps();
        await recordView.UpsertAsync(null, 3);

        CollectionAssert.IsEmpty(defaultServer.ClientOps);
        Assert.AreEqual(new[] { ClientOp.TupleUpsert }, secondaryServer.ClientOps);
    }

    [Test]
    public async Task TestPutWithTxUsesDefaultNode()
    {
        using var client = await GetClient();
        var recordView = (await client.Tables.GetTableAsync(FakeServer.ExistingTableName))!.GetRecordView<int>();
        var tx = await client.Transactions.BeginAsync();
        var (defaultServer, secondaryServer) = GetServerPair();

        // Second server.
        await recordView.UpsertAsync(tx, 1);
        await recordView.UpsertAsync(tx, 3);

        Assert.AreEqual(
            new[] { ClientOp.TableGet, ClientOp.TxBegin, ClientOp.SchemasGet, ClientOp.TupleUpsert, ClientOp.TupleUpsert },
            defaultServer.ClientOps);

        CollectionAssert.IsEmpty(secondaryServer.ClientOps);
    }

    [Test]
    public async Task TestClientReceivesPartitionAssignmentUpdates()
    {
        using var client = await GetClient();
        var recordView = (await client.Tables.GetTableAsync(FakeServer.ExistingTableName))!.GetRecordView<int>();
        var (defaultServer, secondaryServer) = GetServerPair();

        // Default assignment.
        await recordView.UpsertAsync(null, 1);

        CollectionAssert.IsEmpty(secondaryServer.ClientOps);

        // Updated assignment.
        foreach (var server in GetServers())
        {
            server.ClearOps();
            Array.Reverse(server.PartitionAssignment);
            server.PartitionAssignmentChanged = true;
        }

        await recordView.UpsertAsync(null, 1);
        await recordView.UpsertAsync(null, 1);

        Assert.AreEqual(new[] { ClientOp.TupleUpsert }, defaultServer.ClientOps);
        Assert.AreEqual(new[] { ClientOp.TupleUpsert }, secondaryServer.ClientOps);
    }

    private async Task<IIgniteClient> GetClient()
    {
        var cfg = new IgniteClientConfiguration
        {
            Endpoints =
            {
                "127.0.0.1: " + _server1.Port,
                "127.0.0.1: " + _server2.Port
            }
        };

        return await IgniteClient.StartAsync(cfg);
    }

    private void ClearOps()
    {
        _server1.ClearOps();
        _server2.ClearOps();
    }

    private IEnumerable<FakeServer> GetServers()
    {
        yield return _server1;
        yield return _server2;
    }

    private (FakeServer Default, FakeServer Secondary) GetServerPair()
    {
        // Any server can be primary due to round-robin balancing in ClientFailoverSocket.
        return _server1.ClientOps.Count > 0 ? (_server1, _server2) : (_server2, _server1);
    }
}
