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
using System.Linq;
using System.Threading.Tasks;
using Internal.Proto;
using NUnit.Framework;

/// <summary>
/// Tests partition awareness.
/// TODO:
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
        var (defaultServer, _) = GetServerPair();

        // Warm up.
        await recordView.UpsertAsync(null, 1);

        Assert.AreEqual(
            new[] { ClientOp.TableGet, ClientOp.SchemasGet, ClientOp.PartitionAssignmentGet },
            defaultServer.ClientOps.Take(3));

        // Check.
        await AssertOpOnNode(async () => await recordView.UpsertAsync(null, 1), ClientOp.TupleUpsert, _server2, _server1);
        await AssertOpOnNode(async () => await recordView.UpsertAsync(null, 3), ClientOp.TupleUpsert, _server1, _server2);
        await AssertOpOnNode(async () => await recordView.UpsertAsync(null, 4), ClientOp.TupleUpsert, _server2, _server1);
        await AssertOpOnNode(async () => await recordView.UpsertAsync(null, 5), ClientOp.TupleUpsert, _server1, _server2);
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

        // Check default assignment.
        await recordView.UpsertAsync(null, 1);
        CollectionAssert.IsEmpty(secondaryServer.ClientOps);

        // Update assignment.
        foreach (var server in new[] { defaultServer, secondaryServer })
        {
            server.ClearOps();
            server.PartitionAssignment = server.PartitionAssignment.Reverse().ToArray();
            server.PartitionAssignmentChanged = true;
        }

        await recordView.UpsertAsync(null, 1);
        await recordView.UpsertAsync(null, 1);

        Assert.AreEqual(
            new[] { ClientOp.TupleUpsert, ClientOp.PartitionAssignmentGet },
            defaultServer.ClientOps,
            "First request uses old assignment and receives update flag.");

        Assert.AreEqual(
            new[] { ClientOp.TupleUpsert },
            secondaryServer.ClientOps,
            "Second request uses new assignment.");
    }

    [Test]
    public async Task TestAllRecordBinaryViewOperations()
    {
        // TODO
        await Task.Delay(1);
    }

    [Test]
    [TestCase(1, 2)]
    [TestCase(4, 2)]
    [TestCase(3, 1)]
    [TestCase(5, 1)]
    public async Task TestAllRecordViewOperations(int key, int node)
    {
        using var client = await GetClient();
        var recordView = (await client.Tables.GetTableAsync(FakeServer.ExistingTableName))!.GetRecordView<int>();

        // Warm up.
        await recordView.UpsertAsync(null, 1);

        // Single-key operations.
        var expectedNode = node == 1 ? _server1 : _server2;

        await AssertOpOnNode(() => recordView.GetAsync(null, key), ClientOp.TupleGet, expectedNode);
        await AssertOpOnNode(() => recordView.GetAndDeleteAsync(null, key), ClientOp.TupleGetAndDelete, expectedNode);
        await AssertOpOnNode(() => recordView.GetAndReplaceAsync(null, key), ClientOp.TupleGetAndReplace, expectedNode);
        await AssertOpOnNode(() => recordView.GetAndUpsertAsync(null, key), ClientOp.TupleGetAndUpsert, expectedNode);
        await AssertOpOnNode(() => recordView.UpsertAsync(null, key), ClientOp.TupleUpsert, expectedNode);
        await AssertOpOnNode(() => recordView.InsertAsync(null, key), ClientOp.TupleInsert, expectedNode);
        await AssertOpOnNode(() => recordView.ReplaceAsync(null, key), ClientOp.TupleReplace, expectedNode);
        await AssertOpOnNode(() => recordView.ReplaceAsync(null, key, key), ClientOp.TupleReplaceExact, expectedNode);
        await AssertOpOnNode(() => recordView.DeleteAsync(null, key), ClientOp.TupleDelete, expectedNode);
        await AssertOpOnNode(() => recordView.DeleteExactAsync(null, key), ClientOp.TupleDeleteExact, expectedNode);

        // Multi-key operations use the first key for colocation.
        var keys = new[] { key, key - 1, key + 1 };
        await AssertOpOnNode(() => recordView.GetAllAsync(null, keys), ClientOp.TupleGetAll, expectedNode);
        await AssertOpOnNode(() => recordView.InsertAllAsync(null, keys), ClientOp.TupleInsertAll, expectedNode);
        await AssertOpOnNode(() => recordView.UpsertAllAsync(null, keys), ClientOp.TupleUpsertAll, expectedNode);
        await AssertOpOnNode(() => recordView.DeleteAllAsync(null, keys), ClientOp.TupleDeleteAll, expectedNode);
        await AssertOpOnNode(() => recordView.DeleteAllExactAsync(null, keys), ClientOp.TupleDeleteAllExact, expectedNode);
    }

    [Test]
    public async Task TestAllKeyValueBinaryViewOperations()
    {
        // TODO
        await Task.Delay(1);
    }

    [Test]
    public async Task TestAllKeyValueViewOperations()
    {
        // TODO
        await Task.Delay(1);
    }

    private static async Task AssertOpOnNode(Func<Task> action, ClientOp op, FakeServer node, FakeServer? node2 = null)
    {
        node.ClearOps();
        node2?.ClearOps();

        await action();

        Assert.AreEqual(new[] { op }, node.ClientOps);

        if (node2 != null)
        {
            CollectionAssert.IsEmpty(node2.ClientOps);
        }
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

    private (FakeServer Default, FakeServer Secondary) GetServerPair()
    {
        // Any server can be primary due to round-robin balancing in ClientFailoverSocket.
        return _server1.ClientOps.Count > 0 ? (_server1, _server2) : (_server2, _server1);
    }
}
