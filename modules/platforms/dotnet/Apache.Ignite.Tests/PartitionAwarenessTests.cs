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
using System.Linq;
using System.Threading.Channels;
using System.Threading.Tasks;
using Compute;
using Ignite.Compute;
using Ignite.Table;
using Ignite.Transactions;
using Internal.Proto;
using Internal.Transactions;
using NUnit.Framework;

/// <summary>
/// Tests partition awareness.
/// </summary>
public class PartitionAwarenessTests
{
    private static readonly object[] KeyNodeCases =
    {
        new object[] { 0, 1 },
        new object[] { 1, 2 },
        new object[] { 3, 1 },
        new object[] { 4, 2 },
        new object[] { 5, 2 },
        new object[] { 8, 2 },
        new object[] { int.MaxValue, 2 },
        new object[] { int.MaxValue - 1, 2 },
        new object[] { int.MinValue, 2 }
    };

    private FakeServer _server1 = null!;
    private FakeServer _server2 = null!;

    [SetUp]
    public void SetUp()
    {
        _server1 = new FakeServer(nodeName: "srv1");
        _server2 = new FakeServer(nodeName: "srv2");

        var assignment = new[] { _server1.Node.Name, _server2.Node.Name };
        var assignmentTimestamp = DateTime.UtcNow.AddDays(-1).Ticks; // Old assignment.

        foreach (var server in new[] { _server1, _server2 })
        {
            server.PartitionAssignment = assignment;
            server.PartitionAssignmentTimestamp = assignmentTimestamp;
        }
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

        // Warm up.
        await recordView.UpsertAsync(null, 1);

        // Check.
        await AssertOpOnNode(async tx => await recordView.UpsertAsync(tx, 1), ClientOp.TupleUpsert, _server2, _server1);
        await AssertOpOnNode(async tx => await recordView.UpsertAsync(tx, 3), ClientOp.TupleUpsert, _server1, _server2);
        await AssertOpOnNode(async tx => await recordView.UpsertAsync(tx, 4), ClientOp.TupleUpsert, _server2, _server1);
        await AssertOpOnNode(async tx => await recordView.UpsertAsync(tx, 7), ClientOp.TupleUpsert, _server1, _server2);
    }

    [Test]
    public async Task TestPutWithTxUsesTxNode()
    {
        using var client = await GetClient();
        var recordView = (await client.Tables.GetTableAsync(FakeServer.ExistingTableName))!.GetRecordView<int>();

        var txServer = _server1;
        txServer.ClearOps();

        var tx = await client.Transactions.BeginAsync();
        await TestUtils.ForceLazyTxStart(tx, client, PreferredNode.FromName(_server1.Node.Name));

        Assert.AreEqual(ClientOp.TxBegin, txServer.ClientOps.Single());

        for (int i = 0; i < 10; i++)
        {
            await recordView.UpsertAsync(tx, i);
        }

        Assert.AreEqual(Enumerable.Repeat(ClientOp.TupleUpsert, 10), txServer.ClientOps.TakeLast(10));
    }

    [Test]
    public async Task TestClientReceivesPartitionAssignmentUpdates() =>
        await TestClientReceivesPartitionAssignmentUpdates((view, tx) => view.UpsertAsync(tx, 1), ClientOp.TupleUpsert);

    [Test]
    public async Task TestDataStreamerReceivesPartitionAssignmentUpdates() =>
        await TestClientReceivesPartitionAssignmentUpdates(
            (view, _) => view.StreamDataAsync(new[] { 1 }.ToAsyncEnumerable()),
            ClientOp.StreamerBatchSend);

    [Test]
    public async Task TestDataStreamerWithReceiverReceivesPartitionAssignmentUpdates() =>
        await TestClientReceivesPartitionAssignmentUpdates(
            (view, _) => view.StreamDataAsync(
                new[] { 1 }.ToAsyncEnumerable(),
                keySelector: x => x,
                payloadSelector: x => x.ToString(),
                units: Array.Empty<DeploymentUnit>(),
                receiverClassName: "x"),
            ClientOp.StreamerWithReceiverBatchSend);

    [Test]
    public async Task TestDataStreamerReceivesPartitionAssignmentUpdatesWhileStreaming([Values(true, false)] bool withReceiver)
    {
        var clientOp = withReceiver ? ClientOp.StreamerWithReceiverBatchSend : ClientOp.StreamerBatchSend;
        var producer = Channel.CreateUnbounded<int>();

        using var client = await GetClient();
        var recordView = (await client.Tables.GetTableAsync(FakeServer.ExistingTableName))!.GetRecordView<int>();

        var options = new DataStreamerOptions { PageSize = 1 };
        var data = producer.Reader.ReadAllAsync();
        var streamerTask = withReceiver
            ? recordView.StreamDataAsync(data, x => x, x => x.ToString(), Array.Empty<DeploymentUnit>(), "x", null, options)
            : recordView.StreamDataAsync(data, options);

        Func<ITransaction?, Task> action = async _ =>
        {
             await producer.Writer.WriteAsync(1);
             TestUtils.WaitForCondition(
                 () => new[] { _server1, _server2 }.SelectMany(x => x.ClientOps).Contains(clientOp));
        };

        // Check default assignment.
        await recordView.UpsertAsync(null, 1);
        await AssertOpOnNode(action, clientOp, _server2);

        // Update assignment - first request receives update flag.
        ReversePartitionAssignment();
        await client.Tables.GetTablesAsync();

        // Second request loads and uses new assignment.
        await AssertOpOnNode(action, clientOp, _server1, allowExtraOps: true);

        // End streaming.
        producer.Writer.Complete();
        await streamerTask;
    }

    [Test]
    [TestCaseSource(nameof(KeyNodeCases))]
    public async Task TestAllRecordBinaryViewOperations(int keyId, int node)
    {
        using var client = await GetClient();
        var recordView = (await client.Tables.GetTableAsync(FakeServer.ExistingTableName))!.RecordBinaryView;

        // Warm up (retrieve assignment).
        var key = new IgniteTuple { ["ID"] = keyId };
        await recordView.UpsertAsync(null, key);

        // Single-key operations.
        var expectedNode = node == 1 ? _server1 : _server2;

        await AssertOpOnNode(tx => recordView.GetAsync(tx, key), ClientOp.TupleGet, expectedNode);
        await AssertOpOnNode(tx => recordView.GetAndDeleteAsync(tx, key), ClientOp.TupleGetAndDelete, expectedNode);
        await AssertOpOnNode(tx => recordView.GetAndReplaceAsync(tx, key), ClientOp.TupleGetAndReplace, expectedNode);
        await AssertOpOnNode(tx => recordView.GetAndUpsertAsync(tx, key), ClientOp.TupleGetAndUpsert, expectedNode);
        await AssertOpOnNode(tx => recordView.UpsertAsync(tx, key), ClientOp.TupleUpsert, expectedNode);
        await AssertOpOnNode(tx => recordView.InsertAsync(tx, key), ClientOp.TupleInsert, expectedNode);
        await AssertOpOnNode(tx => recordView.ReplaceAsync(tx, key), ClientOp.TupleReplace, expectedNode);
        await AssertOpOnNode(tx => recordView.ReplaceAsync(tx, key, key), ClientOp.TupleReplaceExact, expectedNode);
        await AssertOpOnNode(tx => recordView.DeleteAsync(tx, key), ClientOp.TupleDelete, expectedNode);
        await AssertOpOnNode(tx => recordView.DeleteExactAsync(tx, key), ClientOp.TupleDeleteExact, expectedNode);
        await AssertOpOnNode(_ => recordView.StreamDataAsync(new[] { key }.ToAsyncEnumerable()), ClientOp.StreamerBatchSend, expectedNode);

        // Multi-key operations use the first key for colocation.
        var keys = new[] { key, new IgniteTuple { ["ID"] = keyId - 1 }, new IgniteTuple { ["ID"] = keyId + 1 } };
        await AssertOpOnNode(tx => recordView.GetAllAsync(tx, keys), ClientOp.TupleGetAll, expectedNode);
        await AssertOpOnNode(tx => recordView.InsertAllAsync(tx, keys), ClientOp.TupleInsertAll, expectedNode);
        await AssertOpOnNode(tx => recordView.UpsertAllAsync(tx, keys), ClientOp.TupleUpsertAll, expectedNode);
        await AssertOpOnNode(tx => recordView.DeleteAllAsync(tx, keys), ClientOp.TupleDeleteAll, expectedNode);
        await AssertOpOnNode(tx => recordView.DeleteAllExactAsync(tx, keys), ClientOp.TupleDeleteAllExact, expectedNode);
    }

    [Test]
    [TestCaseSource(nameof(KeyNodeCases))]
    public async Task TestAllRecordViewOperations(int key, int node)
    {
        using var client = await GetClient();
        var recordView = (await client.Tables.GetTableAsync(FakeServer.ExistingTableName))!.GetRecordView<int>();

        // Warm up (retrieve assignment).
        await recordView.UpsertAsync(null, 1);

        // Single-key operations.
        var expectedNode = node == 1 ? _server1 : _server2;

        await AssertOpOnNode(tx => recordView.GetAsync(tx, key), ClientOp.TupleGet, expectedNode);
        await AssertOpOnNode(tx => recordView.GetAndDeleteAsync(tx, key), ClientOp.TupleGetAndDelete, expectedNode);
        await AssertOpOnNode(tx => recordView.GetAndReplaceAsync(tx, key), ClientOp.TupleGetAndReplace, expectedNode);
        await AssertOpOnNode(tx => recordView.GetAndUpsertAsync(tx, key), ClientOp.TupleGetAndUpsert, expectedNode);
        await AssertOpOnNode(tx => recordView.UpsertAsync(tx, key), ClientOp.TupleUpsert, expectedNode);
        await AssertOpOnNode(tx => recordView.InsertAsync(tx, key), ClientOp.TupleInsert, expectedNode);
        await AssertOpOnNode(tx => recordView.ReplaceAsync(tx, key), ClientOp.TupleReplace, expectedNode);
        await AssertOpOnNode(tx => recordView.ReplaceAsync(tx, key, key), ClientOp.TupleReplaceExact, expectedNode);
        await AssertOpOnNode(tx => recordView.DeleteAsync(tx, key), ClientOp.TupleDelete, expectedNode);
        await AssertOpOnNode(tx => recordView.DeleteExactAsync(tx, key), ClientOp.TupleDeleteExact, expectedNode);
        await AssertOpOnNode(_ => recordView.StreamDataAsync(new[] { key }.ToAsyncEnumerable()), ClientOp.StreamerBatchSend, expectedNode);

        // Multi-key operations use the first key for colocation.
        var keys = new[] { key, key - 1, key + 1 };
        await AssertOpOnNode(tx => recordView.GetAllAsync(tx, keys), ClientOp.TupleGetAll, expectedNode);
        await AssertOpOnNode(tx => recordView.InsertAllAsync(tx, keys), ClientOp.TupleInsertAll, expectedNode);
        await AssertOpOnNode(tx => recordView.UpsertAllAsync(tx, keys), ClientOp.TupleUpsertAll, expectedNode);
        await AssertOpOnNode(tx => recordView.DeleteAllAsync(tx, keys), ClientOp.TupleDeleteAll, expectedNode);
        await AssertOpOnNode(tx => recordView.DeleteAllExactAsync(tx, keys), ClientOp.TupleDeleteAllExact, expectedNode);
    }

    [Test]
    [TestCaseSource(nameof(KeyNodeCases))]
    public async Task TestAllKeyValueBinaryViewOperations(int keyId, int node)
    {
        using var client = await GetClient();
        var kvView = (await client.Tables.GetTableAsync(FakeServer.ExistingTableName))!.KeyValueBinaryView;

        // Warm up (retrieve assignment).
        var key = new IgniteTuple { ["ID"] = keyId };
        var val = new IgniteTuple();
        await kvView.PutAsync(null, key, val);

        // Single-key operations.
        var expectedNode = node == 1 ? _server1 : _server2;

        await AssertOpOnNode(tx => kvView.GetAsync(tx, key), ClientOp.TupleGet, expectedNode);
        await AssertOpOnNode(tx => kvView.GetAndRemoveAsync(tx, key), ClientOp.TupleGetAndDelete, expectedNode);
        await AssertOpOnNode(tx => kvView.GetAndReplaceAsync(tx, key, val), ClientOp.TupleGetAndReplace, expectedNode);
        await AssertOpOnNode(tx => kvView.GetAndPutAsync(tx, key, val), ClientOp.TupleGetAndUpsert, expectedNode);
        await AssertOpOnNode(tx => kvView.PutAsync(tx, key, val), ClientOp.TupleUpsert, expectedNode);
        await AssertOpOnNode(tx => kvView.PutIfAbsentAsync(tx, key, val), ClientOp.TupleInsert, expectedNode);
        await AssertOpOnNode(tx => kvView.ReplaceAsync(tx, key, val), ClientOp.TupleReplace, expectedNode);
        await AssertOpOnNode(tx => kvView.ReplaceAsync(tx, key, val, val), ClientOp.TupleReplaceExact, expectedNode);
        await AssertOpOnNode(tx => kvView.RemoveAsync(tx, key), ClientOp.TupleDelete, expectedNode);
        await AssertOpOnNode(tx => kvView.RemoveAsync(tx, key, val), ClientOp.TupleDeleteExact, expectedNode);
        await AssertOpOnNode(tx => kvView.ContainsAsync(tx, key), ClientOp.TupleContainsKey, expectedNode);

        // Multi-key operations use the first key for colocation.
        var keys = new[] { key, new IgniteTuple { ["ID"] = keyId - 1 }, new IgniteTuple { ["ID"] = keyId + 1 } };
        var pairs = keys.ToDictionary(x => (IIgniteTuple)x, _ => (IIgniteTuple)val);

        await AssertOpOnNode(tx => kvView.GetAllAsync(tx, keys), ClientOp.TupleGetAll, expectedNode);
        await AssertOpOnNode(tx => kvView.PutAllAsync(tx, pairs), ClientOp.TupleUpsertAll, expectedNode);
        await AssertOpOnNode(tx => kvView.RemoveAllAsync(tx, keys), ClientOp.TupleDeleteAll, expectedNode);
        await AssertOpOnNode(tx => kvView.RemoveAllAsync(tx, pairs), ClientOp.TupleDeleteAllExact, expectedNode);
        await AssertOpOnNode(_ => kvView.StreamDataAsync(pairs.ToAsyncEnumerable()), ClientOp.StreamerBatchSend, expectedNode);
    }

    [Test]
    [TestCaseSource(nameof(KeyNodeCases))]
    public async Task TestAllKeyValueViewOperations(int key, int node)
    {
        using var client = await GetClient();
        var kvView = (await client.Tables.GetTableAsync(FakeServer.ExistingTableName))!.GetKeyValueView<int, int>();

        // Warm up (retrieve assignment).
        var val = 0;
        await kvView.PutAsync(null, 1, val);

        // Single-key operations.
        var expectedNode = node == 1 ? _server1 : _server2;

        await AssertOpOnNode(tx => kvView.GetAsync(tx, key), ClientOp.TupleGet, expectedNode);
        await AssertOpOnNode(tx => kvView.GetAndRemoveAsync(tx, key), ClientOp.TupleGetAndDelete, expectedNode);
        await AssertOpOnNode(tx => kvView.GetAndReplaceAsync(tx, key, val), ClientOp.TupleGetAndReplace, expectedNode);
        await AssertOpOnNode(tx => kvView.GetAndPutAsync(tx, key, val), ClientOp.TupleGetAndUpsert, expectedNode);
        await AssertOpOnNode(tx => kvView.PutAsync(tx, key, val), ClientOp.TupleUpsert, expectedNode);
        await AssertOpOnNode(tx => kvView.PutIfAbsentAsync(tx, key, val), ClientOp.TupleInsert, expectedNode);
        await AssertOpOnNode(tx => kvView.ReplaceAsync(tx, key, val), ClientOp.TupleReplace, expectedNode);
        await AssertOpOnNode(tx => kvView.ReplaceAsync(tx, key, val, val), ClientOp.TupleReplaceExact, expectedNode);
        await AssertOpOnNode(tx => kvView.RemoveAsync(tx, key), ClientOp.TupleDelete, expectedNode);
        await AssertOpOnNode(tx => kvView.RemoveAsync(tx, key, val), ClientOp.TupleDeleteExact, expectedNode);
        await AssertOpOnNode(tx => kvView.ContainsAsync(tx, key), ClientOp.TupleContainsKey, expectedNode);
        await AssertOpOnNode(
            _ => kvView.StreamDataAsync(new[] { new KeyValuePair<int, int>(key, val) }.ToAsyncEnumerable()),
            ClientOp.StreamerBatchSend,
            expectedNode);

        // Multi-key operations use the first key for colocation.
        var keys = new[] { key, key - 1, key + 1 };
        var pairs = keys.ToDictionary(x => x, _ => val);
        await AssertOpOnNode(tx => kvView.GetAllAsync(tx, keys), ClientOp.TupleGetAll, expectedNode);
        await AssertOpOnNode(tx => kvView.PutAllAsync(tx, pairs), ClientOp.TupleUpsertAll, expectedNode);
        await AssertOpOnNode(tx => kvView.RemoveAllAsync(tx, keys), ClientOp.TupleDeleteAll, expectedNode);
        await AssertOpOnNode(tx => kvView.RemoveAllAsync(tx, pairs), ClientOp.TupleDeleteAllExact, expectedNode);
    }

    [Test]
    public async Task TestCompositeKey()
    {
        using var client = await GetClient();
        var view = (await client.Tables.GetTableAsync(FakeServer.CompositeKeyTableName))!.GetRecordView<CompositeKey>();

        await view.UpsertAsync(null, new CompositeKey("1", Guid.Empty)); // Warm up.

        await Test("1", Guid.Empty, _server1);
        await Test("1", Guid.Parse("b0000000-0000-0000-0000-000000000000"), _server2);

        await Test("c", Guid.Empty, _server2);
        await Test("c", Guid.Parse("b0000000-0000-0000-0000-000000000000"), _server1);

        async Task Test(string idStr, Guid idGuid, FakeServer node) =>
            await AssertOpOnNode(tx => view.UpsertAsync(tx, new CompositeKey(idStr, idGuid)), ClientOp.TupleUpsert, node);
    }

    [Test]
    public async Task TestCustomColocationKey()
    {
        using var client = await GetClient();
        var view = (await client.Tables.GetTableAsync(FakeServer.CustomColocationKeyTableName))!.GetRecordView<CompositeKey>();

        // Warm up.
        await view.UpsertAsync(null, new CompositeKey("1", Guid.Empty));

        // Both columns are part of key, but only string column is colocation key, so random Guid does not affect the hash.
        await Test("1", Guid.NewGuid(), _server2);
        await Test("c", Guid.NewGuid(), _server1);

        async Task Test(string idStr, Guid idGuid, FakeServer node) =>
            await AssertOpOnNode(tx => view.UpsertAsync(tx, new CompositeKey(idStr, idGuid)), ClientOp.TupleUpsert, node);
    }

    [Test]
    [TestCaseSource(nameof(KeyNodeCases))]
    public async Task TestExecuteColocatedTupleKeyRoutesRequestToPrimaryNode(int keyId, int node)
    {
        using var client = await GetClient();
        var expectedNode = node == 1 ? _server1 : _server2;
        var key = new IgniteTuple { ["ID"] = keyId };

        // Warm up.
        await client.Compute.SubmitColocatedAsync<object?>(FakeServer.ExistingTableName, key, Array.Empty<DeploymentUnit>(), "job");

        await AssertOpOnNode(
            _ => client.Compute.SubmitColocatedAsync<object?>(FakeServer.ExistingTableName, key, Array.Empty<DeploymentUnit>(), "job"),
            ClientOp.ComputeExecuteColocated,
            expectedNode);
    }

    [Test]
    [TestCaseSource(nameof(KeyNodeCases))]
    public async Task TestExecuteColocatedObjectKeyRoutesRequestToPrimaryNode(int keyId, int node)
    {
        using var client = await GetClient();
        var expectedNode = node == 1 ? _server1 : _server2;
        var key = new SimpleKey(keyId);

        // Warm up.
        await client.Compute.SubmitColocatedAsync<object?, SimpleKey>(
            FakeServer.ExistingTableName, key, Array.Empty<DeploymentUnit>(), "job");

        await AssertOpOnNode(
            _ => client.Compute.SubmitColocatedAsync<object?, SimpleKey>(
                FakeServer.ExistingTableName, key, Array.Empty<DeploymentUnit>(), "job"),
            ClientOp.ComputeExecuteColocated,
            expectedNode);
    }

    [Test]
    public async Task TestOldAssignmentIsIgnored()
    {
        using var client = await GetClient();
        var recordView = (await client.Tables.GetTableAsync(FakeServer.ExistingTableName))!.GetRecordView<int>();

        // Check default assignment.
        await recordView.UpsertAsync(null, 1);
        await AssertOpOnNode(tx => recordView.UpsertAsync(tx, 1), ClientOp.TupleUpsert, _server2);

        // One server has old assignment
        _server1.PartitionAssignment = _server1.PartitionAssignment.Reverse().ToArray();
        _server1.PartitionAssignmentTimestamp -= 1000;

        // Multiple requests to receive timestamp from all servers.
        for (int i = 0; i < 10; i++)
        {
            await client.Tables.GetTablesAsync();
        }

        // Check that assignment has not changed - update with old timestamp was ignored.
        _server1.ClearOps();
        _server2.ClearOps();

        await recordView.UpsertAsync(null, 1);
        await AssertOpOnNode(tx => recordView.UpsertAsync(tx, 1), ClientOp.TupleUpsert, _server2);
    }

    private static async Task AssertOpOnNode(
        Func<ITransaction?, Task> action,
        ClientOp op,
        FakeServer node,
        FakeServer? node2 = null,
        bool allowExtraOps = false)
    {
        await AssertOpOnNodeInner(action, op, node, node2, allowExtraOps, withTx: false);

        if (op != ClientOp.StreamerBatchSend && op != ClientOp.ComputeExecuteColocated && op != ClientOp.StreamerWithReceiverBatchSend)
        {
            await AssertOpOnNodeInner(action, op, node, node2, allowExtraOps, withTx: true);
        }
    }

    private static async Task AssertOpOnNodeInner(
        Func<ITransaction?, Task> action,
        ClientOp op,
        FakeServer node,
        FakeServer? node2 = null,
        bool allowExtraOps = false,
        bool withTx = false)
    {
        node.ClearOps();
        node2?.ClearOps();

        ITransaction? tx = withTx ? new LazyTransaction(default) : null;

        await action(tx);

        if (allowExtraOps)
        {
            CollectionAssert.Contains(node.ClientOps, op);
        }
        else
        {
            if (withTx)
            {
                Assert.AreEqual(new[] { ClientOp.TxBegin, op }, node.ClientOps);
            }
            else
            {
                Assert.AreEqual(new[] { op }, node.ClientOps);
            }
        }

        if (node2 != null)
        {
            CollectionAssert.IsEmpty(node2.ClientOps);
        }
    }

    private async Task TestClientReceivesPartitionAssignmentUpdates(Func<IRecordView<int>, ITransaction?, Task> func, ClientOp op)
    {
        using var client = await GetClient();
        var recordView = (await client.Tables.GetTableAsync(FakeServer.ExistingTableName))!.GetRecordView<int>();

        // Check default assignment.
        await recordView.UpsertAsync(null, 1);
        await AssertOpOnNode(tx => func(recordView, tx), op, _server2);

        // Update assignment - first request receives update flag.
        ReversePartitionAssignment();
        await client.Tables.GetTablesAsync();

        // Second request loads and uses new assignment.
        await AssertOpOnNode(tx => func(recordView, tx), op, _server1, allowExtraOps: true);
    }

    private void ReversePartitionAssignment()
    {
        var assignmentTimestamp = DateTime.UtcNow.Ticks;

        foreach (var server in new[] { _server1, _server2 })
        {
            server.ClearOps();
            server.PartitionAssignment = server.PartitionAssignment.Reverse().ToArray();
            server.PartitionAssignmentTimestamp = assignmentTimestamp;
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

        var client = await IgniteClient.StartAsync(cfg);
        client.WaitForConnections(2);

        return client;
    }

    // ReSharper disable NotAccessedPositionalProperty.Local
    private record CompositeKey(string IdStr, Guid IdGuid);

    private record SimpleKey(int Id);
}
