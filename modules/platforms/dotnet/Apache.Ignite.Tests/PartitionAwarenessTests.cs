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
using System.Threading.Tasks;
using Compute;
using Ignite.Compute;
using Ignite.Table;
using Internal;
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
        await AssertOpOnNode(async () => await recordView.UpsertAsync(null, 1), ClientOp.TupleUpsert, _server2, _server1);
        await AssertOpOnNode(async () => await recordView.UpsertAsync(null, 3), ClientOp.TupleUpsert, _server1, _server2);
        await AssertOpOnNode(async () => await recordView.UpsertAsync(null, 4), ClientOp.TupleUpsert, _server2, _server1);
        await AssertOpOnNode(async () => await recordView.UpsertAsync(null, 7), ClientOp.TupleUpsert, _server1, _server2);
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
        await TestClientReceivesPartitionAssignmentUpdates(view => view.UpsertAsync(null, 1), ClientOp.TupleUpsert);

    [Test]
    public async Task TestDataStreamerReceivesPartitionAssignmentUpdates() =>
        await TestClientReceivesPartitionAssignmentUpdates(
            view => view.StreamDataAsync(new[] { 1 }.ToAsyncEnumerable()),
            ClientOp.StreamerBatchSend);

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
        await AssertOpOnNode(() => recordView.StreamDataAsync(new[] { key }.ToAsyncEnumerable()), ClientOp.StreamerBatchSend, expectedNode);

        // Multi-key operations use the first key for colocation.
        var keys = new[] { key, new IgniteTuple { ["ID"] = keyId - 1 }, new IgniteTuple { ["ID"] = keyId + 1 } };
        await AssertOpOnNode(() => recordView.GetAllAsync(null, keys), ClientOp.TupleGetAll, expectedNode);
        await AssertOpOnNode(() => recordView.InsertAllAsync(null, keys), ClientOp.TupleInsertAll, expectedNode);
        await AssertOpOnNode(() => recordView.UpsertAllAsync(null, keys), ClientOp.TupleUpsertAll, expectedNode);
        await AssertOpOnNode(() => recordView.DeleteAllAsync(null, keys), ClientOp.TupleDeleteAll, expectedNode);
        await AssertOpOnNode(() => recordView.DeleteAllExactAsync(null, keys), ClientOp.TupleDeleteAllExact, expectedNode);
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
        await AssertOpOnNode(() => recordView.StreamDataAsync(new[] { key }.ToAsyncEnumerable()), ClientOp.StreamerBatchSend, expectedNode);

        // Multi-key operations use the first key for colocation.
        var keys = new[] { key, key - 1, key + 1 };
        await AssertOpOnNode(() => recordView.GetAllAsync(null, keys), ClientOp.TupleGetAll, expectedNode);
        await AssertOpOnNode(() => recordView.InsertAllAsync(null, keys), ClientOp.TupleInsertAll, expectedNode);
        await AssertOpOnNode(() => recordView.UpsertAllAsync(null, keys), ClientOp.TupleUpsertAll, expectedNode);
        await AssertOpOnNode(() => recordView.DeleteAllAsync(null, keys), ClientOp.TupleDeleteAll, expectedNode);
        await AssertOpOnNode(() => recordView.DeleteAllExactAsync(null, keys), ClientOp.TupleDeleteAllExact, expectedNode);
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

        await AssertOpOnNode(() => kvView.GetAsync(null, key), ClientOp.TupleGet, expectedNode);
        await AssertOpOnNode(() => kvView.GetAndRemoveAsync(null, key), ClientOp.TupleGetAndDelete, expectedNode);
        await AssertOpOnNode(() => kvView.GetAndReplaceAsync(null, key, val), ClientOp.TupleGetAndReplace, expectedNode);
        await AssertOpOnNode(() => kvView.GetAndPutAsync(null, key, val), ClientOp.TupleGetAndUpsert, expectedNode);
        await AssertOpOnNode(() => kvView.PutAsync(null, key, val), ClientOp.TupleUpsert, expectedNode);
        await AssertOpOnNode(() => kvView.PutIfAbsentAsync(null, key, val), ClientOp.TupleInsert, expectedNode);
        await AssertOpOnNode(() => kvView.ReplaceAsync(null, key, val), ClientOp.TupleReplace, expectedNode);
        await AssertOpOnNode(() => kvView.ReplaceAsync(null, key, val, val), ClientOp.TupleReplaceExact, expectedNode);
        await AssertOpOnNode(() => kvView.RemoveAsync(null, key), ClientOp.TupleDelete, expectedNode);
        await AssertOpOnNode(() => kvView.RemoveAsync(null, key, val), ClientOp.TupleDeleteExact, expectedNode);
        await AssertOpOnNode(() => kvView.ContainsAsync(null, key), ClientOp.TupleContainsKey, expectedNode);

        // Multi-key operations use the first key for colocation.
        var keys = new[] { key, new IgniteTuple { ["ID"] = keyId - 1 }, new IgniteTuple { ["ID"] = keyId + 1 } };
        var pairs = keys.ToDictionary(x => (IIgniteTuple)x, _ => (IIgniteTuple)val);

        await AssertOpOnNode(() => kvView.GetAllAsync(null, keys), ClientOp.TupleGetAll, expectedNode);
        await AssertOpOnNode(() => kvView.PutAllAsync(null, pairs), ClientOp.TupleUpsertAll, expectedNode);
        await AssertOpOnNode(() => kvView.RemoveAllAsync(null, keys), ClientOp.TupleDeleteAll, expectedNode);
        await AssertOpOnNode(() => kvView.RemoveAllAsync(null, pairs), ClientOp.TupleDeleteAllExact, expectedNode);
        await AssertOpOnNode(() => kvView.StreamDataAsync(pairs.ToAsyncEnumerable()), ClientOp.StreamerBatchSend, expectedNode);
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

        await AssertOpOnNode(() => kvView.GetAsync(null, key), ClientOp.TupleGet, expectedNode);
        await AssertOpOnNode(() => kvView.GetAndRemoveAsync(null, key), ClientOp.TupleGetAndDelete, expectedNode);
        await AssertOpOnNode(() => kvView.GetAndReplaceAsync(null, key, val), ClientOp.TupleGetAndReplace, expectedNode);
        await AssertOpOnNode(() => kvView.GetAndPutAsync(null, key, val), ClientOp.TupleGetAndUpsert, expectedNode);
        await AssertOpOnNode(() => kvView.PutAsync(null, key, val), ClientOp.TupleUpsert, expectedNode);
        await AssertOpOnNode(() => kvView.PutIfAbsentAsync(null, key, val), ClientOp.TupleInsert, expectedNode);
        await AssertOpOnNode(() => kvView.ReplaceAsync(null, key, val), ClientOp.TupleReplace, expectedNode);
        await AssertOpOnNode(() => kvView.ReplaceAsync(null, key, val, val), ClientOp.TupleReplaceExact, expectedNode);
        await AssertOpOnNode(() => kvView.RemoveAsync(null, key), ClientOp.TupleDelete, expectedNode);
        await AssertOpOnNode(() => kvView.RemoveAsync(null, key, val), ClientOp.TupleDeleteExact, expectedNode);
        await AssertOpOnNode(() => kvView.ContainsAsync(null, key), ClientOp.TupleContainsKey, expectedNode);
        await AssertOpOnNode(
            () => kvView.StreamDataAsync(new[] { new KeyValuePair<int, int>(key, val) }.ToAsyncEnumerable()),
            ClientOp.StreamerBatchSend,
            expectedNode);

        // Multi-key operations use the first key for colocation.
        var keys = new[] { key, key - 1, key + 1 };
        var pairs = keys.ToDictionary(x => x, _ => val);
        await AssertOpOnNode(() => kvView.GetAllAsync(null, keys), ClientOp.TupleGetAll, expectedNode);
        await AssertOpOnNode(() => kvView.PutAllAsync(null, pairs), ClientOp.TupleUpsertAll, expectedNode);
        await AssertOpOnNode(() => kvView.RemoveAllAsync(null, keys), ClientOp.TupleDeleteAll, expectedNode);
        await AssertOpOnNode(() => kvView.RemoveAllAsync(null, pairs), ClientOp.TupleDeleteAllExact, expectedNode);
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
            await AssertOpOnNode(() => view.UpsertAsync(null, new CompositeKey(idStr, idGuid)), ClientOp.TupleUpsert, node);
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
            await AssertOpOnNode(() => view.UpsertAsync(null, new CompositeKey(idStr, idGuid)), ClientOp.TupleUpsert, node);
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
            () => client.Compute.SubmitColocatedAsync<object?>(FakeServer.ExistingTableName, key, Array.Empty<DeploymentUnit>(), "job"),
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
            () => client.Compute.SubmitColocatedAsync<object?, SimpleKey>(
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
        await AssertOpOnNode(() => recordView.UpsertAsync(null, 1), ClientOp.TupleUpsert, _server2);

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
        await AssertOpOnNode(() => recordView.UpsertAsync(null, 1), ClientOp.TupleUpsert, _server2);
    }

    private static async Task AssertOpOnNode(
        Func<Task> action,
        ClientOp op,
        FakeServer node,
        FakeServer? node2 = null,
        bool allowExtraOps = false)
    {
        node.ClearOps();
        node2?.ClearOps();

        await action();

        if (allowExtraOps)
        {
            CollectionAssert.Contains(node.ClientOps, op);
        }
        else
        {
            Assert.AreEqual(new[] { op }, node.ClientOps);
        }

        if (node2 != null)
        {
            CollectionAssert.IsEmpty(node2.ClientOps);
        }
    }

    private async Task TestClientReceivesPartitionAssignmentUpdates(Func<IRecordView<int>, Task> func, ClientOp op)
    {
        using var client = await GetClient();
        var recordView = (await client.Tables.GetTableAsync(FakeServer.ExistingTableName))!.GetRecordView<int>();

        // Check default assignment.
        await recordView.UpsertAsync(null, 1);
        await AssertOpOnNode(() => func(recordView), op, _server2);

        // Update assignment.
        var assignmentTimestamp = DateTime.UtcNow.Ticks;

        foreach (var server in new[] { _server1, _server2 })
        {
            server.ClearOps();
            server.PartitionAssignment = server.PartitionAssignment.Reverse().ToArray();
            server.PartitionAssignmentTimestamp = assignmentTimestamp;
        }

        // First request receives update flag.
        await client.Tables.GetTablesAsync();

        // Second request loads and uses new assignment.
        await AssertOpOnNode(() => func(recordView), op, _server1, allowExtraOps: true);
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
