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

using System.Threading.Tasks;
using Internal.Proto;
using NUnit.Framework;

/// <summary>
/// Tests partition awareness.
/// TODO:
/// * Put/get routing - use FakeServer
/// * testNonNullTxDisablesPartitionAwareness
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

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _server1 = new FakeServer(nodeName: "srv1");
        _server2 = new FakeServer(nodeName: "srv2");

        var assignment = new[] { _server1.Node.Id, _server2.Node.Id };
        _server1.PartitionAssignment = assignment;
        _server2.PartitionAssignment = assignment;
    }

    [OneTimeTearDown]
    public void OneTimeTearDown()
    {
        _server1.Dispose();
        _server2.Dispose();
    }

    [TearDown]
    public void ClearOps()
    {
        _server1.ClearOps();
        _server2.ClearOps();
    }

    [Test]
    public async Task TestPutRoutesRequestToPrimaryNode()
    {
        using var client = await GetClient();
        var table = await client.Tables.GetTableAsync(FakeServer.ExistingTableName);
        var recordView = table!.GetRecordView<int>();

        await recordView.UpsertAsync(null, 1);

        CollectionAssert.IsEmpty(_server1.ClientOps);
        Assert.AreEqual(
            new[] { ClientOp.TableGet, ClientOp.SchemasGet, ClientOp.PartitionAssignmentGet, ClientOp.TupleUpsert },
            _server2.ClientOps);

        ClearOps();
        await recordView.UpsertAsync(null, 3);

        Assert.AreEqual(new[] { ClientOp.TupleUpsert }, _server1.ClientOps);
        CollectionAssert.IsEmpty(_server2.ClientOps);
    }

    private async Task<IIgniteClient> GetClient()
    {
        var cfg = new IgniteClientConfiguration
        {
            Endpoints =
            {
                "127.0.0.1: " + _server1!.Port,
                "127.0.0.1: " + _server2!.Port
            }
        };

        return await IgniteClient.StartAsync(cfg);
    }
}
