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
    [Test]
    public async Task TestPutRoutesRequestToPrimaryNode()
    {
        using var server1 = new FakeServer();
        using var server2 = new FakeServer();

        var assignment = new[] { server1.Node.Id, server2.Node.Id };
        server1.PartitionAssignment = assignment;
        server2.PartitionAssignment = assignment;

        var cfg = new IgniteClientConfiguration
        {
            Endpoints =
            {
                "127.0.0.1: " + server1.Port,
                "127.0.0.1: " + server2.Port
            }
        };

        using var client = await IgniteClient.StartAsync(cfg);
        var table = await client.Tables.GetTableAsync(FakeServer.ExistingTableName);
        var recordView = table!.GetRecordView<int>();

        await recordView.UpsertAsync(null, 1);

        Assert.AreEqual(new[] { ClientOp.TupleUpsert }, server1.ClientOps);
        Assert.AreEqual(new[] { ClientOp.TableGet, ClientOp.SchemasGet, ClientOp.TupleUpsert }, server2.ClientOps);
    }
}
