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
        // TODO: Servers should return correct distribution somehow.
        using var server1 = new FakeServer();
        using var server2 = new FakeServer();

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
        var recordView = table!.GetRecordView<long>();

        await recordView.UpsertAsync(null, 1L);
    }
}
