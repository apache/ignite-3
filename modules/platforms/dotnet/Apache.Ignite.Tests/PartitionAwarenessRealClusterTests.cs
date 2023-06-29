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
/// Tests partition awareness in real cluster.
/// </summary>
public class PartitionAwarenessRealClusterTests : IgniteTestsBase
{
    // TODO:
    // 1. Use Proxy to check actual routing.
    // 2. Use Compute to get the primary node for the given key.
    [Test]
    public async Task TestPutRoutesRequestToPrimaryNode()
    {
        var proxies = GetProxies();
        using var client = await IgniteClient.StartAsync(GetConfig(proxies));
        var recordView = (await client.Tables.GetTableAsync(FakeServer.ExistingTableName))!.GetRecordView<int>();

        // Warm up.
        await recordView.UpsertAsync(null, 1);

        // Check.
        // await AssertOpOnNode(async () => await recordView.UpsertAsync(null, 1), ClientOp.TupleUpsert, _server2, _server1);
        // await AssertOpOnNode(async () => await recordView.UpsertAsync(null, 3), ClientOp.TupleUpsert, _server1, _server2);
        // await AssertOpOnNode(async () => await recordView.UpsertAsync(null, 4), ClientOp.TupleUpsert, _server2, _server1);
        // await AssertOpOnNode(async () => await recordView.UpsertAsync(null, 5), ClientOp.TupleUpsert, _server1, _server2);
    }
}
