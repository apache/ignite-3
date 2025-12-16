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
using System.Threading.Tasks;
using Common;
using Common.Compute;
using Compute;
using Ignite.Compute;
using Ignite.Table;
using Internal.Proto;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using static Common.Table.TestTables;

/// <summary>
/// Tests partition awareness in real cluster.
/// </summary>
public class PartitionAwarenessRealClusterTests : IgniteTestsBase
{
    /// <summary>
    /// Uses <see cref="ComputeTests.NodeNameJob"/> to get the name of the node that should be the primary for the given key,
    /// and compares to the actual node that received the request (using IgniteProxy).
    /// </summary>
    /// <param name="withTx">Whether to use transactions.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task TestPutRoutesRequestToPrimaryNode([Values(true, false)] bool withTx)
    {
        using var loggerFactory = new ConsoleLogger(LogLevel.Trace);
        var proxies = GetProxies();
        using var client = await IgniteClient.StartAsync(GetConfig(proxies, loggerFactory));
        var recordView = (await client.Tables.GetTableAsync(TableName))!.RecordBinaryView;

        client.WaitForConnections(proxies.Count);

        // Warm up.
        await recordView.GetAsync(null, new IgniteTuple { ["KEY"] = 1L });

        // Check.
        for (long key = 0; key < 50; key++)
        {
            var keyTuple = new IgniteTuple { ["KEY"] = key };

            var primaryNodeNameExec = await client.Compute.SubmitAsync(
                JobTarget.Colocated(TableName, keyTuple),
                JavaJobs.NodeNameJob,
                null);

            var primaryNodeName = await primaryNodeNameExec.GetResultAsync();

            if (primaryNodeName.EndsWith("_3", StringComparison.Ordinal) || primaryNodeName.EndsWith("_4", StringComparison.Ordinal))
            {
                // Skip nodes without direct client connection.
                continue;
            }

            var tx = withTx ? await client.Transactions.BeginAsync() : null;

            try
            {
                await recordView.UpsertAsync(tx, keyTuple);
                var requestTargetNodeName = GetRequestTargetNodeName(proxies, ClientOp.TupleUpsert);

                Assert.AreEqual(primaryNodeName, requestTargetNodeName);
            }
            finally
            {
                if (tx != null)
                {
                    await tx.RollbackAsync();
                }
            }
        }
    }
}
