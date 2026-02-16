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
using Ignite.Compute;
using Ignite.Table;
using Internal.Proto;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using static Common.Table.TestTables;

/// <summary>
/// Tests partition awareness in a real cluster.
/// </summary>
public class PartitionAwarenessRealClusterTests : IgniteTestsBase
{
    private const int Iterations = 50;

    /// <summary>
    /// Uses <see cref="JavaJobs.NodeNameJob"/> to get the name of the node that should be the primary for the given key,
    /// and compares to the actual node that received the request (using IgniteProxy).
    /// </summary>
    /// <param name="withTx">Whether to use transactions.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task TestPutRoutesRequestToPrimaryNode([Values(true, false)] bool withTx)
    {
        await TestRequestRouting(
            TableName,
            id => new IgniteTuple { ["KEY"] = id },
            async (client, view, tuple) =>
            {
                await using var tx = withTx ? await client.Transactions.BeginAsync() : null;
                await view.UpsertAsync(tx, tuple);
            },
            ClientOp.TupleUpsert);
    }

    [Test]
    [TestCase("SELECT * FROM TBL1 WHERE KEY = ?")]
    [TestCase("SELECT * FROM TBL1 WHERE 1 = 1 AND KEY = ?")]
    [TestCase("SELECT * FROM TBL1 WHERE VAL IS NOT NULL AND KEY = ? AND 2 = 2")]
    public async Task TestSqlSimpleKey(string query)
    {
        await TestRequestRouting(
            TableName,
            id => new IgniteTuple { ["KEY"] = id },
            async (client, _, tuple) =>
            {
                await using var resultSet = await client.Sql.ExecuteAsync(
                    transaction: null,
                    statement: query,
                    tuple[KeyCol]);
            },
            ClientOp.SqlExec);
    }

    private static async Task<string> GetPrimaryNodeNameWithJavaJob(IIgniteClient client, string tableName, IIgniteTuple tuple)
    {
        var primaryNodeNameExec = await client.Compute.SubmitAsync(
            JobTarget.Colocated(tableName, tuple),
            JavaJobs.NodeNameJob,
            null);

        return await primaryNodeNameExec.GetResultAsync();
    }

    private async Task TestRequestRouting(
        string tableName,
        Func<long, IIgniteTuple> tupleFactory,
        Func<IIgniteClient, IRecordView<IIgniteTuple>, IIgniteTuple, Task> operation,
        ClientOp expectedOp)
    {
        using var loggerFactory = new ConsoleLogger(LogLevel.Trace);
        var proxies = GetProxies();
        using var client = await IgniteClient.StartAsync(GetConfig(proxies, loggerFactory));
        var recordView = (await client.Tables.GetTableAsync(tableName))!.RecordBinaryView;

        client.WaitForConnections(proxies.Count);

        // Warm up.
        await recordView.GetAsync(null, new IgniteTuple { [KeyCol] = 1L });

        // Check.
        for (long key = 0; key < Iterations; key++)
        {
            var tuple = tupleFactory(key);
            var primaryNodeName = await GetPrimaryNodeNameWithJavaJob(client, tableName, tuple);

            if (primaryNodeName.EndsWith("_3", StringComparison.Ordinal) || primaryNodeName.EndsWith("_4", StringComparison.Ordinal))
            {
                // Skip nodes without direct client connection.
                continue;
            }

            await operation(client, recordView, tuple);

            var requestTargetNodeName = GetRequestTargetNodeName(proxies, expectedOp);

            Assert.AreEqual(primaryNodeName, requestTargetNodeName);
        }
    }
}
