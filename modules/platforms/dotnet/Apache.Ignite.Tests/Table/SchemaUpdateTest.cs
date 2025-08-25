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

namespace Apache.Ignite.Tests.Table;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Ignite.Table;
using Internal.Proto;
using Internal.Table;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

/// <summary>
/// Tests schema update logic.
/// </summary>
public class SchemaUpdateTest
{
    [Test]
    public async Task TestMultipleParallelOperationsRequestSchemaOnce()
    {
        using var server = new FakeServer
        {
            OperationDelay = TimeSpan.FromMilliseconds(100)
        };

        using var client = await server.ConnectClientAsync();
        var table = await client.Tables.GetTableAsync(FakeServer.ExistingTableName);
        var view = table!.RecordBinaryView;

        // Schema is not known initially, so both operations will request it.
        // However, we cache the request task, so only one request will be sent.
        var task1 = view.UpsertAsync(null, new IgniteTuple { ["id"] = 1 });
        var task2 = view.UpsertAsync(null, new IgniteTuple { ["id"] = 2 });

        await Task.WhenAll(task1, task2);
        Assert.AreEqual(1, server.ClientOps.Count(x => x == ClientOp.SchemasGet), string.Join(", ", server.ClientOps));

        var schemas = table.GetFieldValue<IDictionary<int, Task<Schema>>>("_schemas");

        // Same schema is cached as "Unknown latest" (-1) and specific version (1).
        CollectionAssert.AreEquivalent(new[] { -1, 1 }, schemas.Keys);
        Assert.AreEqual(1, schemas[-1].GetAwaiter().GetResult().Version);
        Assert.AreEqual(1, schemas[1].GetAwaiter().GetResult().Version);
    }

    [Test]
    public async Task TestFailedSchemaLoadTaskIsRetried()
    {
        using var server = new FakeServer(shouldDropConnection: ctx => ctx is
        {
            OpCode: ClientOp.SchemasGet or ClientOp.Heartbeat,
            RequestCount: < 3
        });

        var cfg = new IgniteClientConfiguration
        {
            RetryPolicy = new RetryNonePolicy(),
            LoggerFactory = TestUtils.GetConsoleLoggerFactory(LogLevel.Trace),
            HeartbeatInterval = TimeSpan.FromDays(1)
        };

        using var client = await server.ConnectClientAsync(cfg);

        var table = await client.Tables.GetTableAsync(FakeServer.ExistingTableName);
        var view = table!.RecordBinaryView;
        var schemas = table.GetFieldValue<IDictionary<int, Task<Schema>>>("_schemas");

        // First operation fails because server drops connection.
        bool firstOpFailed = false;
        try
        {
            await view.UpsertAsync(null, new IgniteTuple { ["id"] = 1 });
            Console.WriteLine("Upsert success.");
        }
        catch (IgniteClientConnectionException expected)
        {
            firstOpFailed = true;
            Console.WriteLine($"Upsert fail: {expected.Message}");
        }

        Assert.IsTrue(schemas[-1].IsFaulted);

        // Second operation should ignore failed task and create a new one, which will succeed.
        await view.UpsertAsync(null, new IgniteTuple { ["id"] = 1 });

        Assert.IsTrue(schemas[-1].IsCompletedSuccessfully, "schemas[-1].IsCompletedSuccessfully");
        Assert.IsTrue(schemas[1].IsCompletedSuccessfully, "schemas[1].IsCompletedSuccessfully");

        Assert.IsTrue(firstOpFailed, "firstOpFailed");
    }
}
