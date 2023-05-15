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
using System.Threading.Tasks;
using Ignite.Table;
using Internal.Proto;
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

        CollectionAssert.AreEqual(
            new[]
            {
                ClientOp.TableGet,
                ClientOp.SchemasGet,
                ClientOp.PartitionAssignmentGet,
                ClientOp.TupleUpsert,
                ClientOp.TupleUpsert
            },
            server.ClientOps);
    }

    [Test]
    public async Task TestFailedSchemaLoadTaskIsRetried()
    {
        await Task.Delay(1);
        Assert.Fail("TODO");
    }
}
