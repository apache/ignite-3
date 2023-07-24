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

namespace Apache.Ignite.Tests
{
    using System;
    using System.Threading.Tasks;
    using NUnit.Framework;

    /// <summary>
    /// Tests that <see cref="FakeServer"/> works as expected.
    /// </summary>
    public class FakeServerTests
    {
        [Test]
        public async Task TestConnectToFakeServerAndGetTablesReturnsEmptyList()
        {
            using var server = new FakeServer();
            using var client = await server.ConnectClientAsync();

            var tables = await client.Tables.GetTablesAsync();
            Assert.AreEqual(0, tables.Count);
        }

        [Test]
        public async Task TestConnectToFakeServerAndGetTableThrowsError()
        {
            using var server = new FakeServer();
            using var client = await server.ConnectClientAsync();

            var ex = Assert.ThrowsAsync<IgniteException>(async () => await client.Tables.GetTableAsync("t"));
            Assert.AreEqual("Err!", ex!.Message);
            Assert.AreEqual("org.foo.bar.BazException", ex.InnerException!.Message);
            Assert.AreEqual(Guid.Empty, ex.TraceId);
            Assert.AreEqual(ErrorGroups.Sql.StmtValidation, ex.Code);
        }

        [Test]
        public async Task TestConnectToFakeServerAndGetExistingTableReturnsTable()
        {
            using var server = new FakeServer();
            using var client = await server.ConnectClientAsync();

            var table = await client.Tables.GetTableAsync(FakeServer.ExistingTableName);
            Assert.IsNotNull(table);
            Assert.AreEqual(FakeServer.ExistingTableName, table!.Name);
        }

        [Test]
        public async Task TestFakeServerDropsConnectionOnSpecifiedRequestCount()
        {
            var cfg = new IgniteClientConfiguration
            {
                RetryPolicy = RetryNonePolicy.Instance
            };

            using var server = new FakeServer(ctx => ctx.RequestCount % 3 == 0);
            using var client = await server.ConnectClientAsync(cfg);

            // 2 requests succeed, 3rd fails.
            await client.Tables.GetTablesAsync();
            await client.Tables.GetTablesAsync();

            Assert.CatchAsync(async () => await client.Tables.GetTablesAsync());

            // Reconnect by FailoverSocket logic.
            await client.Tables.GetTablesAsync();
        }

        [Test]
        public async Task TestFakeServerExecutesSql()
        {
            using var server = new FakeServer(disableOpsTracking: true);
            using var client = await server.ConnectClientAsync(new());

            for (int i = 0; i < 100; i++)
            {
                await using var res = await client.Sql.ExecuteAsync(null, "select 1");
                var count = 0;

                await foreach (var row in res)
                {
                    Assert.AreEqual(count, row[0]);
                    count++;
                }

                Assert.IsTrue(res.HasRowSet);
                Assert.AreEqual(1012, count);
            }
        }
    }
}
