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
    using System.Collections.Generic;
    using System.Data;
    using System.Threading.Tasks;
    using Ignite.Table;
    using NUnit.Framework;

    /// <summary>
    /// Tests client behavior with different <see cref="IgniteClientConfiguration.RetryPolicy"/> settings.
    /// </summary>
    public class RetryPolicyTests
    {
        private const int IterCount = 100;

        [Test]
        public async Task TestFailoverWithRetryPolicyCompletesOperationWithoutException()
        {
            var cfg = new IgniteClientConfiguration
            {
                RetryPolicy = new RetryLimitPolicy { RetryLimit = 1 }
            };

            using var server = new FakeServer(ctx => ctx.RequestCount % 2 == 0);
            using var client = await server.ConnectClientAsync(cfg);

            for (int i = 0; i < IterCount; i++)
            {
                await client.Tables.GetTablesAsync();
            }
        }

        [Test]
        public async Task TestFailoverWithRetryPolicyDoesNotRetryUnrelatedErrors()
        {
            var cfg = new IgniteClientConfiguration { RetryPolicy = new RetryLimitPolicy() };

            using var server = new FakeServer(ctx => ctx.RequestCount % 2 == 0);
            using var client = await server.ConnectClientAsync(cfg);

            var ex = Assert.ThrowsAsync<IgniteException>(async () => await client.Tables.GetTableAsync("bad-table"));
            StringAssert.Contains(FakeServer.Err, ex!.Message);
        }

        [Test]
        public async Task TestFailoverWithRetryPolicyDoesNotRetryTxCommit()
        {
            var testRetryPolicy = new TestRetryPolicy();
            var cfg = new IgniteClientConfiguration { RetryPolicy = testRetryPolicy };

            using var server = new FakeServer(ctx => ctx.RequestCount % 2 == 0);
            using var client = await server.ConnectClientAsync(cfg);

            var tx = await client.Transactions.BeginAsync();
            await TestUtils.ForceLazyTxStart(tx, client);

            Assert.ThrowsAsync<IgniteClientConnectionException>(async () => await tx.CommitAsync());
            Assert.IsEmpty(testRetryPolicy.Invocations);
        }

        [Test]
        public async Task TestFailoverWithRetryPolicyThrowsOnRetryLimitExceeded()
        {
            var cfg = new IgniteClientConfiguration
            {
                RetryPolicy = new TestRetryPolicy { RetryLimit = 5 }
            };

            using var server = new FakeServer(ctx => ctx.RequestCount > 1);
            using var client = await server.ConnectClientAsync(cfg);

            await client.Tables.GetTablesAsync();

            var ex = Assert.ThrowsAsync<IgniteClientConnectionException>(async () => await client.Tables.GetTablesAsync());
            Assert.AreEqual("Operation TablesGet failed after 5 retries, examine InnerException for details.", ex!.Message);
        }

        [Test]
        public async Task TestFailoverWithRetryPolicyThrowsOnDefaultRetryLimitExceeded()
        {
            using var server = new FakeServer(ctx => ctx.RequestCount > 1);
            using var client = await server.ConnectClientAsync();

            await client.Tables.GetTablesAsync();

            var ex = Assert.ThrowsAsync<IgniteClientConnectionException>(async () => await client.Tables.GetTablesAsync());
            Assert.AreEqual("Operation TablesGet failed after 16 retries, examine InnerException for details.", ex!.Message);
        }

        [Test]
        public async Task TestZeroRetryLimitDoesNotLimitRetryCount()
        {
            var cfg = new IgniteClientConfiguration
            {
                RetryPolicy = new RetryLimitPolicy
                {
                    RetryLimit = 0
                }
            };

            using var server = new FakeServer(ctx => ctx.RequestCount % 30 != 0);
            using var client = await server.ConnectClientAsync(cfg);

            for (var i = 0; i < IterCount; i++)
            {
                await client.Tables.GetTablesAsync();
            }
        }

        [Test]
        public async Task TestRetryPolicyIsDisabledByDefault()
        {
            using var server = new FakeServer(ctx => ctx.RequestCount > 1);
            using var client = await server.ConnectClientAsync();

            await client.Tables.GetTablesAsync();

            Assert.ThrowsAsync<IgniteClientConnectionException>(async () => await client.Tables.GetTablesAsync());
        }

        [Test]
        public async Task TestNullRetryPolicyIsSameAsNoRetry()
        {
            var cfg = new IgniteClientConfiguration
            {
                RetryPolicy = null!
            };

            using var server = new FakeServer(ctx => ctx.RequestCount > 1);
            using var client = await server.ConnectClientAsync(cfg);

            await client.Tables.GetTablesAsync();

            Assert.ThrowsAsync<IgniteClientConnectionException>(async () => await client.Tables.GetTablesAsync());
        }

        [Test]
        public async Task TestCustomRetryPolicyIsInvokedWithCorrectContext()
        {
            var testRetryPolicy = new TestRetryPolicy { RetryLimit = 3 };

            var cfg = new IgniteClientConfiguration
            {
                RetryPolicy = testRetryPolicy
            };

            using var server = new FakeServer(ctx => ctx.RequestCount % 3 == 0);
            using var client = await server.ConnectClientAsync(cfg);

            for (var i = 0; i < IterCount; i++)
            {
                await client.Tables.GetTablesAsync();
            }

            Assert.AreEqual(49, testRetryPolicy.Invocations.Count);

            var inv = testRetryPolicy.Invocations[0];

            Assert.AreNotSame(cfg, inv.Configuration);
            Assert.AreSame(testRetryPolicy, inv.Configuration.RetryPolicy);
            Assert.AreEqual(ClientOperationType.TablesGet, inv.Operation);
            Assert.AreEqual(0, inv.Iteration);
        }

        [Test]
        public async Task TestExceptionInRetryPolicyPropagatesToCaller()
        {
            var testRetryPolicy = new TestRetryPolicy { ShouldThrow = true };

            var cfg = new IgniteClientConfiguration
            {
                RetryPolicy = testRetryPolicy
            };

            using var server = new FakeServer(ctx => ctx.RequestCount % 2 == 0);
            using var client = await server.ConnectClientAsync(cfg);

            await client.Tables.GetTablesAsync();

            var ex = Assert.ThrowsAsync<DataException>(async () => await client.Tables.GetTablesAsync());
            Assert.AreEqual("Error in TestRetryPolicy.", ex!.Message);
        }

        [Test]
        public async Task TestTableOperationWithoutTxIsRetried()
        {
            var cfg = new IgniteClientConfiguration
            {
                RetryPolicy = new TestRetryPolicy { RetryLimit = 1 }
            };

            using var server = new FakeServer(ctx => ctx.RequestCount % 2 == 0);
            using var client = await server.ConnectClientAsync(cfg);

            for (int i = 0; i < IterCount; i++)
            {
                var table = await client.Tables.GetTableAsync(FakeServer.ExistingTableName);

                await table!.RecordBinaryView.UpsertAsync(null, new IgniteTuple { ["ID"] = 1 });
            }
        }

        [Test]
        public async Task TestTableOperationWithTxIsNotRetried()
        {
            var cfg = new IgniteClientConfiguration
            {
                RetryPolicy = new TestRetryPolicy()
            };

            using var server = new FakeServer(ctx => ctx.RequestCount % 2 == 0);
            using var client = await server.ConnectClientAsync(cfg);
            var tx = await client.Transactions.BeginAsync();
            await TestUtils.ForceLazyTxStart(tx, client);

            var table = await client.Tables.GetTableAsync(FakeServer.ExistingTableName);

            var ex = Assert.ThrowsAsync<IgniteClientConnectionException>(
                async () => await table!.RecordBinaryView.UpsertAsync(tx, new IgniteTuple { ["ID"] = 1 }));
            StringAssert.StartsWith("Socket is closed due to an error", ex!.Message);
        }

        [Test]
        public async Task TestRetryOperationWithPayloadReusesPooledBufferCorrectly()
        {
            var cfg = new IgniteClientConfiguration
            {
                RetryPolicy = new TestRetryPolicy { RetryLimit = 1 }
            };

            using var server = new FakeServer(ctx => ctx.RequestCount % 2 == 0);
            using var client = await server.ConnectClientAsync(cfg);

            for (int i = 0; i < IterCount; i++)
            {
                var table = await client.Tables.GetTableAsync(FakeServer.ExistingTableName);
                Assert.IsNotNull(table);
            }
        }

        [Test]
        public async Task TestRetryReadPolicyDoesNotRetryWriteOperations()
        {
            var cfg = new IgniteClientConfiguration
            {
                RetryPolicy = new RetryReadPolicy()
            };

            using var server = new FakeServer(ctx => ctx.RequestCount % 2 == 0);
            using var client = await server.ConnectClientAsync(cfg);

            var table = await client.Tables.GetTableAsync(FakeServer.ExistingTableName);
            Assert.ThrowsAsync<IgniteClientConnectionException>(
                async () => await table!.RecordBinaryView.UpsertAsync(null, new IgniteTuple { ["ID"] = 1 }));
        }

        [Test]
        public void TestToString()
        {
            Assert.AreEqual("RetryReadPolicy { RetryLimit = 3 }", new RetryReadPolicy { RetryLimit = 3 }.ToString());
            Assert.AreEqual("RetryLimitPolicy { RetryLimit = 4 }", new RetryLimitPolicy { RetryLimit = 4 }.ToString());
            Assert.AreEqual("TestRetryPolicy { RetryLimit = 5 }", new TestRetryPolicy { RetryLimit = 5 }.ToString());
            Assert.AreEqual("RetryNonePolicy { }", new RetryNonePolicy().ToString());
        }

        private class TestRetryPolicy : RetryLimitPolicy
        {
            private readonly List<IRetryPolicyContext> _invocations = new();

            public IReadOnlyList<IRetryPolicyContext> Invocations => _invocations;

            public bool ShouldThrow { get; set; }

            public override bool ShouldRetry(IRetryPolicyContext context)
            {
                _invocations.Add(context);

                if (ShouldThrow)
                {
                    throw new DataException("Error in TestRetryPolicy.");
                }

                return base.ShouldRetry(context);
            }
        }
    }
}
