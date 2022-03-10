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
    using System.Threading.Tasks;
    using NUnit.Framework;

    /// <summary>
    /// Tests client behavior with different <see cref="IgniteClientConfiguration.RetryPolicy"/> settings.
    /// </summary>
    public class RetryPolicyTests
    {
        [Test]
        public async Task TestFailoverWithRetryPolicyCompletesOperationWithoutException()
        {
            var cfg = new IgniteClientConfiguration
            {
                RetryLimit = 1,
                RetryPolicy = new TestRetryPolicy()
            };

            using var server = new FakeServer(reqId => reqId % 2 == 0);
            using var client = await server.ConnectClientAsync(cfg);

            for (int i = 0; i < 100; i++)
            {
                await client.Tables.GetTablesAsync();
            }
        }

        [Test]
        public async Task TestFailoverWithRetryPolicyDoesNotRetryUnrelatedErrors()
        {
            var cfg = new IgniteClientConfiguration { RetryPolicy = new TestRetryPolicy() };

            using var server = new FakeServer(reqId => reqId % 2 == 0);
            using var client = await server.ConnectClientAsync(cfg);

            var ex = Assert.ThrowsAsync<IgniteClientException>(async () => await client.Tables.GetTableAsync("bad-table"));
            Assert.AreEqual(FakeServer.Err, ex!.Message);
        }

        [Test]
        public async Task TestFailoverWithRetryPolicyThrowsOnRetryCountExceeded()
        {
            var cfg = new IgniteClientConfiguration
            {
                RetryLimit = 5,
                RetryPolicy = new TestRetryPolicy()
            };

            using var server = new FakeServer(reqId => reqId > 1);
            using var client = await server.ConnectClientAsync(cfg);

            await client.Tables.GetTablesAsync();

            var ex = Assert.ThrowsAsync<IgniteClientException>(async () => await client.Tables.GetTablesAsync());
            Assert.AreEqual("Operation failed after 5 retries, examine InnerException for details.", ex!.Message);
        }

        [Test]
        public async Task TestCustomRetryPolicyIsInvokedWithCorrectContext()
        {
            var testRetryPolicy = new TestRetryPolicy();

            var cfg = new IgniteClientConfiguration
            {
                RetryLimit = 3,
                RetryPolicy = testRetryPolicy
            };

            using var server = new FakeServer(reqId => reqId % 3 == 0);
            using var client = await server.ConnectClientAsync(cfg);

            for (var i = 0; i < 100; i++)
            {
                await client.Tables.GetTablesAsync();
            }

            Assert.AreEqual(49, testRetryPolicy.Invocations.Count);

            var inv = testRetryPolicy.Invocations[0];

            Assert.AreNotSame(cfg, inv.Configuration);
            Assert.AreSame(testRetryPolicy, inv.Configuration.RetryPolicy);
            Assert.AreEqual(3, inv.Configuration.RetryLimit);
            Assert.AreEqual(ClientOperationType.TablesGet, inv.Operation);
            Assert.AreEqual(0, inv.Iteration);
        }

        [Test]
        public async Task TestRetryOperationWithPayloadReusesPooledBufferCorrectly()
        {
            var cfg = new IgniteClientConfiguration
            {
                RetryLimit = 1,
                RetryPolicy = new TestRetryPolicy()
            };

            using var server = new FakeServer(reqId => reqId % 2 == 0);
            using var client = await server.ConnectClientAsync(cfg);

            for (int i = 0; i < 100; i++)
            {
                var table = await client.Tables.GetTableAsync(FakeServer.ExistingTableName);
                Assert.IsNotNull(table);
            }
        }

        private class TestRetryPolicy : IRetryPolicy
        {
            private readonly List<IRetryPolicyContext> _invocations = new();

            public IReadOnlyList<IRetryPolicyContext> Invocations => _invocations;

            public bool ShouldRetry(IRetryPolicyContext context)
            {
                _invocations.Add(context);

                return true;
            }
        }
    }
}
