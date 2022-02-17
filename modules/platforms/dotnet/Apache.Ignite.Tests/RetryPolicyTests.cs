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
        public void TestFailoverWithRetryPolicyDoesNotRetryUnrelatedErrors()
        {
            // TODO
        }

        [Test]
        public void TestFailoverWithRetryPolicyCompletesOperationWithoutException()
        {
            // TODO
        }

        [Test]
        public void TestFailoverWithRetryPolicyThrowsOnRetryCountExceeded()
        {
            // TODO
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

            using var server = new FakeServer(requestCountBeforeClientDrop: 1);
            using var client = await server.ConnectClientAsync(cfg);

            await client.Tables.GetTablesAsync();
            await client.Tables.GetTablesAsync();

            Assert.AreEqual(1, testRetryPolicy.Invocations.Count);
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
