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
    using System.Threading.Tasks;
    using NUnit.Framework;

    /// <summary>
    /// Tests client behavior with different <see cref="IgniteClientConfiguration.RetryPolicy"/> settings.
    /// </summary>
    public class RetryPolicyTests
    {
        [Test]
        public async Task TestFakeServerConnect()
        {
            using var srv = new FakeServer();
            using var client = await IgniteClient.StartAsync(new IgniteClientConfiguration("127.0.0.1:" + FakeServer.Port));

            var tables = await client.Tables.GetTablesAsync();
            Assert.AreEqual(0, tables.Count);
        }
    }
}
