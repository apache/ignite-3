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
    using Microsoft.Extensions.Logging;
    using NUnit.Framework;

    /// <summary>
    /// Tests client heartbeat functionality (<see cref="IgniteClientConfiguration.HeartbeatInterval"/>).
    /// </summary>
    public class HeartbeatTests : IgniteTestsBase
    {
        [Test]
        public async Task TestServerDoesNotDisconnectIdleClientWithHeartbeats()
        {
            var logger = new ListLoggerFactory(enabledLevels: new[] { LogLevel.Error, LogLevel.Warning });

            var cfg = new IgniteClientConfiguration
            {
                Endpoints = { "127.0.0.1:" + ServerPort },
                LoggerFactory = logger
            };
            using var client = await IgniteClient.StartAsync(cfg);

            logger.Clear();

            await Task.Delay(ServerIdleTimeout * 3);

            Assert.DoesNotThrowAsync(async () => await client.Tables.GetTablesAsync());
            Assert.IsEmpty(logger.GetLogString(), "No disconnects or reconnects should be logged.");
        }

        [Test]
        public async Task TestDefaultIdleTimeoutUsesRecommendedHeartbeatInterval()
        {
            var log = await ConnectAndGetLog(IgniteClientConfiguration.DefaultHeartbeatInterval);

            StringAssert.Contains(
                "[Warning] Server-side IdleTimeout is 00:00:06, " +
                "configured IgniteClientConfiguration.HeartbeatInterval is 00:00:30, which is longer than recommended IdleTimeout / 3. " +
                "Overriding heartbeat interval with max(IdleTimeout / 3, 500ms): 00:00:02",
                log);
        }

        [Test]
        public async Task TestCustomHeartbeatIntervalOverridesCalculatedFromIdleTimeout()
        {
            var log = await ConnectAndGetLog(TimeSpan.FromMilliseconds(50));

            StringAssert.Contains(
                "[Information] Server-side IdleTimeout is 00:00:06, " +
                "using configured IgniteClientConfiguration.HeartbeatInterval: 00:00:00.0500000",
                log);
        }

        [Test]
        public async Task TestCustomHeartbeatIntervalLongerThanRecommendedDoesNotOverrideCalculatedFromIdleTimeout()
        {
            var log = await ConnectAndGetLog(TimeSpan.FromSeconds(8));

            StringAssert.Contains(
                "[Warning] Server-side IdleTimeout is 00:00:06, " +
                "configured IgniteClientConfiguration.HeartbeatInterval is 00:00:08, which is longer than recommended IdleTimeout / 3. " +
                "Overriding heartbeat interval with max(IdleTimeout / 3, 500ms): 00:00:02",
                log);
        }

        [Test]
        public void TestZeroOrNegativeHeartbeatIntervalThrows()
        {
            Assert.ThrowsAsync<IgniteClientConnectionException>(async () => await ConnectAndGetLog(TimeSpan.Zero));
            Assert.ThrowsAsync<IgniteClientConnectionException>(async () => await ConnectAndGetLog(TimeSpan.FromSeconds(-1)));
        }

        private static async Task<string> ConnectAndGetLog(TimeSpan heartbeatInterval)
        {
            var logger = new ListLoggerFactory();

            var cfg = new IgniteClientConfiguration(GetConfig())
            {
                LoggerFactory = logger,
                HeartbeatInterval = heartbeatInterval
            };

            using var client = await IgniteClient.StartAsync(cfg);
            return logger.GetLogString();
        }
    }
}
