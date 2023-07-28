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

package org.apache.ignite.client;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.client.IgniteClient.Builder;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.api.Test;

/**
 * Tests heartbeat and idle timeout behavior.
 */
public class HeartbeatTest extends BaseIgniteAbstractTest {
    @Test
    public void testHeartbeatLongerThanIdleTimeoutCausesDisconnect() throws Exception {
        try (var srv = new TestServer(50, new FakeIgnite())) {
            int srvPort = srv.port();
            var loggerFactory = new TestLoggerFactory("client");

            Builder builder = IgniteClient.builder()
                    .addresses("127.0.0.1:" + srvPort)
                    .retryPolicy(null)
                    .loggerFactory(loggerFactory);

            try (var ignored = builder.build()) {
                assertTrue(
                        IgniteTestUtils.waitForCondition(
                                () -> loggerFactory.logger.entries().stream().anyMatch(x -> x.contains("Connection closed")),
                                10000));
            }
        }
    }

    @Test
    public void testHeartbeatShorterThanIdleTimeoutKeepsConnectionAlive() throws Exception {
        try (var srv = new TestServer(300, new FakeIgnite())) {
            int srvPort = srv.port();

            Builder builder = IgniteClient.builder()
                    .addresses("127.0.0.1:" + srvPort)
                    .heartbeatInterval(50);

            try (var client = builder.build()) {
                Thread.sleep(900);

                assertEquals(0, client.tables().tables().size());
            }
        }
    }

    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testInvalidHeartbeatIntervalThrows() throws Exception {
        try (var srv = new TestServer(300, new FakeIgnite())) {

            Builder builder = IgniteClient.builder()
                    .addresses("127.0.0.1:" + srv.port())
                    .heartbeatInterval(-50);

            assertThrowsWithCause(builder::build, IllegalArgumentException.class, "Negative delay.");
        }
    }

    @Test
    public void testHeartbeatTimeoutClosesConnection() throws Exception {
        Function<Integer, Integer> responseDelayFunc = requestCount -> requestCount > 1 ? 500 : 0;
        var loggerFactory = new TestLoggerFactory("client");

        try (var srv = new TestServer(300, new FakeIgnite(), x -> false, responseDelayFunc, null, UUID.randomUUID(), null, null)) {
            int srvPort = srv.port();

            Builder builder = IgniteClient.builder()
                    .addresses("127.0.0.1:" + srvPort)
                    .retryPolicy(new RetryLimitPolicy().retryLimit(1))
                    .heartbeatTimeout(30)
                    .reconnectThrottlingPeriod(5000)
                    .reconnectThrottlingRetries(0)
                    .heartbeatInterval(50)
                    .loggerFactory(loggerFactory);

            try (var ignored = builder.build()) {
                assertTrue(
                        IgniteTestUtils.waitForCondition(
                                () -> loggerFactory.logger.entries().stream()
                                        .anyMatch(x -> x.contains("Heartbeat timeout, closing the channel")),
                                3000));
            }
        }
    }
}
