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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.ignite.client.IgniteClient.Builder;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.junit.jupiter.api.Test;

/**
 * Tests heartbeat and idle timeout behavior.
 */
public class HeartbeatTest {
    @Test
    public void testHeartbeatLongerThanIdleTimeoutCausesDisconnect() throws Exception {
        try (var srv = new TestServer(10800, 10, 50, new FakeIgnite())) {
            int srvPort = srv.port();

            Builder builder = IgniteClient.builder()
                    .addresses("127.0.0.1:" + srvPort)
                    .retryPolicy(null);

            try (var client = builder.build()) {
                Thread.sleep(300);

                assertThrows(IgniteClientConnectionException.class, () -> client.tables().tables());
            }
        }
    }

    @Test
    public void testHeartbeatShorterThanIdleTimeoutKeepsConnectionAlive() throws Exception {
        try (var srv = new TestServer(10800, 10, 300, new FakeIgnite())) {
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

    @Test
    public void testInvalidHeartbeatIntervalThrows() throws Exception {
        try (var srv = new TestServer(10800, 10, 300, new FakeIgnite())) {

            Builder builder = IgniteClient.builder()
                    .addresses("127.0.0.1:" + srv.port())
                    .heartbeatInterval(-50);

            Throwable ex = assertThrows(IllegalArgumentException.class, builder::build);

            assertEquals("Negative delay.", ex.getMessage());
        }
    }


    @Test
    public void testHeartbeatTimeout() throws Exception {
        try (var srv = new TestServer(10800, 10, 300, new FakeIgnite())) {
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
}
