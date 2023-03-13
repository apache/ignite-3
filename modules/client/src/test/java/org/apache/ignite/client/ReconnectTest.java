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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.client.IgniteClient.Builder;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.fakes.FakeIgniteTables;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ClusterNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests thin client reconnect.
 */
public class ReconnectTest {
    /** Test server. */
    TestServer server;

    /** Test server 2. */
    TestServer server2;

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(server, server2);
    }

    @Test
    public void clientReconnectsToAnotherAddressOnNodeFail() throws Exception {
        FakeIgnite ignite1 = new FakeIgnite();
        ((FakeIgniteTables) ignite1.tables()).createTable("t");

        server = AbstractClientTest.startServer(
                10900,
                10,
                0,
                ignite1);

        var client = IgniteClient.builder()
                .addresses("127.0.0.1:10900..10910", "127.0.0.1:10950..10960")
                .retryPolicy(new RetryLimitPolicy().retryLimit(100))
                .build();

        assertEquals("t", client.tables().tables().get(0).name());

        server.close();

        FakeIgnite ignite2 = new FakeIgnite();
        ((FakeIgniteTables) ignite2.tables()).createTable("t2");

        server2 = AbstractClientTest.startServer(
                10950,
                10,
                0,
                ignite2);

        assertEquals("t2", client.tables().tables().get(0).name());
    }

    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testOperationFailsWhenAllServersFail() throws Exception {
        FakeIgnite ignite1 = new FakeIgnite();
        ((FakeIgniteTables) ignite1.tables()).createTable("t");

        server = AbstractClientTest.startServer(
                10900,
                10,
                0,
                ignite1);

        var client = IgniteClient.builder()
                .addresses("127.0.0.1:10900..10910", "127.0.0.1:10950..10960")
                .build();

        assertEquals("t", client.tables().tables().get(0).name());

        server.close();

        assertThrowsWithCause(() -> client.tables().tables(), IgniteClientConnectionException.class);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testClientRepairsBackgroundConnectionsPeriodically(boolean reconnectEnabled) throws Exception {
        server = AbstractClientTest.startServer(
                10900,
                0,
                0,
                new FakeIgnite(),
                "node1");

        server2 = AbstractClientTest.startServer(
                10901,
                0,
                0,
                new FakeIgnite(),
                "node2");

        Builder builder = IgniteClient.builder()
                .addresses("127.0.0.1:10900..10902")
                .reconnectInterval(reconnectEnabled ? 50 : 0)
                .heartbeatInterval(50);

        try (var client = builder.build()) {
            assertTrue(IgniteTestUtils.waitForCondition(
                            () -> client.connections().size() == 2, 5000),
                    () -> "Client should have 2 connections: " + client.connections().size());

            server2.close();

            assertTrue(IgniteTestUtils.waitForCondition(
                            () -> client.connections().size() == 1, 5000),
                    () -> "Client should have 1 connections: " + client.connections().size());

            server2 = AbstractClientTest.startServer(
                    10902,
                    0,
                    0,
                    new FakeIgnite(),
                    "node3");

            if (reconnectEnabled) {
                assertTrue(IgniteTestUtils.waitForCondition(
                                () -> client.connections().size() == 2, 5000),
                        () -> "Client should have 2 connections: " + client.connections().size());

                String[] nodeNames = client.connections().stream().map(ClusterNode::name).sorted().toArray(String[]::new);
                assertArrayEquals(new String[]{"node1", "node3"}, nodeNames);
            } else {
                Thread.sleep(100);
                assertEquals(1, client.connections().size());
            }
        }
    }
}
