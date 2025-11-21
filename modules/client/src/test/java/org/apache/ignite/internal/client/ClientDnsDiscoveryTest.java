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

package org.apache.ignite.internal.client;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.client.RetryLimitPolicy;
import org.apache.ignite.client.TestServer;
import org.apache.ignite.network.ClusterNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests client DNS resolution.
 */
class ClientDnsDiscoveryTest {
    protected static final String DEFAULT_TABLE = "DEFAULT_TEST_TABLE";

    private static TestServer server1;

    private static TestServer server2;

    private static TestServer server3;

    private static String loopbackAddress;

    private static String hostAddress;

    private static final UUID clusterId = UUID.randomUUID();

    private static int port;

    @BeforeAll
    static void setUp() throws UnknownHostException {
        loopbackAddress = InetAddress.getLoopbackAddress().getHostAddress();
        hostAddress = InetAddress.getLocalHost().getHostAddress();

        server1 = TestServer.builder()
                .listenAddresses(loopbackAddress)
                .nodeName("server1")
                .clusterId(clusterId)
                .build();

        port = server1.port();

        server2 = TestServer.builder()
                .listenAddresses(hostAddress)
                .nodeName("server2")
                .clusterId(clusterId)
                .port(port)
                .build();

        server3 = TestServer.builder()
                .nodeName("server3")
                .clusterId(clusterId)
                .port(port + 1)
                .build();
    }

    @AfterAll
    static void tearDown() throws Exception {
        closeAll(server1, server2, server3);
    }

    @Test
    void testClientResolvesAllHostNameAddresses() {
        String[] addresses = {"my-cluster:" + port};

        // One invalid and one valid address.
        AtomicReference<String[]> resolvedAddressesRef = new AtomicReference<>(new String[]{"1.1.1.1", loopbackAddress});

        try (var client = TcpIgniteClient.startAsync(getClientConfiguration(addresses, 0L, resolvedAddressesRef)).join()) {
            assertDoesNotThrow(() -> client.tables().tables());
            assertEquals("server1", client.connections().get(0).name());
        }
    }

    @Test
    void testClientConnectToAllNodes() {
        String[] addresses = {"my-cluster:" + port, "my-cluster:" + server3.port()};

        // All nodes addresses.
        AtomicReference<String[]> resolvedAddressesRef = new AtomicReference<>(new String[]{loopbackAddress, hostAddress});

        try (var client = TcpIgniteClient.startAsync(getClientConfiguration(addresses, 0L, resolvedAddressesRef)).join()) {
            assertDoesNotThrow(() -> client.tables().tables());

            // Wait until client connects to all nodes.
            await().pollDelay(ofMillis(100L)).atMost(ofSeconds(1L))
                    .until(() -> client.connections().stream().map(ClusterNode::name).collect(toList()),
                            containsInAnyOrder("server1", "server2", "server3"));
        }
    }

    @Test
    void testClientRefreshesDnsOnNodeFailure() {
        int port = server3.port();
        String[] addresses = {"my-cluster:" + port};

        // One valid address.
        AtomicReference<String[]> resolvedAddressesRef = new AtomicReference<>(new String[]{loopbackAddress});

        try (var server4 = TestServer.builder()
                .listenAddresses(loopbackAddress).nodeName("server4").clusterId(clusterId).port(port).build();
                var client = TcpIgniteClient.startAsync(getClientConfiguration(addresses, 0L, resolvedAddressesRef)).join()) {
            assertDoesNotThrow(() -> client.tables().tables());
            assertEquals("server4", client.connections().get(0).name());

            // Both nodes.
            resolvedAddressesRef.set(new String[]{loopbackAddress, hostAddress});

            // Stop first node.
            server4.close();

            // Client should reconnect to the second node.
            assertDoesNotThrow(() -> client.tables().tables());
            assertEquals("server3", client.connections().get(0).name());
        }
    }

    @Test
    void testClientRefreshesDnsOnPrimaryReplicaChange() throws InterruptedException {
        String[] addresses = {"my-cluster:" + port};

        // One valid address points to first node.
        AtomicReference<String[]> resolvedAddressesRef = new AtomicReference<>(new String[]{loopbackAddress});

        try (var client = TcpIgniteClient.startAsync(getClientConfiguration(addresses, 0L, resolvedAddressesRef)).join()) {
            assertDoesNotThrow(() -> client.tables().tables());
            assertEquals("server1", client.connections().get(0).name());

            // Change address to second node.
            resolvedAddressesRef.set(new String[]{hostAddress});

            server1.placementDriver().setReplicas(List.of("server3", "server2", "server1", "server2"),
                    1,
                    1,
                    server1.clock().nowLong());

            // Client should reconnect to the second node.
            assertDoesNotThrow(() -> client.tables().tables());
            assertEquals("server2", client.connections().get(0).name());
        }
    }

    @Test
    void testClientRefreshesDnsByTimeout() throws InterruptedException {
        String[] addresses = {"my-cluster:" + port};

        // One valid address points to first node.
        AtomicReference<String[]> resolvedAddressesRef = new AtomicReference<>(new String[]{loopbackAddress});

        try (var client = TcpIgniteClient.startAsync(getClientConfiguration(addresses, 500L, resolvedAddressesRef)).join()) {
            assertDoesNotThrow(() -> client.tables().tables());
            assertEquals("server1", client.connections().get(0).name());

            // Change address to second node.
            resolvedAddressesRef.set(new String[]{hostAddress});

            Thread.sleep(500L); // Wait for background dns refresh.

            // Client should reconnect to the second node.
            assertDoesNotThrow(() -> client.tables().tables());
            assertEquals("server2", client.connections().get(0).name());
        }
    }

    @Test
    void testMultipleIpsSameNode() {
        String[] addresses = {"my-cluster:" + server3.port()};

        // One node.
        AtomicReference<String[]> resolvedAddressesRef = new AtomicReference<>(new String[]{loopbackAddress, hostAddress});

        try (var client = TcpIgniteClient.startAsync(getClientConfiguration(addresses, 0L, resolvedAddressesRef)).join()) {
            assertDoesNotThrow(() -> client.tables().tables());
            assertEquals("server3", client.connections().get(0).name());

            // Update to another IPs for the same node.
            resolvedAddressesRef.set(new String[]{hostAddress});

            ((TcpIgniteClient) client).channel().channelsInitAsync().join();

            // Client should reconnect to the second node.
            assertDoesNotThrow(() -> client.tables().tables());
            assertEquals("server3", client.connections().get(0).name());
        }
    }

    private static IgniteClientConfigurationImpl getClientConfiguration(
            String[] addresses,
            long backgroundReResolveAddressesInterval,
            AtomicReference<String[]> resolvedAddressesRef
    ) {
        InetAddressResolver addressResolver = (addr) -> {
            if ("my-cluster".equals(addr)) {
                String[] resolved = resolvedAddressesRef.get();
                InetAddress[] result = new InetAddress[resolved.length];

                for (int i = 0; i < resolved.length; i++) {
                    result[i] = InetAddress.getByName(resolved[i]);
                }

                return result;
            } else {
                return InetAddress.getAllByName(addr);
            }
        };

        return new IgniteClientConfigurationImpl(
                null,
                addresses,
                1000,
                1000,
                null,
                1000,
                1000,
                new RetryLimitPolicy(),
                null,
                null,
                false,
                null,
                1000,
                1000,
                "my-client",
                backgroundReResolveAddressesInterval,
                addressResolver
        );
    }
}
