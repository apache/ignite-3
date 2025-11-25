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

import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.client.IgniteClientConfigurationImpl;
import org.apache.ignite.internal.client.InetAddressResolver;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.TcpIgniteClient;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.network.ClusterNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests client DNS resolution.
 */
class ClientDnsDiscoveryTest extends BaseIgniteAbstractTest {
    protected static final String DEFAULT_TABLE = "DEFAULT_TEST_TABLE";

    private static TestServer server1;

    private static TestServer server2;

    private static TestServer server3;

    private static String loopbackAddress;

    private static String hostAddress;

    private static int port;

    @BeforeAll
    static void setUp() throws UnknownHostException {
        loopbackAddress = InetAddress.getLoopbackAddress().getHostAddress();
        hostAddress = InetAddress.getLocalHost().getHostAddress();

        server1 = TestServer.builder()
                .listenAddresses(loopbackAddress)
                .nodeName("server1")
                .clusterId(AbstractClientTest.clusterId)
                .build();

        port = server1.port();

        server2 = TestServer.builder()
                .listenAddresses(hostAddress)
                .nodeName("server2")
                .clusterId(AbstractClientTest.clusterId)
                .port(port)
                .build();

        server3 = TestServer.builder()
                .nodeName("server3")
                .clusterId(AbstractClientTest.clusterId)
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
        AtomicReference<String[]> resolvedAddressesRef = new AtomicReference<>(new String[]{loopbackAddress, "1.1.1.1"});

        try (var client = TcpIgniteClient.startAsync(getClientConfiguration(addresses, 0L, resolvedAddressesRef)).join()) {
            assertDoesNotThrow(() -> client.tables().tables());
            assertEquals("server1", client.connections().get(0).name());
        }
    }

    @Test
    void testClientConnectToAllNodes() throws InterruptedException {
        String[] addresses = {"my-cluster:" + port, "my-cluster:" + server3.port()};

        // All nodes addresses.
        AtomicReference<String[]> resolvedAddressesRef = new AtomicReference<>(new String[]{loopbackAddress, hostAddress});
        Set<String> allNodeNames = Set.of("server1", "server2", "server3");

        try (var client = TcpIgniteClient.startAsync(getClientConfiguration(addresses, 0L, resolvedAddressesRef)).join()) {
            assertDoesNotThrow(() -> client.tables().tables());

            // Wait until client connects to all nodes.
            assertTrue(IgniteTestUtils.waitForCondition(
                            () -> client.connections().stream().map(ClusterNode::name).allMatch(allNodeNames::contains), 1000),
                    () -> "Client should have three connections: " + client.connections().size());
        }
    }

    @Test
    void testClientRefreshesDnsOnNodeFailure() throws Exception {
        // One valid address.
        AtomicReference<String[]> resolvedAddressesRef = new AtomicReference<>(new String[]{loopbackAddress});

        try (var server4 = TestServer.builder().listenAddresses(loopbackAddress).nodeName("server4").clusterId(AbstractClientTest.clusterId)
                .build();
                var ignored = TestServer.builder().listenAddresses(hostAddress).nodeName("server5").clusterId(AbstractClientTest.clusterId)
                        .port(server4.port()).build();
                var client = TcpIgniteClient.startAsync(getClientConfiguration(new String[]{"my-cluster:" + server4.port()},
                        0L, resolvedAddressesRef)).join()) {
            assertDoesNotThrow(() -> client.tables().tables());
            assertEquals("server4", client.connections().get(0).name());

            // Both nodes.
            resolvedAddressesRef.set(new String[]{loopbackAddress, hostAddress});

            // Stop first node.
            server4.close();

            // Client should reconnect to the second node.
            assertDoesNotThrow(() -> client.tables().tables());
            assertEquals("server5", client.connections().get(0).name());
        }
    }

    @Test
    void testClientRefreshesDnsOnPrimaryReplicaChange() {
        String[] addresses = {"my-cluster:" + port};

        // One valid address points to first node.
        AtomicReference<String[]> resolvedAddressesRef = new AtomicReference<>(new String[]{loopbackAddress});

        try (var client = TcpIgniteClient.startAsync(getClientConfiguration(addresses, 0L, resolvedAddressesRef)).join()) {
            assertDoesNotThrow(() -> client.tables().tables());
            assertEquals("server1", client.connections().get(0).name());

            // Change address to second node.
            resolvedAddressesRef.set(new String[]{hostAddress});

            ReliableChannel ch = ((TcpIgniteClient) client).channel();
            server1.placementDriver().setReplicas(List.of("server3", "server2", "server1", "server2"),
                    1,
                    1,
                    ch.partitionAssignmentTimestamp() + 1);

            // Client should reconnect to the second node.
            assertDoesNotThrow(() -> client.tables().tables());
            assertEquals("server2", client.connections().get(0).name());
        }
    }

    @Test
    void testClientRefreshesDnsByTimeout() throws Exception {
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
    void testMultipleIpsSameNode() throws InterruptedException {
        String[] addresses = {"my-cluster:" + server3.port()};

        // One node.
        AtomicReference<String[]> resolvedAddressesRef = new AtomicReference<>(new String[]{loopbackAddress, hostAddress});

        try (var client = TcpIgniteClient.startAsync(getClientConfiguration(addresses, 0L, resolvedAddressesRef)).join()) {
            assertDoesNotThrow(() -> client.tables().tables());
            assertEquals("server3", client.connections().get(0).name());

            ReliableChannel ch = ((TcpIgniteClient) client).channel();

            List<?> channels = IgniteTestUtils.getFieldValue(ch, "channels");
            assertEquals(2, channels.size());

            // Update to another IPs for the same node.
            resolvedAddressesRef.set(new String[]{hostAddress});

            server3.placementDriver().setReplicas(List.of("server3", "server2", "server1", "server2"),
                    1,
                    1,
                    ch.partitionAssignmentTimestamp() + 1);

            // Wait until client connects to all nodes.
            assertTrue(IgniteTestUtils.waitForCondition(() -> IgniteTestUtils.<List>getFieldValue(ch, "channels").size() == 1, 1000),
                    () -> "Client should have three connections: " + client.connections().size());

            // Client should reconnect to the second ips.
            assertDoesNotThrow(() -> client.tables().tables());
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
                500,
                0,
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
