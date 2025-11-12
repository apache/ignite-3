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

import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.client.RetryLimitPolicy;
import org.apache.ignite.client.TestServer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests client DNS resolution.
 */
public class ClientDnsDiscoveryTest {
    private static TestServer server1;

    private static TestServer server2;

    private static int port;

    @BeforeAll
    public static void setUp() {
        UUID clusterId = UUID.randomUUID();

        server1 = TestServer.builder()
                .listenAddresses("127.0.0.1")
                .nodeName("server1")
                .clusterId(clusterId)
                .build();

        port = server1.port();

        server2 = TestServer.builder()
                .listenAddresses("127.0.0.2")
                .nodeName("server2")
                .clusterId(clusterId)
                .port(port)
                .build();
    }

    @AfterAll
    public static void tearDown() throws Exception {
        closeAll(server1, server2);
    }

    @Test
    public void testClientResolvesAllHostNameAddresses() {
        String[] addresses = {"my-cluster:" + port};

        // One invalid and one valid address.
        AtomicReference<String[]> resolvedAddressesRef = new AtomicReference<>(new String[]{"1.1.1.1", "127.0.0.1"});

        try (var client = TcpIgniteClient.startAsync(getClientConfiguration(addresses, resolvedAddressesRef)).join()) {
            client.tables().tables();
        }
    }

    @Test
    public void testClientRefreshesDnsOnNodeFailure() {
        String[] addresses = {"my-cluster:" + port};

        // One node.
        AtomicReference<String[]> resolvedAddressesRef = new AtomicReference<>(new String[]{"127.0.0.1"});

        try (var client = TcpIgniteClient.startAsync(getClientConfiguration(addresses, resolvedAddressesRef)).join()) {
            assertDoesNotThrow(() -> client.tables().tables());
            assertEquals("server1", client.connections().get(0).name());

            // Both nodes.
            resolvedAddressesRef.set(new String[]{"127.0.0.1", "127.0.0.2"});

            // Stop first node.
            server1.close();

            // Client should reconnect to the second node.
            assertDoesNotThrow(() -> client.tables().tables());
            assertEquals("server2", client.connections().get(0).name());
        }
    }

    @Test
    public void testMultipleIpsSameNode() {
        assert false : "TODO";
    }

    private static @NotNull IgniteClientConfigurationImpl getClientConfiguration(
            String[] addresses,
            AtomicReference<String[]> resolvedAddressesRef) {
        var cfg = new IgniteClientConfigurationImpl(
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
                "my-client");

        cfg.addressResolver = (addr) -> {
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

        return cfg;
    }
}
