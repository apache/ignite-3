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

import java.net.InetAddress;
import org.apache.ignite.client.RetryLimitPolicy;
import org.apache.ignite.client.TestServer;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests client DNS resolution.
 */
public class ClientDnsDiscoveryTest {
    private static TestServer server;

    @BeforeAll
    public static void setUp() {
        server = new TestServer(1000, new FakeIgnite());
    }

    @AfterAll
    public static void tearDown() throws Exception {
        closeAll(server);
    }

    @Test
    public void testClientResolvesAllHostNameAddresses() {
        String[] addresses = {"my-cluster:" + server.port()};

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
                return new InetAddress[]{
                        // One invalid address and one valid address.
                        InetAddress.getByName("1.1.1.1"),
                        InetAddress.getByName("127.0.0.1")
                };
            } else {
                return InetAddress.getAllByName(addr);
            }
        };

        try (var client = TcpIgniteClient.startAsync(cfg).join()) {
            client.tables().tables();
        }
    }

    @Test
    public void testClientRefreshesDnsOnNodeFailure() {
        // TODO: Listen to specific localhost address in TestServer.
    }
}
