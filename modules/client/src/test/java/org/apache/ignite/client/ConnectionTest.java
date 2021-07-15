/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Checks if it can connect to a valid address from the node address list.
 */
public class ConnectionTest {
    /** IPv4 default host. */
    public static final String IPv4_HOST = "127.0.0.1";

    /** IPv6 default host. */
    public static final String IPv6_HOST = "::1";

    /** */
    @Test
    public void testEmptyNodeAddress() throws Exception {
        assertThrows(IgniteClientException.class, () -> testConnection(IPv4_HOST, ""));
    }

    /** */
    @Test // (expected = org.apache.ignite.client.IgniteClientException.class)
    public void testNullNodeAddress() throws Exception {
        testConnection(IPv4_HOST, null);
    }

    /** */
    @Test //(expected = org.apache.ignite.client.IgniteClientException.class)
    public void testNullNodeAddresses() throws Exception {
        testConnection(IPv4_HOST, null, null);
    }

    /** */
    @Test
    public void testValidNodeAddresses() throws Exception {
        testConnection(IPv4_HOST, "127.0.0.1:10800");
    }

    /** */
    @Test //(expected = org.apache.ignite.client.ClientConnectionException.class)
    public void testInvalidNodeAddresses() throws Exception {
        testConnection(IPv4_HOST, "127.0.0.1:47500", "127.0.0.1:10801");
    }

    /** */
    @Test
    public void testValidInvalidNodeAddressesMix() throws Exception {
        testConnection(IPv4_HOST, "127.0.0.1:47500", "127.0.0.1:10801", "127.0.0.1:10800");
    }

    /** */
    @Disabled("IPv6 is not enabled by default on some systems.")
    @Test
    public void testIPv6NodeAddresses() throws Exception {
        testConnection(IPv6_HOST, "[::1]:10800");
    }

    /**
     * @param addrs Addresses to connect.
     * @param host LocalIgniteCluster host.
     */
    private void testConnection(String host, String... addrs) throws Exception {
        var serverFuture = AbstractClientTest.startServer(host);

        try {
            AbstractClientTest.startClient(addrs).close();
        } finally {
            serverFuture.cancel(true);
            serverFuture.await();
        }
    }
}
