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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.apache.ignite.client.IgniteClient.Builder;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests client connection to various addresses.
 */
public class ConnectionTest extends AbstractClientTest {
    @Test
    public void testEmptyNodeAddress() {
        var ex = assertThrows(IgniteException.class, () -> testConnection(""));
        assertThat(ex.getMessage(), containsString("Failed to parse Ignite server address (Address is empty): "));
    }

    @Test
    public void testNullNodeAddresses() {
        var ex = assertThrows(IgniteException.class, () -> testConnection(null, null));
        assertThat(ex.getMessage(), containsString("Failed to parse Ignite server address (Address is empty): null"));
    }

    @Test
    public void testValidNodeAddresses() throws Exception {
        testConnection("127.0.0.1:" + serverPort);
    }

    @Test
    public void testDefaultClientConfig() throws Exception {
        try (var ignored = new TestServer(0, new FakeIgnite(), null, null, "abc", clusterId, null, 10800)) {
            IgniteClient.builder()
                    .addresses("localhost")
                    .build()
                    .close();
        }
    }

    @Test
    public void testInvalidNodeAddresses() {
        var ex = assertThrows(IgniteClientConnectionException.class,
                () -> testConnection("127.0.0.1:47500"));

        String errMsg = ex.getCause().getMessage();

        // It does not seem possible to verify that it's a 'Connection refused' exception because with different
        // user locales the message differs, so let's just check that the message ends with the known suffix.
        assertThat(errMsg, endsWith(": /127.0.0.1:47500"));
    }

    @Test
    public void testValidInvalidNodeAddressesMix() throws Exception {
        testConnection("127.0.0.1:47500", "127.0.0.1:10801", "127.0.0.1:" + serverPort);
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15611 . IPv6 is not enabled by default on some systems.")
    @Test
    public void testIpv6NodeAddresses() throws Exception {
        testConnection("[::1]:" + serverPort);
    }

    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testNoResponseFromServerWithinConnectTimeoutThrowsException() throws Exception {
        Function<Integer, Integer> responseDelay = x -> 500;

        try (var srv = new TestServer(300, new FakeIgnite(), x -> false, responseDelay, null, UUID.randomUUID(), null, null)) {
            Builder builder = IgniteClient.builder()
                    .addresses("127.0.0.1:" + srv.port())
                    .retryPolicy(new RetryLimitPolicy().retryLimit(1))
                    .connectTimeout(50);

            assertThrowsWithCause(builder::build, TimeoutException.class);
        }
    }

    private static void testConnection(String... addrs) throws Exception {
        IgniteClient c = AbstractClientTest.startClient(addrs);

        c.close();
    }
}
