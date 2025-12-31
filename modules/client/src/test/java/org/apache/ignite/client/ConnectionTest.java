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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutIn;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.apache.ignite.client.IgniteClient.Builder;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests client connection to various addresses.
 */
@WithSystemProperty(key = "IGNITE_TIMEOUT_WORKER_SLEEP_INTERVAL", value = "10")
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
    public void testValidNodeAddresses() {
        testConnection("127.0.0.1:" + serverPort);
    }

    @Test
    public void testDefaultClientConfig() {
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
        assertThat(errMsg, endsWith(":47500]"));
    }

    @Test
    public void testValidInvalidNodeAddressesMix() {
        testConnection("127.0.0.1:47500", "127.0.0.1:10801", "127.0.0.1:" + serverPort);
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15611 . IPv6 is not enabled by default on some systems.")
    @Test
    public void testIpv6NodeAddresses() {
        testConnection("[::1]:" + serverPort);
    }

    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testNoResponseFromServerWithinConnectTimeoutThrowsException() {
        // Delay should be more than TimeoutWorker#sleepInterval.
        Function<Integer, Integer> responseDelay = x -> 1000;

        try (var srv = new TestServer(3000, new FakeIgnite(), x -> false, responseDelay, null, UUID.randomUUID(), null, null)) {
            Builder builder = IgniteClient.builder()
                    .addresses("127.0.0.1:" + srv.port())
                    .retryPolicy(new RetryLimitPolicy().retryLimit(0))
                    .connectTimeout(50);

            assertThrowsWithCause(builder::build, TimeoutException.class);
        }
    }

    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testNoResponseFromServerWithinOperationTimeoutThrowsException() {
        Function<Integer, Integer> responseDelay = x -> x > 2 ? 600 : 0;

        try (var srv = new TestServer(300, new FakeIgnite(), x -> false, responseDelay, null, UUID.randomUUID(), null, null)) {
            Builder builder = IgniteClient.builder()
                    .addresses("127.0.0.1:" + srv.port())
                    .retryPolicy(new RetryLimitPolicy().retryLimit(0))
                    .operationTimeout(30);

            try (IgniteClient client = builder.build()) {
                client.tables().tables();

                // Second request fails according to responseDelay function.
                assertThrowsWithCause(() -> client.tables().tables(), TimeoutException.class);
            }
        }
    }

    /** Verifies that the client handler doesn't handle requests until it is explicitly enabled. */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testDisabledRequestHandling() {
        String nodeName = "server-2";
        FakeIgnite ignite = new FakeIgnite(nodeName);

        try (TestServer testServer = TestServer.builder()
                .nodeName(nodeName)
                .ignite(ignite)
                .enableRequestHandling(false)
                .build()) {
            Builder clientBuilder = IgniteClient.builder()
                    .addresses("127.0.0.1:" + testServer.port())
                    .retryPolicy(new RetryLimitPolicy().retryLimit(0))
                    .connectTimeout(500);

            assertThrowsWithCause(
                    clientBuilder::build,
                    IgniteClientConnectionException.class,
                    "Handshake timeout [endpoint=127.0.0.1"
            );

            testServer.enableClientRequestHandling();

            try (IgniteClient client = clientBuilder.build()) {
                client.tables().tables();
            }
        }
    }

    /** The test verifies that if request processing is enabled during connection establishment, the request is processed without errors. */
    @Test
    public void testEnableRequestHandlingDuringConnectionEstablishment() throws Exception {
        String nodeName = "server-2";
        FakeIgnite ignite = new FakeIgnite(nodeName);

        try (TestServer testServer = TestServer.builder()
                .nodeName(nodeName)
                .ignite(ignite)
                .enableRequestHandling(false)
                .build()) {
            Builder clientBuilder = IgniteClient.builder()
                    .addresses("127.0.0.1:" + testServer.port())
                    .retryPolicy(new RetryLimitPolicy().retryLimit(0))
                    .connectTimeout(30_000);

            CountDownLatch syncLatch = new CountDownLatch(1);

            CompletableFuture<Void> fut = IgniteTestUtils.runAsync(() -> {
                syncLatch.countDown();

                try (IgniteClient client = clientBuilder.build()) {
                    client.tables().tables();
                }
            });

            syncLatch.await();

            assertThat(fut, willTimeoutIn(500, TimeUnit.MILLISECONDS));

            testServer.enableClientRequestHandling();

            assertThat(fut, willCompleteSuccessfully());
        }
    }

    @Test
    public void testServerDisconnect() {
        try (var server = TestServer.builder().build();
             var client = IgniteClient.builder()
                     .addresses("localhost:" + server.port())
                     .heartbeatInterval(100)
                     .retryPolicy(new RetryLimitPolicy().retryLimit(1))
                     .loggerFactory(new ConsoleLoggerFactory("testServerDisconnect"))
                     .backgroundReconnectInterval(100)
                     .build()) {
            client.tables().tables();

            server.close();

            await().until(() -> client.connections().isEmpty());
        }
    }

    private static void testConnection(String... addrs) {
        IgniteClient c = startClient(addrs);

        c.close();
    }
}
