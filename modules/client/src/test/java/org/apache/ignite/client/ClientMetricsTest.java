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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.ignite.client.IgniteClient.Builder;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.fakes.FakeSession;
import org.apache.ignite.internal.client.ClientMetricSource;
import org.apache.ignite.internal.client.TcpIgniteClient;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests client-side metrics (see also server-side metrics tests in {@link ServerMetricsTest}).
 */
public class ClientMetricsTest {
    private TestServer server;
    private IgniteClient client;

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testConnectionMetrics(boolean gracefulDisconnect) throws Exception {
        server = AbstractClientTest.startServer(1000, new FakeIgnite());
        client = clientBuilder().build();

        ClientMetricSource metrics = metrics();

        assertEquals(1, metrics.connectionsEstablished());
        assertEquals(1, metrics.connectionsActive());

        if (gracefulDisconnect) {
            client.close();
        } else {
            server.close();
        }

        assertTrue(
                IgniteTestUtils.waitForCondition(() -> metrics.connectionsActive() == 0, 1000),
                () -> "connectionsActive: " + metrics.connectionsActive());

        assertEquals(1, metrics.connectionsEstablished());
        assertEquals(gracefulDisconnect ? 0 : 1, metrics.connectionsLost());
    }

    @Test
    public void testConnectionsLostTimeout() throws InterruptedException {
        Function<Integer, Boolean> shouldDropConnection = requestIdx -> requestIdx == 0;
        Function<Integer, Integer> responseDelay = idx -> idx > 1 ? 500 : 0;
        server = new TestServer(
                1000,
                new FakeIgnite(),
                shouldDropConnection,
                responseDelay,
                null,
                AbstractClientTest.clusterId,
                null,
                null
        );
        client = clientBuilder()
                .connectTimeout(100)
                .heartbeatTimeout(100)
                .heartbeatInterval(100)
                .build();

        assertTrue(
                IgniteTestUtils.waitForCondition(() -> metrics().connectionsLostTimeout() == 1, 1000),
                () -> "connectionsLostTimeout: " + metrics().connectionsLostTimeout());
    }

    @Test
    public void testHandshakesFailed() {
        AtomicInteger counter = new AtomicInteger();
        Function<Integer, Boolean> shouldDropConnection = requestIdx -> counter.incrementAndGet() < 3; // Fail 2 handshakes.
        server = new TestServer(
                1000,
                new FakeIgnite(),
                shouldDropConnection,
                null,
                null,
                AbstractClientTest.clusterId,
                null,
                null
        );

        client = clientBuilder().build();

        assertEquals(2, metrics().handshakesFailed());
    }

    @Test
    public void testHandshakesFailedTimeout() throws InterruptedException {
        AtomicInteger counter = new AtomicInteger();
        Function<Integer, Boolean> shouldDropConnection = requestIdx -> false;
        Function<Integer, Integer> responseDelay = idx -> counter.incrementAndGet() == 1 ? 500 : 0;
        server = new TestServer(
                1000,
                new FakeIgnite(),
                shouldDropConnection,
                responseDelay,
                null,
                AbstractClientTest.clusterId,
                null,
                null
        );
        client = clientBuilder()
                .connectTimeout(100)
                .build();

        assertTrue(
                IgniteTestUtils.waitForCondition(() -> metrics().handshakesFailedTimeout() == 1, 1000),
                () -> "handshakesFailedTimeout: " + metrics().handshakesFailedTimeout());
    }

    @Test
    public void testRequestsMetrics() throws InterruptedException {
        Function<Integer, Boolean> shouldDropConnection = requestIdx -> requestIdx == 5;
        Function<Integer, Integer> responseDelay = idx -> idx == 4 ? 1000 : 0;
        server = new TestServer(
                1000,
                new FakeIgnite(),
                shouldDropConnection,
                responseDelay,
                null,
                AbstractClientTest.clusterId,
                null,
                null
        );
        client = clientBuilder().build();

        assertEquals(0, metrics().requestsActive());
        assertEquals(0, metrics().requestsFailed());
        assertEquals(0, metrics().requestsCompleted());
        assertEquals(0, metrics().requestsSent());
        assertEquals(0, metrics().requestsRetried());

        client.tables().tables();

        assertEquals(0, metrics().requestsActive());
        assertEquals(0, metrics().requestsFailed());
        assertEquals(1, metrics().requestsCompleted());
        assertEquals(1, metrics().requestsSent());
        assertEquals(0, metrics().requestsRetried());

        assertThrows(IgniteException.class, () -> client.sql().createSession().execute(null, FakeSession.FAILED_SQL));

        assertEquals(0, metrics().requestsActive());
        assertEquals(1, metrics().requestsFailed());
        assertEquals(1, metrics().requestsCompleted());
        assertEquals(2, metrics().requestsSent());
        assertEquals(0, metrics().requestsRetried());

        client.tables().tablesAsync();

        assertTrue(
                IgniteTestUtils.waitForCondition(() -> metrics().requestsSent() == 3, 1000),
                () -> "requestsSent: " + metrics().requestsSent());

        assertEquals(1, metrics().requestsActive());
        assertEquals(1, metrics().requestsFailed());
        assertEquals(1, metrics().requestsCompleted());
        assertEquals(0, metrics().requestsRetried());

        client.tables().tables();

        assertTrue(
                IgniteTestUtils.waitForCondition(() -> metrics().requestsRetried() == 1, 1000),
                () -> "requestsRetried: " + metrics().requestsRetried());

        assertEquals(1, metrics().requestsFailed());
        assertEquals(3, metrics().requestsCompleted());
        assertEquals(6, metrics().requestsSent());
        assertEquals(1, metrics().requestsRetried());
    }

    @Test
    public void testBytesSentReceived() {
        server = AbstractClientTest.startServer(1000, new FakeIgnite());
        client = clientBuilder().build();

        assertEquals(15, metrics().bytesSent());
        assertEquals(50, metrics().bytesReceived());

        client.tables().tables();

        assertEquals(21, metrics().bytesSent());
        assertEquals(55, metrics().bytesReceived());
    }

    @AfterEach
    public void afterAll() throws Exception {
        if (client != null) {
            client.close();
        }

        if (server != null) {
            server.close();
        }
    }

    private Builder clientBuilder() {
        return IgniteClient.builder()
                .addresses("127.0.0.1:" + server.port())
                .metricsEnabled(true);
    }

    private ClientMetricSource metrics() {
        return ((TcpIgniteClient) client).metrics();
    }
}
