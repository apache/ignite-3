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

import static org.apache.ignite.client.fakes.FakeIgniteTables.TABLE_ONE_COLUMN;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Constructor;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.ignite.client.IgniteClient.Builder;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.fakes.FakeIgniteQueryProcessor;
import org.apache.ignite.client.fakes.FakeIgniteTables;
import org.apache.ignite.internal.client.ClientMetricSource;
import org.apache.ignite.internal.client.TcpIgniteClient;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests client-side metrics (see also server-side metrics tests in {@link ServerMetricsTest}).
 */
public class ClientMetricsTest extends BaseIgniteAbstractTest {
    private TestServer server;
    private IgniteClient client;

    @AfterEach
    public void afterEach() throws Exception {
        closeAll(client, server);
    }

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

        assertTrue(
                IgniteTestUtils.waitForCondition(() -> metrics.connectionsLost() == (gracefulDisconnect ? 0 : 1), 1000),
                () -> "connectionsLost: " + metrics.connectionsLost());

        assertEquals(1, metrics.connectionsEstablished());
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

        assertThrowsSqlException(
                Sql.STMT_VALIDATION_ERR,
                "Query failed",
                () -> client.sql().execute(null, FakeIgniteQueryProcessor.FAILED_SQL));

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

        long handshakeReceived = metrics().bytesReceived();
        assertThat(handshakeReceived, greaterThan(80L));

        client.tables().tables();

        assertEquals(21, metrics().bytesSent());
        assertEquals(handshakeReceived + 21, metrics().bytesReceived());
    }

    @Test
    public void testStreamer() throws InterruptedException {
        server = AbstractClientTest.startServer(0, new FakeIgnite());
        client = clientBuilder().build();

        Table table = oneColumnTable();
        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<DataStreamerItem<Tuple>>(ForkJoinPool.commonPool(), 1)) {
            streamerFut = table.recordView().streamData(publisher, null);

            publisher.submit(DataStreamerItem.of(Tuple.create().set("ID", "1")));
            publisher.submit(DataStreamerItem.of(Tuple.create().set("ID", "2")));

            assertTrue(IgniteTestUtils.waitForCondition(() -> metrics().streamerItemsQueued() == 2, 1000));
            assertEquals(0, metrics().streamerItemsSent());
            assertEquals(0, metrics().streamerBatchesSent());
            assertEquals(0, metrics().streamerBatchesActive());
        }

        streamerFut.orTimeout(3, TimeUnit.SECONDS).join();

        assertEquals(2, metrics().streamerItemsSent());
        assertEquals(2, metrics().streamerBatchesSent());
        assertEquals(0, metrics().streamerBatchesActive());
        assertEquals(0, metrics().streamerItemsQueued());
    }

    @SuppressWarnings("ConfusingArgumentToVarargsMethod")
    @Test
    public void testAllMetricsAreRegistered() throws Exception {
        Constructor<ClientMetricSource> ctor = ClientMetricSource.class.getDeclaredConstructor(null);
        ctor.setAccessible(true);

        ClientMetricSource source = ctor.newInstance();
        MetricSet metricSet = source.enable();
        assertNotNull(metricSet);

        var holder = IgniteTestUtils.getFieldValue(source, AbstractMetricSource.class, "holder");

        // Check that all fields from holder are registered in MetricSet.
        for (var field : holder.getClass().getDeclaredFields()) {
            if ("metrics".equals(field.getName())) {
                continue;
            }

            var metricName = field.getName().substring(0, 1).toUpperCase() + field.getName().substring(1);
            var metric = metricSet.get(metricName);

            assertNotNull(metric, "Metric is not registered: " + metricName);
        }
    }

    private Table oneColumnTable() {
        if (server.ignite().tables().table(TABLE_ONE_COLUMN) == null) {
            ((FakeIgniteTables) server.ignite().tables()).createTable(TABLE_ONE_COLUMN);
        }

        return client.tables().table(TABLE_ONE_COLUMN);
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
