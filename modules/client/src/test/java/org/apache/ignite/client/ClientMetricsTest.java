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
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import org.apache.ignite.client.IgniteClient.Builder;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.fakes.FakeIgniteQueryProcessor;
import org.apache.ignite.client.fakes.FakeIgniteTables;
import org.apache.ignite.internal.client.ClientMetricSource;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.TcpIgniteClient;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.LoggerFactory;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

/**
 * Tests client-side metrics (see also server-side metrics tests in {@link ServerMetricsTest}).
 */
@WithSystemProperty(key = "IGNITE_TIMEOUT_WORKER_SLEEP_INTERVAL", value = "10")
public class ClientMetricsTest extends BaseIgniteAbstractTest {
    private TestServer server;
    private IgniteClient client;
    private IgniteClient client2;

    @AfterEach
    public void afterEach() throws Exception {
        closeAll(client2, client, server);
    }

    @Test
    public void testConnectionMetrics() throws Exception {
        server = AbstractClientTest.startServer(1000, new FakeIgnite());
        client = clientBuilder().build();

        ClientMetricSource metrics = metrics();

        assertEquals(1, metrics.connectionsEstablished());
        assertEquals(1, metrics.connectionsActive());

        server.close();

        assertTrue(
                IgniteTestUtils.waitForCondition(() -> metrics.connectionsActive() == 0, 1000),
                () -> "connectionsActive: " + metrics.connectionsActive());

        assertTrue(
                IgniteTestUtils.waitForCondition(() -> metrics.connectionsLost() == 1, 1000),
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
    public void testHandshakesFailedTimeout() {
        // Record handshake timeout logs.
        // These logs are sent after the timeout is detected by the timeout task and after the metric manager is updated.
        // Therefore it's safe to way for them.
        // Checkout: org.apache.ignite.internal.client.TcpClientChannel.handshakeAsync
        CountDownLatch latch = new CountDownLatch(1);
        HandshakeTimeoutLoggerListener handshakeTimeoutListener = new HandshakeTimeoutLoggerListener(latch);

        Function<Integer, Boolean> shouldDropConnection = requestIdx -> false;
        // Blocks until a timeout was observed.
        Function<Integer, Integer> responseDelay = idx -> {
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for handshake timeout latch", e);
            }

            return 0;
        };

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
                .loggerFactory(handshakeTimeoutListener)
                .build();

        long numObservedTimeouts = handshakeTimeoutListener.count();
        assertThat("handshakesFailedTimeout", metrics().handshakesFailedTimeout(), greaterThanOrEqualTo(numObservedTimeouts));
    }

    @SuppressWarnings("resource")
    @Test
    public void testRequestsMetrics() throws InterruptedException {
        Function<Integer, Boolean> shouldDropConnection = requestIdx -> requestIdx == 5;
        Function<Integer, Integer> responseDelay = idx -> idx == 4 ? 1000 : 0;
        server = new TestServer(
                1_000_000,
                new FakeIgnite(),
                shouldDropConnection,
                responseDelay,
                null,
                AbstractClientTest.clusterId,
                null,
                null
        );
        client = clientBuilder()
                .heartbeatInterval(1_000_000)
                .build();

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
        assertEquals(5, metrics().requestsSent());
        assertEquals(1, metrics().requestsRetried());
    }

    @Test
    public void testBytesSentReceived() {
        server = AbstractClientTest.startServer(1000, new FakeIgnite());
        client = clientBuilder().build();

        assertEquals(17, metrics().bytesSent());

        long handshakeReceived = metrics().bytesReceived();
        assertThat(handshakeReceived, greaterThanOrEqualTo(77L));

        client.tables().tables();

        assertEquals(23, metrics().bytesSent());
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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testJmxExport(boolean metricsEnabled) throws Exception {
        server = AbstractClientTest.startServer(1000, new FakeIgnite());
        client = clientBuilder().metricsEnabled(metricsEnabled).name("testJmxExport").build();
        client.tables().tables();

        String beanName = "org.apache.ignite:nodeName=testJmxExport,type=metrics,name=client";
        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

        ObjectName objName = new ObjectName(beanName);
        boolean registered = mbeanSrv.isRegistered(objName);

        assertEquals(metricsEnabled, registered, "Unexpected MBean state: [name=" + beanName + ", registered=" + registered + ']');

        if (!metricsEnabled) {
            return;
        }

        DynamicMBean bean = MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, objName, DynamicMBean.class, false);
        assertEquals(1L, bean.getAttribute("ConnectionsActive"));
        assertEquals(1L, bean.getAttribute("ConnectionsEstablished"));

        MBeanInfo beanInfo = bean.getMBeanInfo();
        MBeanAttributeInfo[] attributes = beanInfo.getAttributes();
        MBeanAttributeInfo attribute = attributes[0];
        assertEquals("ConnectionsActive", attribute.getName());
        assertEquals("Currently active connections", attribute.getDescription());
        assertEquals("java.lang.Long", attribute.getType());
    }

    @Test
    public void testJmxExportTwoClients() throws Exception {
        server = AbstractClientTest.startServer(1000, new FakeIgnite());

        // Client names are auto-generated, unless explicitly specified.
        client = clientBuilder().metricsEnabled(true).build();
        client2 = clientBuilder().metricsEnabled(true).build();

        client.tables().tables();
        client2.tables().tables();

        for (var clientName : new String[]{client.name(), client2.name()}) {
            String beanName = "org.apache.ignite:nodeName=" + clientName + ",type=metrics,name=client";
            MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

            ObjectName objName = new ObjectName(beanName);
            boolean registered = mbeanSrv.isRegistered(objName);

            assertTrue(registered, "Unexpected MBean state: [name=" + beanName + ", registered=" + registered + ']');

            DynamicMBean bean = MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, objName, DynamicMBean.class, false);
            assertEquals(1L, bean.getAttribute("ConnectionsActive"));
            assertEquals(1L, bean.getAttribute("ConnectionsEstablished"));
        }
    }

    @Test
    public void testJmxExportTwoClientsSameName() throws Exception {
        server = AbstractClientTest.startServer(1000, new FakeIgnite());

        var loggerFactory1 = new TestLoggerFactory("client1");
        var loggerFactory2 = new TestLoggerFactory("client2");

        String clientName = "testJmxExportTwoClientsSameName";
        client = clientBuilder().metricsEnabled(true).name(clientName).loggerFactory(loggerFactory1).build();
        client2 = clientBuilder().metricsEnabled(true).name(clientName).loggerFactory(loggerFactory2).build();

        client.tables().tables();
        client2.tables().tables();

        String beanName = "org.apache.ignite:nodeName=" + clientName + ",type=metrics,name=client";
        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

        ObjectName objName = new ObjectName(beanName);
        boolean registered = mbeanSrv.isRegistered(objName);

        assertTrue(registered, "Unexpected MBean state: [name=" + beanName + ", registered=" + registered + ']');

        DynamicMBean bean = MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, objName, DynamicMBean.class, false);
        assertEquals(1L, bean.getAttribute("ConnectionsActive"));
        assertEquals(1L, bean.getAttribute("ConnectionsEstablished"));

        // Error is logged, but the client is functional.
        loggerFactory2.waitForLogContains("MBean for metric set can't be created [name=client].", 3_000);
        loggerFactory1.assertLogDoesNotContain("MBean for metric set can't be created");
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

    /** This logger factory intercepts and counts Handshake Timeout messages. Can be generalised in the future for other messages. */
    private static class HandshakeTimeoutLoggerListener implements LoggerFactory {
        private final CountDownLatch latch;

        private final AtomicInteger counter;

        HandshakeTimeoutLoggerListener(CountDownLatch latch) {
            this.latch = latch;
            this.counter = new AtomicInteger(0);
        }

        @Override
        public Logger forName(String name) {
            Logger base = System.getLogger(name);
            if (ReliableChannel.class.getName().equals(name)) {
                Logger tracker = Mockito.mock(
                        base.getClass(),
                        Mockito.withSettings()
                                .spiedInstance(base)
                                .defaultAnswer(Mockito.CALLS_REAL_METHODS)
                                .stubOnly()
                );

                Mockito.doAnswer(inv -> {
                    Throwable err = inv.getArgument(2);
                    if (err instanceof CompletionException && err.getCause() instanceof IgniteClientConnectionException) {
                        IgniteClientConnectionException ex = (IgniteClientConnectionException) err.getCause();
                        if (ex.getMessage().startsWith("Handshake timeout")) {
                            // Updates the timeout counter and releases the responses so that we can connect to the server.
                            counter.getAndIncrement();
                            latch.countDown();
                        }
                    }

                    return inv.callRealMethod();
                }).when(tracker).log(Mockito.eq(Level.WARNING), Mockito.anyString(), Mockito.any(Throwable.class));

                return tracker;
            } else {
                return base;
            }
        }

        int count() {
            return this.counter.get();
        }
    }
}
