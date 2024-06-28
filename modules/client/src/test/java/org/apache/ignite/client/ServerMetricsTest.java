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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.client.fakes.FakeCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests client handler metrics. See also {@code org.apache.ignite.client.handler.ItClientHandlerMetricsTest}.
 */
@SuppressWarnings("AssignmentToStaticFieldFromInstanceMethod")
public class ServerMetricsTest extends AbstractClientTest {
    @AfterEach
    public void resetCompute() {
        FakeCompute.future = null;
        FakeCompute.latch = new CountDownLatch(0);
        FakeCompute.err = null;
    }

    @BeforeEach
    public void enableMetrics() {
        testServer.metrics().enable();
    }

    @Test
    public void testTxMetrics() {
        assertEquals(0, testServer.metrics().transactionsActive());

        Transaction tx1 = client.transactions().begin(); // Lazy tx does not begin until first operation.
        assertEquals(0, testServer.metrics().transactionsActive());

        client.sql().execute(tx1, "select 1").close(); // Force lazy tx init.
        assertEquals(1, testServer.metrics().transactionsActive());

        Transaction tx2 = client.transactions().begin();
        client.sql().execute(tx2, "select 1").close(); // Force lazy tx init.
        assertEquals(2, testServer.metrics().transactionsActive());

        tx1.rollback();
        assertEquals(1, testServer.metrics().transactionsActive());

        tx2.rollback();
        assertEquals(0, testServer.metrics().transactionsActive());
    }

    @Test
    public void testSqlMetrics() {
        Statement statement = client.sql().statementBuilder()
                .query("select 1")
                .build();

        assertEquals(0, testServer.metrics().cursorsActive());

        ResultSet<SqlRow> resultSet = client.sql().execute(null, statement);
        assertEquals(1, testServer.metrics().cursorsActive());

        resultSet.close();
        assertEquals(0, testServer.metrics().cursorsActive());
    }

    @Test
    public void testRequestsActive() throws Exception {
        assertEquals(0, testServer.metrics().requestsActive());

        FakeCompute.latch = new CountDownLatch(1);

        client.compute().submit(getClusterNodes("s1"), JobDescriptor.builder("job").build(), null);

        assertTrue(
                IgniteTestUtils.waitForCondition(() -> testServer.metrics().requestsActive() == 1, 1000),
                () -> "requestsActive: " + testServer.metrics().requestsActive());

        FakeCompute.latch.countDown();

        assertTrue(
                IgniteTestUtils.waitForCondition(() -> testServer.metrics().requestsActive() == 0, 1000),
                () -> "requestsActive: " + testServer.metrics().requestsActive());
    }

    @Test
    public void testRequestsProcessed() throws Exception {
        long processed = testServer.metrics().requestsProcessed();

        client.compute().submit(getClusterNodes("s1"), JobDescriptor.builder("job").build(), null);

        assertTrue(
                IgniteTestUtils.waitForCondition(() -> testServer.metrics().requestsProcessed() == processed + 1, 1000),
                () -> "requestsProcessed: " + testServer.metrics().requestsProcessed());
    }

    @Test
    public void testRequestsFailed() throws Exception {
        assertEquals(0, testServer.metrics().requestsFailed());

        FakeCompute.err = new RuntimeException("test");

        client.compute().submit(getClusterNodes("s1"), JobDescriptor.builder("job").build(), null);

        assertTrue(
                IgniteTestUtils.waitForCondition(() -> testServer.metrics().requestsFailed() == 1, 1000),
                () -> "requestsFailed: " + testServer.metrics().requestsFailed());
    }

    @Test
    public void testMetricsDisabled() {
        testServer.metrics().disable();

        assertFalse(testServer.metrics().enabled());
        assertEquals(0, testServer.metrics().requestsProcessed());

        client.compute().execute(getClusterNodes("s1"), JobDescriptor.builder("job").build(), null);

        assertEquals(0, testServer.metrics().requestsProcessed());
        assertFalse(testServer.metrics().enabled());
    }

    @Test
    public void testEnabledMetricsTwiceReturnsSameMetricSet() {
        var metrics = testServer.metrics();
        var set1 = metrics.enable();
        var set2 = metrics.enable();

        assertSame(set1, set2);
    }
}
